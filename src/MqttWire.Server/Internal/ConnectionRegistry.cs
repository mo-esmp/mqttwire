using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace MqttWire.Server.Internal;

/// <summary>
/// The server's connections (plan decision 4): a slot array that the sweep walks without the lock, and the
/// client-id table that admission uses. Both are changed only under <see cref="Gate"/>.
/// </summary>
internal sealed class ConnectionRegistry
{
    /// <summary>
    /// The number of slots the array starts with; it doubles when full.
    /// </summary>
    private const int InitialSlots = 16;

    /// <summary>
    /// The connection that has each client id, from its admission until just before its
    /// <see cref="IMqttWireHandler.OnClosed"/>.
    /// </summary>
    private readonly Dictionary<string, MqttWireConnection> _table = new(StringComparer.Ordinal);

    /// <summary>
    /// One slot per running connection; a free slot is <see langword="null"/>. Replaced, never shrunk, when it grows.
    /// </summary>
    private MqttWireConnection?[] _slots = new MqttWireConnection?[InitialSlots];

    /// <summary>
    /// The indexes of the free slots below <see cref="_used"/>, as a stack of <see cref="_freeCount"/> entries.
    /// </summary>
    private int[] _free = new int[InitialSlots];

    /// <summary>
    /// The number of entries in <see cref="_free"/>.
    /// </summary>
    private int _freeCount;

    /// <summary>
    /// The number of slots ever handed out: every slot at or above it is free and not in <see cref="_free"/>.
    /// </summary>
    private int _used;

    /// <summary>
    /// The number of slots taken.
    /// </summary>
    private int _taken;

    /// <summary>
    /// The connections that hold a slot the stop no longer waits for (<see cref="TryAbandon"/>), or
    /// <see langword="null"/> while there is none, which is the normal case.
    /// </summary>
    private List<MqttWireConnection>? _abandoned;

    /// <summary>
    /// Completes when no slot is taken that the stop waits for, or <see langword="null"/> when no one waits.
    /// </summary>
    private TaskCompletionSource? _empty;

    /// <summary>
    /// <see langword="true"/> once the server's stop has begun; written only under <see cref="Gate"/>.
    /// </summary>
    private volatile bool _stopping;

    /// <summary>
    /// Gets the lock that guards the slots and the table; admission runs inside it.
    /// </summary>
    public Lock Gate { get; } = new();

    /// <summary>
    /// Gets the number of connections in the client-id table.
    /// </summary>
    public int ActiveCount
    {
        get
        {
            lock (Gate)
            {
                return _table.Count;
            }
        }
    }

    /// <summary>
    /// Gets a value indicating whether the server's stop has begun. Set under <see cref="Gate"/> by
    /// <see cref="MarkStopping"/>; read without the lock, it may be a moment late, and under it, it is exact.
    /// </summary>
    public bool IsStopping => _stopping;

    /// <summary>
    /// Gets the slot array, read without the lock. An entry may be <see langword="null"/>, and a connection in an
    /// array that has since been replaced may already have ended; a reader must accept both.
    /// </summary>
    public MqttWireConnection?[] Slots => Volatile.Read(ref _slots);

    /// <summary>
    /// Gives a connection a slot, growing the array when every slot is taken, and reports, in the same hold of
    /// <see cref="Gate"/>, whether the server's stop has begun. So a connection is either in the slots before the
    /// stop begins, where the stop's walk finds it, or learns here that the stop has begun, never neither.
    /// </summary>
    /// <param name="connection">
    /// The connection, which must not have a slot yet.
    /// </param>
    /// <returns>
    /// <see langword="false"/> when the server is not stopping. <see langword="true"/> when its stop has begun: the
    /// slot is taken all the same, the stop's walk may not have seen the connection, and the caller must end it,
    /// which frees the slot.
    /// </returns>
    public bool Add(MqttWireConnection connection)
    {
        lock (Gate)
        {
            int slot = _freeCount > 0 ? _free[--_freeCount] : TakeUnusedSlotLocked();
            connection.Slot = slot;
            _taken++;
            Volatile.Write(ref _slots[slot], connection);
            return _stopping;
        }
    }

    /// <summary>
    /// Records that the server's stop has begun, so that every later <see cref="Add"/> reports it. The caller holds
    /// <see cref="Gate"/>, so an admission, which runs under it, either precedes this or sees it.
    /// </summary>
    public void MarkStopping()
    {
        Debug.Assert(Gate.IsHeldByCurrentThread, "The caller holds the gate.");
        _stopping = true;
    }

    /// <summary>
    /// Removes a connection's table entry, only when the entry is this connection, so the end of a replaced
    /// connection never removes the one that replaced it. The slot stays taken until <see cref="Release"/>.
    /// </summary>
    /// <param name="connection">
    /// The connection; one that is not in the table changes nothing.
    /// </param>
    public void RemoveFromTable(MqttWireConnection connection)
    {
        lock (Gate)
        {
            if (_table.TryGetValue(connection.ClientId, out MqttWireConnection? current)
                && ReferenceEquals(current, connection))
            {
                _table.Remove(connection.ClientId);
            }
        }
    }

    /// <summary>
    /// Frees a connection's slot, the last step of its end. Completes <see cref="WhenEmptyAsync"/> when this was
    /// the last slot taken that the stop waits for. The table entry is not touched: <see cref="RemoveFromTable"/>
    /// removed it before.
    /// </summary>
    /// <param name="connection">
    /// The connection, which <see cref="Add"/> gave a slot.
    /// </param>
    public void Release(MqttWireConnection connection)
    {
        TaskCompletionSource? empty = null;
        lock (Gate)
        {
            int slot = connection.Slot;
            Debug.Assert(ReferenceEquals(_slots[slot], connection), "The connection does not hold its slot.");
            Volatile.Write(ref _slots[slot], value: null);
            _free[_freeCount++] = slot;
            _taken--;
            _abandoned?.Remove(connection);
            if (_taken == (_abandoned?.Count ?? 0))
            {
                empty = _empty;
                _empty = null;
            }
        }

        empty?.TrySetResult();
    }

    /// <summary>
    /// Stops counting a connection's slot for <see cref="WhenEmptyAsync"/>, for a connection whose run cannot end
    /// before a call of the host returns: the stop does not wait for it. The slot stays taken, and the sweep goes on
    /// visiting it, until <see cref="Release"/>. Completes <see cref="WhenEmptyAsync"/> when no other slot is taken.
    /// </summary>
    /// <param name="connection">
    /// The connection.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when this call stopped counting the slot; <see langword="false"/> when an earlier call
    /// had, or the connection no longer holds its slot, and nothing changed.
    /// </returns>
    public bool TryAbandon(MqttWireConnection connection)
    {
        TaskCompletionSource? empty = null;
        lock (Gate)
        {
            if (!ReferenceEquals(_slots[connection.Slot], connection) || _abandoned?.Contains(connection) == true)
            {
                return false;
            }

            _abandoned ??= [];
            _abandoned.Add(connection);
            if (_taken == _abandoned.Count)
            {
                empty = _empty;
                _empty = null;
            }
        }

        empty?.TrySetResult();
        return true;
    }

    /// <summary>
    /// Finds the connection that has a client id. The caller holds <see cref="Gate"/>.
    /// </summary>
    /// <param name="clientId">
    /// The client id.
    /// </param>
    /// <returns>
    /// The connection, or <see langword="null"/> when none has the client id.
    /// </returns>
    public MqttWireConnection? Find(string clientId)
    {
        Debug.Assert(Gate.IsHeldByCurrentThread, "The caller holds the gate.");
        return _table.GetValueOrDefault(clientId);
    }

    /// <summary>
    /// Makes a connection the one that has its client id, in place of any other. The caller holds
    /// <see cref="Gate"/>.
    /// </summary>
    /// <param name="connection">
    /// The connection.
    /// </param>
    public void SetCurrent(MqttWireConnection connection)
    {
        Debug.Assert(Gate.IsHeldByCurrentThread, "The caller holds the gate.");
        _table[connection.ClientId] = connection;
    }

    /// <summary>
    /// Looks up the connection that has a client id.
    /// </summary>
    /// <param name="clientId">
    /// The client id.
    /// </param>
    /// <param name="connection">
    /// The connection when found; otherwise <see langword="null"/>.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when a connection has the client id; <see langword="false"/> otherwise.
    /// </returns>
    public bool TryGet(string clientId, [NotNullWhen(true)] out MqttWireConnection? connection)
    {
        lock (Gate)
        {
            return _table.TryGetValue(clientId, out connection);
        }
    }

    /// <summary>
    /// Waits until no slot is taken, those that <see cref="TryAbandon"/> stopped counting aside.
    /// </summary>
    /// <returns>
    /// A task that completes, never inline, when the last slot that counts is freed or no longer counted; at once
    /// when none is taken now.
    /// </returns>
    public Task WhenEmptyAsync()
    {
        lock (Gate)
        {
            if (_taken == (_abandoned?.Count ?? 0))
            {
                return Task.CompletedTask;
            }

            _empty ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            return _empty.Task;
        }
    }

    /// <summary>
    /// Hands out the first slot never used, doubling the slot and free arrays when they are full. The caller holds
    /// <see cref="Gate"/>. A grown slot array is published whole, so a sweep reading the old one sees the
    /// connections it held.
    /// </summary>
    /// <returns>
    /// The index of the slot.
    /// </returns>
    private int TakeUnusedSlotLocked()
    {
        if (_used == _slots.Length)
        {
            MqttWireConnection?[] slots = new MqttWireConnection?[_slots.Length * 2];
            _slots.CopyTo(slots, 0);
            Array.Resize(ref _free, slots.Length);
            Volatile.Write(ref _slots, slots);
        }

        return _used++;
    }
}
