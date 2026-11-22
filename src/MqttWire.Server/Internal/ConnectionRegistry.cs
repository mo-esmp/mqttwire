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
    /// Completes when no slot is taken, or <see langword="null"/> when no one waits.
    /// </summary>
    private TaskCompletionSource? _empty;

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
    /// Gets the slot array, read without the lock. An entry may be <see langword="null"/>, and a connection in an
    /// array that has since been replaced may already have ended; a reader must accept both.
    /// </summary>
    public MqttWireConnection?[] Slots => Volatile.Read(ref _slots);

    /// <summary>
    /// Gives a connection a slot, growing the array when every slot is taken.
    /// </summary>
    /// <param name="connection">
    /// The connection, which must not have a slot yet.
    /// </param>
    public void Add(MqttWireConnection connection)
    {
        lock (Gate)
        {
            int slot = _freeCount > 0 ? _free[--_freeCount] : TakeUnusedSlotLocked();
            connection.Slot = slot;
            _taken++;
            Volatile.Write(ref _slots[slot], connection);
        }
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
    /// the last slot taken. The table entry is not touched: <see cref="RemoveFromTable"/> removed it before.
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
            if (_taken == 0)
            {
                empty = _empty;
                _empty = null;
            }
        }

        empty?.TrySetResult();
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
    /// Waits until no slot is taken.
    /// </summary>
    /// <returns>
    /// A task that completes, never inline, when the last slot is freed; at once when none is taken now.
    /// </returns>
    public Task WhenEmptyAsync()
    {
        lock (Gate)
        {
            if (_taken == 0)
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
