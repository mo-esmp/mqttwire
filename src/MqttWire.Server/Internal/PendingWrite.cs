using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace MqttWire.Server.Internal;

/// <summary>
/// What waits for one write that did not complete at once, in place of an async method: an async method would
/// allocate its state and a task for every such write, which under back-pressure is every write and every piece of a
/// large PUBLISH. A connection takes one from the server's pool only when a write is in fact pending
/// (<see cref="ServerPools.PendingWrites"/>), and the object gives itself back, cleared, as soon as the write has
/// ended and before the send path is carried on, so the pending writes of one drain reuse one object and steady
/// traffic allocates none.
/// </summary>
/// <remarks>
/// It is used by one write at a time and needs no lock: whoever holds the connection's send path arms it and
/// registers its one continuation with the write (<see cref="Await"/>); from that registration on, the object
/// belongs to the continuation, which runs once, on the thread that completes the write, and the thread that
/// registered touches no field of it again. The registration publishes the fields to that thread. The continuation is
/// registered without the execution context of the caller (<c>UnsafeOnCompleted</c>): the drain it runs is not the
/// host's call, and capturing the context would allocate.
/// </remarks>
internal sealed class PendingWrite
{
    /// <summary>
    /// The object whose continuation the current thread is registering at this moment, or <see langword="null"/>:
    /// set around the registration and nowhere else. A write that completes while it is being registered may have
    /// its continuation run inside the registration, on the stack of the caller that started the write; the
    /// continuation finds itself here and then counts as one of that caller's writes, so that such writes cannot
    /// nest without a bound. Per thread, so it costs a connection nothing and is never read across threads.
    /// </summary>
    [ThreadStatic]
    private static PendingWrite? t_registering;

    /// <summary>
    /// The one continuation, <see cref="Continue"/>, created once with the object, so that registering it allocates
    /// nothing.
    /// </summary>
    private readonly Action _continuation;

    /// <summary>
    /// The connection whose write is awaited; <see langword="null"/> while the object awaits nothing.
    /// </summary>
    private MqttWireConnection? _connection;

    /// <summary>
    /// The awaiter of the write, from which the continuation takes the write's outcome.
    /// </summary>
    private ConfiguredValueTaskAwaitable.ConfiguredValueTaskAwaiter _awaiter;

    /// <summary>
    /// The rented buffer the write was given, which the connection returns when the write has ended, or
    /// <see langword="null"/> for a piece of the host's memory.
    /// </summary>
    private byte[]? _buffer;

    /// <summary>
    /// How many writes that complete at once the caller that started the write still makes on its thread; used only
    /// when the continuation runs inside the registration.
    /// </summary>
    private int _inlineWrites;

    /// <summary>
    /// Creates an object that awaits nothing.
    /// </summary>
    public PendingWrite() => _continuation = Continue;

    /// <summary>
    /// Gets a value indicating whether the object awaits nothing and holds no reference: as it is created, and as it
    /// goes back to the pool.
    /// </summary>
    public bool IsIdle => _connection is null && _buffer is null;

    /// <summary>
    /// Waits for a write: arms the object and registers its continuation with the write, then returns at once. When
    /// the write has ended, the continuation takes its outcome, clears the object, and hands everything to the
    /// connection (<see cref="MqttWireConnection.EndPendingWrite"/>), which gives the object back to the pool and
    /// carries the send path on. The caller must not use the object after this call, whether it returns or throws.
    /// </summary>
    /// <param name="connection">
    /// The connection whose send path the caller holds.
    /// </param>
    /// <param name="write">
    /// The write in progress; it is awaited here and nowhere else.
    /// </param>
    /// <param name="buffer">
    /// The rented buffer the write was given, or <see langword="null"/> for a piece of the host's memory.
    /// </param>
    /// <param name="inlineWrites">
    /// How many writes that complete at once the caller that started the write still makes on its thread.
    /// </param>
    /// <exception cref="Exception">
    /// The write refused the registration (a fault of the link). The object stays armed and is not given back to
    /// the pool: should the write end all the same, its continuation still finds what it needs; otherwise the object
    /// is left to the garbage collector, never to another connection.
    /// </exception>
    public void Await(MqttWireConnection connection, ValueTask write, byte[]? buffer, int inlineWrites)
    {
        Debug.Assert(IsIdle, "The object awaits one write at a time.");
        ConfiguredValueTaskAwaitable.ConfiguredValueTaskAwaiter awaiter = write.ConfigureAwait(false).GetAwaiter();
        _connection = connection;
        _awaiter = awaiter;
        _buffer = buffer;
        _inlineWrites = inlineWrites;

        // From the registration on the object is the continuation's, perhaps on another thread already: the copies in
        // the locals are used, and no field is read or written here again.
        Action continuation = _continuation;
        SetRegistering(this);
        try
        {
            awaiter.UnsafeOnCompleted(continuation);
        }
        finally
        {
            SetRegistering(registering: null);
        }
    }

    /// <summary>
    /// Notes, for the current thread, the object whose continuation it is registering, or that it is registering
    /// none.
    /// </summary>
    /// <param name="registering">
    /// The object, or <see langword="null"/>.
    /// </param>
    private static void SetRegistering(PendingWrite? registering) => t_registering = registering;

    /// <summary>
    /// Tells whether the current thread is registering the continuation of an object at this moment, and when it is,
    /// forgets it, so that nothing that runs from here on takes itself to be inside that registration.
    /// </summary>
    /// <param name="completion">
    /// The object whose continuation is running.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when the continuation runs inside its own registration, on the stack of the caller that
    /// started the write.
    /// </returns>
    private static bool TryLeaveRegistration(PendingWrite completion)
    {
        if (!ReferenceEquals(t_registering, completion))
        {
            return false;
        }

        t_registering = null;
        return true;
    }

    /// <summary>
    /// The continuation of the write: runs once, when the write has ended, on the thread that completed it (or on a
    /// thread-pool thread, or, for a write that completed while it was being registered, perhaps inside the
    /// registration). It takes the outcome, a write that threw being a failed write; clears the object, so that it
    /// can go back to the pool before anything else is written; and hands the rest to the connection. Nothing is
    /// thrown from it: what follows the outcome is the connection's, which catches whatever it does not expect.
    /// </summary>
    private void Continue()
    {
        // A write that completed while its continuation was being registered may run it at once, on the stack of the
        // caller that started the write: that caller is then this thread, inside Await.
        bool insideTheRegistration = TryLeaveRegistration(this);
        if (_connection is not { } connection)
        {
            // Run a second time by a link that broke its contract: the first run took everything.
            return;
        }

        ConfiguredValueTaskAwaitable.ConfiguredValueTaskAwaiter awaiter = _awaiter;
        byte[]? buffer = _buffer;
        int inlineWrites = _inlineWrites;
        bool failed = false;
        try
        {
            // Also lets the link reuse what backs the write's task.
            awaiter.GetResult();
        }
        catch (Exception)
        {
            failed = true;
        }

        _connection = null;
        _awaiter = default;
        _buffer = null;
        _inlineWrites = 0;
        connection.EndPendingWrite(this, buffer, failed, insideTheRegistration, inlineWrites);
    }
}
