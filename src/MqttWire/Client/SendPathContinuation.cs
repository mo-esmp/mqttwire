using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace MqttWire.Client;

/// <summary>
/// What carries the client's send path on when a write does not complete at once, in place of an async method,
/// which would allocate its state for every such write; and the work item that carries the path on from the thread
/// pool when a caller has written its share (<see cref="ClientLimits.InlineWrites"/>). A client has at most one write
/// in progress, so it needs one of these: it creates it on first need and keeps it (the client's send-path design,
/// section 5, "The object that waits for a write"). Modelled on the server's <c>PendingWrite</c>.
/// </summary>
/// <remarks>
/// It waits for one write at a time and needs no lock: whoever holds the send path arms it and registers its one
/// continuation with the write (<see cref="Await"/>); from that registration on, the waiting belongs to the
/// continuation, which runs once, on the thread that completes the write, and the thread that registered touches no
/// field of it again but the registration's token, which both sides change only atomically. The registration
/// publishes the fields to the continuation's thread. The continuation is registered without the caller's execution
/// context (<c>UnsafeOnCompleted</c>): carrying the send path on is not the caller's call, and capturing the context
/// would allocate. As a work item it reads no field of its own, so it may be queued while it also waits for a write
/// that its own run starts.
/// </remarks>
internal sealed class SendPathContinuation : IThreadPoolWorkItem
{
    /// <summary>
    /// The value of <see cref="_registration"/> while nothing is registered, or once the continuation has run.
    /// </summary>
    private const int NotRegistered = 0;

    /// <summary>
    /// The value of <see cref="_registration"/> once the write refused the registration before the continuation ran.
    /// </summary>
    private const int Refused = -1;

    /// <summary>
    /// The object whose continuation the current thread is registering at this moment, or <see langword="null"/>: set
    /// around the registration and nowhere else. A write that completes while it is being registered may have its
    /// continuation run inside the registration, on the stack of the caller that started the write; the continuation
    /// finds itself here and then counts as one more of that caller's writes, so that such writes cannot nest without
    /// a bound. Per thread, so it costs a client nothing and is never read across threads.
    /// </summary>
    [ThreadStatic]
    private static SendPathContinuation? t_registering;

    /// <summary>
    /// The client whose send path this carries on.
    /// </summary>
    private readonly MqttWireClient _client;

    /// <summary>
    /// The one continuation, <see cref="Continue"/>, created once with the object, so that registering it allocates
    /// nothing.
    /// </summary>
    private readonly Action _continuation;

    /// <summary>
    /// The awaiter of the write, from which the continuation takes the write's outcome.
    /// </summary>
    private ConfiguredValueTaskAwaitable.ConfiguredValueTaskAwaiter _awaiter;

    /// <summary>
    /// The write awaited: its buffer, returned when the write has ended, and what its end has to do.
    /// </summary>
    private QueuedWrite _write;

    /// <summary>
    /// How many writes that complete at once the caller that started the write still makes on its thread; used only
    /// when the continuation runs inside the registration.
    /// </summary>
    private int _inlineWrites;

    /// <summary>
    /// Whether the object awaits a write; cleared by the continuation before it hands the write's end to the client.
    /// </summary>
    private bool _armed;

    /// <summary>
    /// The token of the registration whose continuation has not run yet; <see cref="NotRegistered"/> before the
    /// first and once the continuation has run; <see cref="Refused"/> once the write refused the registration before
    /// its continuation ran. Changed with <see cref="Interlocked"/> by the registering thread and the continuation,
    /// so that exactly one of them decides who carries the send path on.
    /// </summary>
    private int _registration;

    /// <summary>
    /// The token of the latest registration, counted by <see cref="Await"/> alone, so that a refusal can never be
    /// taken for the refusal of a later registration of the same object.
    /// </summary>
    private int _registrations;

    /// <summary>
    /// Creates the object, which awaits nothing yet.
    /// </summary>
    /// <param name="client">
    /// The client whose send path it carries on.
    /// </param>
    public SendPathContinuation(MqttWireClient client)
    {
        _client = client;
        _continuation = Continue;
    }

    /// <summary>
    /// Waits for a write that has not completed: arms the object and registers its continuation with the write, then
    /// returns at once. When the write has ended, the continuation takes its outcome, clears the object and hands the
    /// rest to the client (<see cref="MqttWireClient.ContinueSendPath"/>), which returns the buffer and carries the
    /// send path on. The caller holds the send path and gives it to the write with this call; it must not touch the object
    /// as a continuation again, whether this returns or throws.
    /// </summary>
    /// <param name="write">
    /// The write in progress; it is awaited here and nowhere else.
    /// </param>
    /// <param name="queued">
    /// The write's buffer and what its end has to do.
    /// </param>
    /// <param name="inlineWrites">
    /// How many writes that complete at once the caller that started the write still makes on its thread.
    /// </param>
    /// <exception cref="Exception">
    /// The write refused the registration (a fault of the link) and its continuation has not run. The object stays
    /// armed: should the write end all the same, its continuation still finds what it needs and only ends the write
    /// (<see cref="MqttWireClient.EndRefusedWrite"/>), without carrying the send path on: the caller faults the path,
    /// drops what waits and gives it up.
    /// A write that refused the registration after its continuation had run is not reported: that continuation has
    /// already carried the send path on.
    /// </exception>
    public void Await(ValueTask write, in QueuedWrite queued, int inlineWrites)
    {
        Debug.Assert(!_armed, "The object awaits one write at a time.");
        ConfiguredValueTaskAwaitable.ConfiguredValueTaskAwaiter awaiter = write.ConfigureAwait(false).GetAwaiter();
        _awaiter = awaiter;
        _write = queued;
        _inlineWrites = inlineWrites;
        _armed = true;
        int token = _registrations == int.MaxValue ? 1 : _registrations + 1;
        _registrations = token;
        Volatile.Write(ref _registration, token);

        // From the registration on the object is the continuation's, perhaps on another thread already: the copies in
        // the locals are used, and no field is read or written here again but the token, atomically.
        Action continuation = _continuation;
        SetRegistering(this);
        try
        {
            awaiter.UnsafeOnCompleted(continuation);
        }
        catch (Exception) when (Interlocked.CompareExchange(ref _registration, Refused, token) == token)
        {
            // The continuation has not run, and from now on it will only end the write: the caller ends the
            // connection and keeps the send path.
            throw;
        }
#pragma warning disable RCS1075 // Swallowed on purpose: the write ended and its continuation carried the path on.
        catch (Exception)
        {
            // The link threw out of the registration after it had run the continuation, or while it ran it on another
            // thread: the write has ended, and its continuation holds the send path now. Nothing is left to do here.
        }
#pragma warning restore RCS1075
        finally
        {
            SetRegistering(registering: null);
        }
    }

    /// <summary>
    /// Carries the send path on from the thread pool, after a caller handed it over still held: takes the next
    /// waiting write and writes on for as long as the writes complete at once
    /// (<see cref="MqttWireClient.RunHandedOverSendPath"/>).
    /// </summary>
    void IThreadPoolWorkItem.Execute() => _client.RunHandedOverSendPath();

    /// <summary>
    /// Notes, for the current thread, the object whose continuation it is registering, or that it is registering
    /// none.
    /// </summary>
    /// <param name="registering">
    /// The object, or <see langword="null"/>.
    /// </param>
    private static void SetRegistering(SendPathContinuation? registering) => t_registering = registering;

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
    private static bool TryLeaveRegistration(SendPathContinuation completion)
    {
        if (!ReferenceEquals(t_registering, completion))
        {
            return false;
        }

        t_registering = null;
        return true;
    }

    /// <summary>
    /// The continuation of the write: runs once, when the write has ended, on the thread that completed it (or, for a
    /// write that completed while it was being registered, perhaps inside the registration). It takes the outcome, a
    /// write that threw being a failed write; clears the object, so that the client can arm it again for the next
    /// write; and hands the rest to the client. When the write refused the registration before this ran, the send
    /// path is no longer this write's: the client only ends the write. Nothing is thrown from it but what the client
    /// throws, a bug of the client.
    /// </summary>
    private void Continue()
    {
        // A write that completed while its continuation was being registered may run it at once, on the stack of the
        // caller that started the write: that caller is then this thread, inside Await.
        bool insideTheRegistration = TryLeaveRegistration(this);
        if (!_armed)
        {
            // Run a second time by a link that broke its contract: the first run took everything.
            return;
        }

        ConfiguredValueTaskAwaitable.ConfiguredValueTaskAwaiter awaiter = _awaiter;
        QueuedWrite write = _write;
        int inlineWrites = _inlineWrites;
        Exception? failure = null;
        try
        {
            // Also lets the link reuse what backs the write's task.
            awaiter.GetResult();
        }
        catch (Exception exception)
        {
            failure = exception;
        }

        _awaiter = default;
        _write = default;
        _inlineWrites = 0;
        _armed = false;
        if (Interlocked.Exchange(ref _registration, NotRegistered) == Refused)
        {
            _client.EndRefusedWrite(write, failure);
            return;
        }

        _client.ContinueSendPath(write, failure, insideTheRegistration, inlineWrites);
    }
}
