using System.Net.WebSockets;
using System.Threading.Tasks.Sources;
using MqttWire.Link;

namespace MqttWire.Tests.Link;

/// <summary>
/// An in-memory <see cref="IMqttLink"/> for tests. Pending operations complete synchronously on the calling
/// thread (continuations run inline), so a test drives the client deterministically and an allocation test can
/// measure the current thread. It is safe to write to from several threads at once, and in steady state it
/// allocates nothing.
/// </summary>
internal sealed class MemoryLink : IMqttLink, IValueTaskSource, IValueTaskSource<int>
{
    /// <summary>
    /// What the single pending read-side operation is.
    /// </summary>
    private enum Pending
    {
        /// <summary>
        /// No read-side operation is pending.
        /// </summary>
        None,

        /// <summary>
        /// A <see cref="WaitForDataAsync"/> is pending.
        /// </summary>
        Wait,

        /// <summary>
        /// A <see cref="ReadAsync"/> is pending.
        /// </summary>
        Read,
    }

    /// <summary>
    /// A write that was issued while writes were held.
    /// </summary>
    /// <param name="Buffer">
    /// The packet the write was given.
    /// </param>
    /// <param name="Completion">
    /// Completes the write.
    /// </param>
    private readonly record struct HeldWrite(ReadOnlyMemory<byte> Buffer, TaskCompletionSource Completion);

    private readonly Lock _gate = new();
    private readonly Queue<HeldWrite> _held = new();
    private ManualResetValueTaskSourceCore<int> _core = new() { RunContinuationsAsynchronously = false };
    private byte[] _data = new byte[4096];
    private int _dataHead;
    private int _dataCount;
    private int[] _chunks = new int[64];
    private int _chunkHead;
    private int _chunkCount;
    private byte[] _written = new byte[4096];
    private int _writtenCount;
    private Pending _pending;
    private Memory<byte> _pendingBuffer;
    private bool _ended;
    private Exception? _readFailure;
    private Exception? _writeFailure;
    private bool _holdWrites;

    /// <summary>
    /// Whether <see cref="WaitForDataAsync"/> completes at once, so that the client's read itself is pending.
    /// </summary>
    private bool _skipWaits;

    /// <summary>
    /// Whether a write stays pending on <see cref="_reusableWrite"/> until <see cref="CompletePendingWrite"/>.
    /// </summary>
    private bool _pendWrites;

    /// <summary>
    /// The one source every write pends on in the mode of <see cref="PendWrites"/>, or <see langword="null"/> until
    /// that mode is first turned on, so a link that never uses it costs nothing more.
    /// </summary>
    private ReusableWrite? _reusableWrite;

    /// <summary>
    /// Whether a write is pending on <see cref="_reusableWrite"/>.
    /// </summary>
    private bool _writePending;

    /// <summary>
    /// The packet of the write that is pending on <see cref="_reusableWrite"/>.
    /// </summary>
    private ReadOnlyMemory<byte> _pendingWriteBuffer;

    /// <summary>
    /// Whether <see cref="Abort"/> leaves a pending wait or read pending, until <see cref="Deliver"/>,
    /// <see cref="EndFromPeer"/> or <see cref="FailReads"/> ends it.
    /// </summary>
    private bool _holdReadsOnAbort;

    /// <summary>
    /// What the next <see cref="Abort"/> throws instead of aborting, or <see langword="null"/> when it aborts.
    /// </summary>
    private Exception? _abortFailure;

    /// <summary>
    /// The number of calls of <see cref="WriteAsync"/> and <see cref="CloseAsync"/> that have begun and not yet
    /// returned to their caller.
    /// </summary>
    private int _beginning;

    /// <summary>
    /// Completes when <see cref="WriteCount"/> reaches <see cref="_writesWanted"/>, or <see langword="null"/> when
    /// no one waits.
    /// </summary>
    private TaskCompletionSource? _writesWaiter;

    /// <summary>
    /// The write count <see cref="_writesWaiter"/> waits for.
    /// </summary>
    private int _writesWanted;
    private CancellationTokenRegistration _registration;

    /// <summary>
    /// Gets or sets a callback that sees each packet as it is handed to <see cref="WriteAsync"/>, on the writer's
    /// thread and before the link takes it, so a test can check which memory the writer handed over, or act at
    /// the moment a write begins; or <see langword="null"/>.
    /// </summary>
    public Action<ReadOnlyMemory<byte>>? Writing { get; set; }

    /// <summary>
    /// Gets or sets a callback that runs at the start of every <see cref="Abort"/>, on the aborting thread and
    /// before the link is aborted, so a test can see what was true at the moment the abort was issued; or
    /// <see langword="null"/>.
    /// </summary>
    public Action? Aborting { get; set; }

    /// <summary>
    /// Gets or sets the task of whoever runs over the link (the server's run of the connection), looked at at the
    /// start of every <see cref="Abort"/>; or <see langword="null"/>.
    /// </summary>
    public Task? Run { get; set; }

    /// <summary>
    /// Gets or sets the checks this link reports to besides counting for itself, shared by the links of one harness
    /// so that the harness can assert on all of them without keeping them alive; or <see langword="null"/>.
    /// </summary>
    public MemoryLinkChecks? Checks { get; set; }

    /// <summary>
    /// Gets the number of calls of <see cref="Abort"/> that were made after <see cref="Run"/> had completed: over a
    /// WebSocket such an abort throws, so a server must never issue one.
    /// </summary>
    public int AbortsAfterEnd
    {
        get
        {
            lock (_gate)
            {
                return field;
            }
        }

        private set;
    }

    /// <summary>
    /// Gets the number of writes and closes that were begun while another write or close had not ended: one that the
    /// link holds (<see cref="HoldWrites"/>), or one whose call had not returned yet. A WebSocket takes one send at
    /// a time, so a correct user of a link never does this; the link only counts it, and goes on as before.
    /// </summary>
    public int OverlappingWrites
    {
        get
        {
            lock (_gate)
            {
                return field;
            }
        }

        private set;
    }

    /// <summary>
    /// Gets the number of writes and closes that were begun after <see cref="Abort"/>. A user that has aborted its
    /// link has nothing more to say on it; but one that writes on one thread while another aborts cannot always
    /// help beginning the one write it had already decided on, which then fails here as it does on a socket. So a
    /// single-threaded test expects 0, and any test expects at most 1, with nothing following it
    /// (<see cref="MemoryLinkChecks.FurtherWritesAfterAbort"/>). The link only counts.
    /// </summary>
    public int WritesAfterAbort
    {
        get
        {
            lock (_gate)
            {
                return field;
            }
        }

        private set;
    }

    /// <summary>
    /// Gets the number of calls of <see cref="Abort"/>, one that threw included.
    /// </summary>
    public int AbortCalls
    {
        get
        {
            lock (_gate)
            {
                return field;
            }
        }

        private set;
    }

    /// <summary>
    /// Gets or sets a value indicating whether a write that succeeds is only counted in <see cref="WriteCount"/>, its
    /// bytes not kept for <see cref="TakeWritten"/>; <see langword="false"/> by default. For a test that writes more
    /// than it could keep, and must not allocate while it measures.
    /// </summary>
    public bool DiscardsWritten
    {
        get
        {
            lock (_gate)
            {
                return field;
            }
        }

        set
        {
            lock (_gate)
            {
                field = value;
            }
        }
    }

    /// <summary>
    /// Gets the number of writes that completed successfully since the link was created.
    /// </summary>
    public int WriteCount
    {
        get
        {
            lock (_gate)
            {
                return field;
            }
        }

        private set;
    }

    /// <summary>
    /// Gets the buffer of the oldest held write, or an empty buffer when none is held.
    /// </summary>
    public ReadOnlyMemory<byte> HeldWriteBuffer
    {
        get
        {
            lock (_gate)
            {
                return _held.Count > 0 ? _held.Peek().Buffer : default;
            }
        }
    }

    /// <summary>
    /// Gets a value indicating whether a write is pending in the mode of <see cref="PendWrites"/>.
    /// </summary>
    public bool IsWritePending
    {
        get
        {
            lock (_gate)
            {
                return _writePending;
            }
        }
    }

    /// <summary>
    /// Gets the buffer the pending read fills, or an empty buffer when no read is pending.
    /// </summary>
    public Memory<byte> PendingReadBuffer
    {
        get
        {
            lock (_gate)
            {
                return _pending == Pending.Read ? _pendingBuffer : default;
            }
        }
    }

    /// <summary>
    /// Gets the status the link was closed with, or <see langword="null"/> when it was not closed.
    /// </summary>
    public WebSocketCloseStatus? ClosedWith
    {
        get
        {
            lock (_gate)
            {
                return field;
            }
        }

        private set;
    }

    /// <summary>
    /// Gets a value indicating whether <see cref="Abort"/> was called.
    /// </summary>
    public bool IsAborted
    {
        get
        {
            lock (_gate)
            {
                return field;
            }
        }

        private set;
    }

    /// <summary>
    /// Gets a value indicating whether a wait or a read is pending.
    /// </summary>
    public bool IsReadPending
    {
        get
        {
            lock (_gate)
            {
                return _pending != Pending.None;
            }
        }
    }

    /// <summary>
    /// Makes the bytes readable as one chunk; a pending wait or read completes now, on the calling thread.
    /// </summary>
    /// <param name="bytes">
    /// The bytes the client will read.
    /// </param>
    public void Deliver(ReadOnlySpan<byte> bytes)
    {
        if (bytes.IsEmpty)
        {
            return;
        }

        int result;
        CancellationTokenRegistration registration;
        lock (_gate)
        {
            Append(bytes);
            if (_pending == Pending.None)
            {
                return;
            }

            result = CompletePendingLocked();
            registration = TakeRegistrationLocked();
        }

        registration.Dispose();
        _core.SetResult(result);
    }

    /// <summary>
    /// Makes the bytes readable in chunks; no read returns bytes from two chunks.
    /// </summary>
    /// <param name="bytes">
    /// The bytes the client will read.
    /// </param>
    /// <param name="chunkSize">
    /// The size of each chunk; the last one may be smaller.
    /// </param>
    public void DeliverInChunks(ReadOnlySpan<byte> bytes, int chunkSize)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(chunkSize, 1);
        while (!bytes.IsEmpty)
        {
            int take = Math.Min(chunkSize, bytes.Length);
            Deliver(bytes[..take]);
            bytes = bytes[take..];
        }
    }

    /// <summary>
    /// Ends the link from the peer: once the delivered bytes are read, the next read returns 0.
    /// </summary>
    public void EndFromPeer()
    {
        int result;
        CancellationTokenRegistration registration;
        lock (_gate)
        {
            _ended = true;
            if (_pending == Pending.None || _dataCount > 0)
            {
                return;
            }

            result = CompletePendingLocked();
            registration = TakeRegistrationLocked();
        }

        registration.Dispose();
        _core.SetResult(result);
    }

    /// <summary>
    /// Makes every read, and a pending one, fail with the exception.
    /// </summary>
    /// <param name="exception">
    /// The exception the read throws.
    /// </param>
    public void FailReads(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        CancellationTokenRegistration registration;
        lock (_gate)
        {
            _readFailure = exception;
            if (_pending == Pending.None)
            {
                return;
            }

            _pending = Pending.None;
            registration = TakeRegistrationLocked();
        }

        registration.Dispose();
        _core.SetException(exception);
    }

    /// <summary>
    /// Makes every later write fail with the exception.
    /// </summary>
    /// <param name="exception">
    /// The exception the write throws.
    /// </param>
    public void FailWrites(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        lock (_gate)
        {
            _writeFailure = exception;
        }
    }

    /// <summary>
    /// Takes everything written since the last call, concatenated.
    /// </summary>
    /// <returns>
    /// The written bytes; empty when nothing was written.
    /// </returns>
    public byte[] TakeWritten()
    {
        lock (_gate)
        {
            byte[] result = _written.AsSpan(0, _writtenCount).ToArray();
            _writtenCount = 0;
            return result;
        }
    }

    /// <summary>
    /// Waits until the link has taken a number of writes in all, for writes that a send lock hands to the thread
    /// pool. One waiter at a time.
    /// </summary>
    /// <param name="count">
    /// The value <see cref="WriteCount"/> must reach.
    /// </param>
    /// <returns>
    /// A task that completes, never inline, once that many writes were taken; it fails with
    /// <see cref="TimeoutException"/> when they are not taken within <see cref="BoundedWaits.Limit"/>.
    /// </returns>
    public Task WhenWrittenAsync(int count)
    {
        lock (_gate)
        {
            if (WriteCount >= count)
            {
                return Task.CompletedTask;
            }

            _writesWanted = count;
            _writesWaiter = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            return _writesWaiter.Task.Within($"Write {count} on the link");
        }
    }

    /// <summary>
    /// Makes later writes stay pending until <see cref="CompleteHeldWrites"/>.
    /// </summary>
    public void HoldWrites()
    {
        lock (_gate)
        {
            _holdWrites = true;
        }
    }

    /// <summary>
    /// Makes later writes stay pending, one at a time, each until <see cref="CompletePendingWrite"/> ends it. Unlike
    /// <see cref="HoldWrites"/>, which gives every held write a task of its own, every write here pends on one source
    /// that is reused, so a pending write allocates nothing in the link: for a test that measures what a write that
    /// does not complete at once costs its writer. One write at a time is all a WebSocket takes; a second one begun
    /// while one is pending is counted in <see cref="OverlappingWrites"/> and fails. Whoever awaits a write must take
    /// its result before it begins the next, as an <c>await</c> does.
    /// </summary>
    public void PendWrites()
    {
        lock (_gate)
        {
            _reusableWrite ??= new ReusableWrite();
            _pendWrites = true;
        }
    }

    /// <summary>
    /// Ends the write that is pending in the mode of <see cref="PendWrites"/>, if there is one; later writes still
    /// pend. What awaits the write carries on here, on the calling thread, before this returns, whatever
    /// synchronization context the thread has. A write that succeeds counts towards <see cref="WriteCount"/> and
    /// <see cref="TakeWritten"/>.
    /// </summary>
    /// <param name="exception">
    /// The exception the write fails with, or <see langword="null"/> to complete it successfully.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when a write was pending and has been ended; <see langword="false"/> when none was,
    /// and nothing changed.
    /// </returns>
    public bool CompletePendingWrite(Exception? exception = null)
    {
        ReusableWrite write;
        lock (_gate)
        {
            if (!_writePending || _reusableWrite is null)
            {
                return false;
            }

            write = _reusableWrite;
            _writePending = false;
            if (exception is null)
            {
                AppendWritten(_pendingWriteBuffer.Span);
            }

            _pendingWriteBuffer = default;
        }

        if (exception is null)
        {
            write.Succeed();
        }
        else
        {
            write.Fail(exception);
        }

        return true;
    }

    /// <summary>
    /// Makes every later <see cref="WaitForDataAsync"/> complete at once, so that the read that follows it stays
    /// pending, holding its buffer, until data comes or the read is ended.
    /// </summary>
    public void SkipWaits()
    {
        lock (_gate)
        {
            _skipWaits = true;
        }
    }

    /// <summary>
    /// Makes the next <see cref="Abort"/>, and only that one, throw instead of aborting, as the abort of a WebSocket
    /// does whose request has ended; the link is left as it was.
    /// </summary>
    /// <param name="exception">
    /// The exception the next abort throws.
    /// </param>
    public void FailNextAbort(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        lock (_gate)
        {
            _abortFailure = exception;
        }
    }

    /// <summary>
    /// Makes <see cref="Abort"/> leave a pending wait or read pending, as a link may whose abort ends its read only
    /// later; <see cref="Deliver"/>, <see cref="EndFromPeer"/> or <see cref="FailReads"/> then ends it.
    /// </summary>
    public void HoldReadsOnAbort()
    {
        lock (_gate)
        {
            _holdReadsOnAbort = true;
        }
    }

    /// <summary>
    /// Completes every held write, oldest first, and stops holding later ones. A write that succeeds counts
    /// towards <see cref="WriteCount"/> and <see cref="TakeWritten"/>.
    /// </summary>
    /// <param name="exception">
    /// The exception the held writes fail with, or <see langword="null"/> to complete them successfully.
    /// </param>
    public void CompleteHeldWrites(Exception? exception = null)
    {
        while (true)
        {
            HeldWrite write;
            lock (_gate)
            {
                _holdWrites = false;
                if (!_held.TryDequeue(out write))
                {
                    return;
                }

                if (exception is null)
                {
                    AppendWritten(write.Buffer.Span);
                }
            }

            if (exception is null)
            {
                write.Completion.SetResult();
            }
            else
            {
                write.Completion.SetException(exception);
            }
        }
    }

    /// <summary>
    /// Completes when a read would not wait; a wait already satisfied completes at once.
    /// </summary>
    /// <param name="cancellationToken">
    /// An already cancelled token throws; one that fires while the operation is pending ends it with <see cref="OperationCanceledException"/>.
    /// </param>
    /// <returns>
    /// A task that completes when data has arrived or the link has ended.
    /// </returns>
    public ValueTask WaitForDataAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            if (_readFailure is not null)
            {
                return ValueTask.FromException(_readFailure);
            }

            if (IsAborted)
            {
                return ValueTask.FromException(new OperationCanceledException());
            }

            if (_dataCount > 0 || _ended || _skipWaits)
            {
                return default;
            }

            BeginPendingLocked(Pending.Wait, default, cancellationToken);
            return new ValueTask(this, _core.Version);
        }
    }

    /// <summary>
    /// Reads at most one delivered chunk; when none is available the read stays pending.
    /// </summary>
    /// <param name="buffer">
    /// Receives the bytes.
    /// </param>
    /// <param name="cancellationToken">
    /// An already cancelled token throws; one that fires while the operation is pending ends it with <see cref="OperationCanceledException"/>.
    /// </param>
    /// <returns>
    /// The number of bytes read, or 0 after <see cref="EndFromPeer"/>.
    /// </returns>
    public ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            if (_readFailure is not null)
            {
                return ValueTask.FromException<int>(_readFailure);
            }

            if (IsAborted)
            {
                return ValueTask.FromException<int>(new OperationCanceledException());
            }

            if (_dataCount > 0)
            {
                return new ValueTask<int>(TakeChunk(buffer.Span));
            }

            if (_ended)
            {
                return new ValueTask<int>(0);
            }

            BeginPendingLocked(Pending.Read, buffer, cancellationToken);
            return new ValueTask<int>(this, _core.Version);
        }
    }

    /// <summary>
    /// Records the packet; the write completes at once unless writes are held.
    /// </summary>
    /// <param name="packet">
    /// The packet to write.
    /// </param>
    /// <param name="cancellationToken">
    /// Observed only on entry: an already cancelled token throws.
    /// </param>
    /// <returns>
    /// A task that completes when the write completes.
    /// </returns>
    public ValueTask WriteAsync(ReadOnlyMemory<byte> packet, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        NoteBeginning();
        try
        {
            Writing?.Invoke(packet);
            lock (_gate)
            {
                // A write begun after the abort fails at once, as on a socket, also when writes are held. A write
                // that was held before the abort is the test's to end, as it chooses: that is deliberately more
                // forgiving than a socket, whose pending write an abort fails.
                if (IsAborted)
                {
                    return ValueTask.FromException(new OperationCanceledException());
                }

                if (_pendWrites && _reusableWrite is { } reusable)
                {
                    if (_writePending)
                    {
                        // Counted as overlapping where it began; the one source cannot carry two writes.
                        return ValueTask.FromException(
                            new InvalidOperationException("A write is already pending on the link."));
                    }

                    _writePending = true;
                    _pendingWriteBuffer = packet;
                    return new ValueTask(reusable, reusable.Version);
                }

                if (_holdWrites)
                {
                    TaskCompletionSource completion = new();
                    _held.Enqueue(new HeldWrite(packet, completion));
                    return new ValueTask(completion.Task);
                }

                if (_writeFailure is not null)
                {
                    return ValueTask.FromException(_writeFailure);
                }

                AppendWritten(packet.Span);
                return default;
            }
        }
        finally
        {
            Interlocked.Decrement(ref _beginning);
        }
    }

    /// <summary>
    /// Records the close status.
    /// </summary>
    /// <param name="status">
    /// The status the peer would see.
    /// </param>
    /// <param name="cancellationToken">
    /// Observed only on entry: an already cancelled token throws.
    /// </param>
    /// <returns>
    /// A completed task.
    /// </returns>
    public ValueTask CloseAsync(WebSocketCloseStatus status, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        NoteBeginning();
        try
        {
            lock (_gate)
            {
                ClosedWith = status;
            }

            return default;
        }
        finally
        {
            Interlocked.Decrement(ref _beginning);
        }
    }

    /// <summary>
    /// Counts a write or a close that begins: as overlapping when another has begun and not returned, or is held
    /// or pending;
    /// and as after the abort when the link was aborted, the second and later ones on this link also in the shared
    /// checks. The caller takes the count of beginnings down again when its call returns.
    /// </summary>
    private void NoteBeginning()
    {
        bool overlapping = Interlocked.Increment(ref _beginning) > 1;
        lock (_gate)
        {
            if (overlapping || _held.Count > 0 || _writePending)
            {
                OverlappingWrites++;
                Checks?.NoteOverlappingWrite();
            }

            if (IsAborted)
            {
                WritesAfterAbort++;
                if (WritesAfterAbort > 1)
                {
                    Checks?.NoteFurtherWriteAfterAbort();
                }
            }
        }
    }

    /// <summary>
    /// Records the abort and ends a pending wait or read with <see cref="OperationCanceledException"/>, as
    /// <see cref="WebSocket.Abort"/> does, unless <see cref="HoldReadsOnAbort"/> was called. Held writes stay
    /// pending until <see cref="CompleteHeldWrites"/>.
    /// </summary>
    /// <exception cref="Exception">
    /// <see cref="FailNextAbort"/> chose this call: the call is counted and nothing else changes.
    /// </exception>
    public void Abort()
    {
        Aborting?.Invoke();
        bool ended = Run?.IsCompleted ?? false;
        CancellationTokenRegistration registration;
        lock (_gate)
        {
            AbortCalls++;
            if (ended)
            {
                AbortsAfterEnd++;
                Checks?.NoteAbortAfterEnd();
            }

            if (_abortFailure is { } failure)
            {
                _abortFailure = null;
                throw failure;
            }

            IsAborted = true;
            if (_pending == Pending.None || _holdReadsOnAbort)
            {
                return;
            }

            _pending = Pending.None;
            registration = TakeRegistrationLocked();
        }

        registration.Dispose();
        _core.SetException(new OperationCanceledException());
    }

    /// <summary>
    /// Ends a pending wait or read because its cancellation token fired.
    /// </summary>
    private void CancelPending()
    {
        CancellationTokenRegistration registration;
        lock (_gate)
        {
            if (_pending == Pending.None)
            {
                return;
            }

            _pending = Pending.None;
            registration = TakeRegistrationLocked();
        }

        registration.Dispose();
        _core.SetException(new OperationCanceledException());
    }

    /// <summary>
    /// Takes the cancellation registration of the pending operation, to be disposed outside the lock.
    /// The caller holds the lock.
    /// </summary>
    /// <returns>
    /// The registration, or a default one when the operation has none.
    /// </returns>
    private CancellationTokenRegistration TakeRegistrationLocked()
    {
        CancellationTokenRegistration registration = _registration;
        _registration = default;
        return registration;
    }

    /// <inheritdoc/>
    void IValueTaskSource.GetResult(short token) => GetResultCore(token);

    /// <inheritdoc/>
    int IValueTaskSource<int>.GetResult(short token) => GetResultCore(token);

    /// <inheritdoc/>
    ValueTaskSourceStatus IValueTaskSource.GetStatus(short token) => _core.GetStatus(token);

    /// <inheritdoc/>
    ValueTaskSourceStatus IValueTaskSource<int>.GetStatus(short token) => _core.GetStatus(token);

    /// <inheritdoc/>
    void IValueTaskSource.OnCompleted(
        Action<object?> continuation,
        object? state,
        short token,
        ValueTaskSourceOnCompletedFlags flags)
        => _core.OnCompleted(continuation, state, token, flags);

    /// <inheritdoc/>
    void IValueTaskSource<int>.OnCompleted(
        Action<object?> continuation,
        object? state,
        short token,
        ValueTaskSourceOnCompletedFlags flags)
        => _core.OnCompleted(continuation, state, token, flags);

    /// <summary>
    /// Gets the result of the pending operation and readies the source for the next one.
    /// </summary>
    /// <param name="token">
    /// The version token of the operation.
    /// </param>
    /// <returns>
    /// The operation's result.
    /// </returns>
    private int GetResultCore(short token)
    {
        try
        {
            return _core.GetResult(token);
        }
        finally
        {
            _core.Reset();
        }
    }

    /// <summary>
    /// Marks a read-side operation pending. The caller holds the lock.
    /// </summary>
    /// <param name="kind">
    /// The kind of operation.
    /// </param>
    /// <param name="buffer">
    /// The buffer a read fills when data arrives.
    /// </param>
    /// <param name="cancellationToken">
    /// Ends the operation with <see cref="OperationCanceledException"/> when it fires while pending.
    /// </param>
    /// <exception cref="InvalidOperationException">
    /// Another wait or read is already pending.
    /// </exception>
    private void BeginPendingLocked(Pending kind, Memory<byte> buffer, CancellationToken cancellationToken)
    {
        if (_pending != Pending.None)
        {
            throw new InvalidOperationException("A wait or read is already pending.");
        }

        _pending = kind;
        _pendingBuffer = buffer;
        if (cancellationToken.CanBeCanceled)
        {
            // The state is always this link, passed just below. If the token fires before Register returns, the
            // callback runs inline on this thread (the lock is re-entrant) before any continuation is attached.
            _registration = cancellationToken.UnsafeRegister(static state => ((MemoryLink)state!).CancelPending(), this);
        }
    }

    /// <summary>
    /// Takes the pending operation off the link and works out its result. The caller holds the lock and, after
    /// releasing it, completes the source with the result.
    /// </summary>
    /// <returns>
    /// The result: the bytes copied for a read, or 0 for a wait or an end.
    /// </returns>
    private int CompletePendingLocked()
    {
        Pending kind = _pending;
        _pending = Pending.None;
        if (kind == Pending.Read && _dataCount > 0)
        {
            int count = TakeChunk(_pendingBuffer.Span);
            _pendingBuffer = default;
            return count;
        }

        return 0;
    }

    /// <summary>
    /// Appends one chunk to the incoming ring, growing it when full. The caller holds the lock.
    /// </summary>
    /// <param name="bytes">
    /// The chunk.
    /// </param>
    private void Append(ReadOnlySpan<byte> bytes)
    {
        if (_dataCount + bytes.Length > _data.Length)
        {
            byte[] bigger = new byte[Math.Max(_data.Length * 2, _dataCount + bytes.Length)];
            CopyOut(bigger);
            _data = bigger;
            _dataHead = 0;
        }

        int tail = (_dataHead + _dataCount) % _data.Length;
        int first = Math.Min(bytes.Length, _data.Length - tail);
        bytes[..first].CopyTo(_data.AsSpan(tail));
        bytes[first..].CopyTo(_data);
        _dataCount += bytes.Length;

        if (_chunkCount == _chunks.Length)
        {
            int[] biggerChunks = new int[_chunks.Length * 2];
            for (int i = 0; i < _chunkCount; i++)
            {
                biggerChunks[i] = _chunks[(_chunkHead + i) % _chunks.Length];
            }

            _chunks = biggerChunks;
            _chunkHead = 0;
        }

        _chunks[(_chunkHead + _chunkCount) % _chunks.Length] = bytes.Length;
        _chunkCount++;
    }

    /// <summary>
    /// Copies the unread bytes, in order, to the start of the destination. The caller holds the lock.
    /// </summary>
    /// <param name="destination">
    /// The destination, at least <c>_dataCount</c> long.
    /// </param>
    private void CopyOut(byte[] destination)
    {
        int first = Math.Min(_dataCount, _data.Length - _dataHead);
        _data.AsSpan(_dataHead, first).CopyTo(destination);
        _data.AsSpan(0, _dataCount - first).CopyTo(destination.AsSpan(first));
    }

    /// <summary>
    /// Takes up to a buffer's worth of bytes from the oldest chunk. The caller holds the lock.
    /// </summary>
    /// <param name="destination">
    /// Receives the bytes.
    /// </param>
    /// <returns>
    /// The number of bytes taken.
    /// </returns>
    private int TakeChunk(Span<byte> destination)
    {
        int take = Math.Min(destination.Length, _chunks[_chunkHead]);
        int first = Math.Min(take, _data.Length - _dataHead);
        _data.AsSpan(_dataHead, first).CopyTo(destination);
        _data.AsSpan(0, take - first).CopyTo(destination[first..]);
        _dataHead = (_dataHead + take) % _data.Length;
        _dataCount -= take;
        _chunks[_chunkHead] -= take;
        if (_chunks[_chunkHead] == 0)
        {
            _chunkHead = (_chunkHead + 1) % _chunks.Length;
            _chunkCount--;
        }

        return take;
    }

    /// <summary>
    /// The one source the writes of a link pend on in the mode of <see cref="PendWrites"/>: completed by the test,
    /// with what awaits it carrying on inline, and ready for the next write as soon as its result has been taken.
    /// </summary>
    private sealed class ReusableWrite : IValueTaskSource
    {
        /// <summary>
        /// The state of the write that pends now, or of the last one.
        /// </summary>
        private ManualResetValueTaskSourceCore<bool> _core = new() { RunContinuationsAsynchronously = false };

        /// <summary>
        /// Gets the version of the write that pends next, to hand out with its task.
        /// </summary>
        public short Version => _core.Version;

        /// <summary>
        /// Completes the pending write successfully; what awaits it carries on before this returns.
        /// </summary>
        public void Succeed() => _core.SetResult(result: true);

        /// <summary>
        /// Fails the pending write; what awaits it carries on before this returns.
        /// </summary>
        /// <param name="exception">
        /// The exception the write throws.
        /// </param>
        public void Fail(Exception exception) => _core.SetException(exception);

        /// <inheritdoc/>
        public ValueTaskSourceStatus GetStatus(short token) => _core.GetStatus(token);

        /// <inheritdoc/>
        public void OnCompleted(Action<object?> continuation, object? state, short token,
            ValueTaskSourceOnCompletedFlags flags) =>
            _core.OnCompleted(continuation, state, token, flags);

        /// <summary>
        /// Takes the result of the write and readies the source for the next one.
        /// </summary>
        /// <param name="token">
        /// The version token of the write.
        /// </param>
        public void GetResult(short token)
        {
            try
            {
                _core.GetResult(token);
            }
            finally
            {
                _core.Reset();
            }
        }
    }

    /// <summary>
    /// Appends a written packet to the written bytes, unless <see cref="DiscardsWritten"/> is set, and counts it. The
    /// caller holds the lock.
    /// </summary>
    /// <param name="bytes">
    /// The packet.
    /// </param>
    private void AppendWritten(ReadOnlySpan<byte> bytes)
    {
        if (DiscardsWritten)
        {
            bytes = [];
        }

        if (_writtenCount + bytes.Length > _written.Length)
        {
            Array.Resize(ref _written, Math.Max(_written.Length * 2, _writtenCount + bytes.Length));
        }

        bytes.CopyTo(_written.AsSpan(_writtenCount));
        _writtenCount += bytes.Length;
        WriteCount++;
        if (_writesWaiter is not null && WriteCount >= _writesWanted)
        {
            _writesWaiter.SetResult();
            _writesWaiter = null;
        }
    }
}
