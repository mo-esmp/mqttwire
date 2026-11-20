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
    private CancellationTokenRegistration _registration;

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

            if (_dataCount > 0 || _ended)
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
        lock (_gate)
        {
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

            if (IsAborted)
            {
                return ValueTask.FromException(new OperationCanceledException());
            }

            AppendWritten(packet.Span);
            return default;
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
        lock (_gate)
        {
            ClosedWith = status;
        }

        return default;
    }

    /// <summary>
    /// Records the abort and ends a pending wait or read with <see cref="OperationCanceledException"/>, as
    /// <see cref="WebSocket.Abort"/> does. Held writes stay pending until <see cref="CompleteHeldWrites"/>.
    /// </summary>
    public void Abort()
    {
        CancellationTokenRegistration registration;
        lock (_gate)
        {
            IsAborted = true;
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
    /// Appends a written packet to the written bytes and counts it. The caller holds the lock.
    /// </summary>
    /// <param name="bytes">
    /// The packet.
    /// </param>
    private void AppendWritten(ReadOnlySpan<byte> bytes)
    {
        if (_writtenCount + bytes.Length > _written.Length)
        {
            Array.Resize(ref _written, Math.Max(_written.Length * 2, _writtenCount + bytes.Length));
        }

        bytes.CopyTo(_written.AsSpan(_writtenCount));
        _writtenCount += bytes.Length;
        WriteCount++;
    }
}
