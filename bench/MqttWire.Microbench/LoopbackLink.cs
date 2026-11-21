using System.Buffers;
using System.Buffers.Binary;
using System.Net.WebSockets;
using System.Threading.Tasks.Sources;
using MqttWire.Codec;
using MqttWire.Link;

namespace MqttWire.Microbench;

/// <summary>
/// An in-memory link that plays a minimal server for the client benchmarks: it answers CONNECT with an accepting
/// CONNACK, every QoS 1 PUBLISH with a PUBACK for its identifier and PINGREQ with PINGRESP, ignores every other
/// packet the client writes, and plays back whatever packet <see cref="Deliver"/> is given. It is also the
/// connector that hands itself to the client. A wait that is pending completes on the thread that supplies the
/// data, with its continuation run inline, so the read loop runs on that thread; writes complete at once. A PUBACK
/// is not handed over inside the write, as a real server's answer never is: a thread-pool work item hands it to
/// the read loop, so a publish awaits its acknowledgement for real. In steady state it allocates nothing.
/// </summary>
internal sealed class LoopbackLink : IMqttLink, IMqttLinkConnector, IValueTaskSource, IThreadPoolWorkItem
{
    /// <summary>
    /// The size of the buffer of bytes the client has not yet read.
    /// </summary>
    private const int IncomingCapacity = 4096;

    /// <summary>
    /// An accepting CONNACK without properties.
    /// </summary>
    private static readonly byte[] s_connAck = [0x20, 0x03, 0x00, 0x00, 0x00];

    /// <summary>
    /// Guards the incoming bytes and the pending wait.
    /// </summary>
    private readonly Lock _gate = new();

    /// <summary>
    /// The bytes the client has not yet read, from <see cref="_head"/> for <see cref="_count"/> bytes.
    /// </summary>
    private readonly byte[] _incoming = new byte[IncomingCapacity];

    /// <summary>
    /// Completes the pending wait; its continuation runs on the thread that completes it.
    /// </summary>
    private ManualResetValueTaskSourceCore<bool> _core = new() { RunContinuationsAsynchronously = false };

    /// <summary>
    /// Where the unread bytes start in <see cref="_incoming"/>.
    /// </summary>
    private int _head;

    /// <summary>
    /// The number of unread bytes.
    /// </summary>
    private int _count;

    /// <summary>
    /// Whether a <see cref="WaitForDataAsync"/> is pending.
    /// </summary>
    private bool _waiting;

    /// <summary>
    /// Whether the link has ended: closed by the client, which this server answers at once, or aborted.
    /// </summary>
    private bool _ended;

    /// <summary>
    /// Whether <see cref="Abort"/> was called.
    /// </summary>
    private bool _aborted;

    /// <summary>
    /// Whether this link is queued on the thread pool to hand buffered PUBACKs to the read loop.
    /// </summary>
    private bool _handOverQueued;

    /// <summary>
    /// Plays back a packet from the server; a pending wait completes now, on the calling thread, so the client's
    /// read loop handles the packet before this returns.
    /// </summary>
    /// <param name="packet">
    /// The whole packet.
    /// </param>
    public void Deliver(ReadOnlySpan<byte> packet)
    {
        bool wake;
        lock (_gate)
        {
            Append(packet);
            wake = TakeWaitLocked();
        }

        if (wake)
        {
            _core.SetResult(true);
        }
    }

    /// <summary>
    /// Hands this link to the client.
    /// </summary>
    /// <param name="options">
    /// The client's options; not used.
    /// </param>
    /// <param name="cancellationToken">
    /// Not used: the link opens at once.
    /// </param>
    /// <returns>
    /// This link, opened.
    /// </returns>
    public ValueTask<LinkConnectResult> ConnectAsync(MqttWireClientOptions options, CancellationToken cancellationToken)
        => new(new LinkConnectResult(this, null));

    /// <summary>
    /// Completes when there are bytes to read or the link has ended.
    /// </summary>
    /// <param name="cancellationToken">
    /// Not used: the client never cancels a wait.
    /// </param>
    /// <returns>
    /// A task that completes when a read would not wait.
    /// </returns>
    /// <exception cref="OperationCanceledException">
    /// The link was aborted.
    /// </exception>
    public ValueTask WaitForDataAsync(CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            if (_aborted)
            {
                return ValueTask.FromException(new OperationCanceledException());
            }

            if (_count > 0 || _ended)
            {
                return default;
            }

            _waiting = true;
            return new ValueTask(this, _core.Version);
        }
    }

    /// <summary>
    /// Reads the unread bytes, as many as fit.
    /// </summary>
    /// <param name="buffer">
    /// Receives the bytes.
    /// </param>
    /// <param name="cancellationToken">
    /// Not used: a read never waits, because the client waits first.
    /// </param>
    /// <returns>
    /// The number of bytes read; 0 once the link has ended and every byte was read.
    /// </returns>
    /// <exception cref="OperationCanceledException">
    /// The link was aborted.
    /// </exception>
    public ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            if (_aborted)
            {
                return ValueTask.FromException<int>(new OperationCanceledException());
            }

            int take = Math.Min(buffer.Length, _count);
            _incoming.AsSpan(_head, take).CopyTo(buffer.Span);
            _head += take;
            _count -= take;
            if (_count == 0)
            {
                _head = 0;
            }

            return new ValueTask<int>(take);
        }
    }

    /// <summary>
    /// Takes a packet the client writes, and answers it as a server would: CONNACK for CONNECT, PUBACK for a QoS 1
    /// PUBLISH, PINGRESP for PINGREQ. The answer is handled by the read loop before this returns.
    /// </summary>
    /// <param name="packet">
    /// The whole packet.
    /// </param>
    /// <param name="cancellationToken">
    /// Not used: the write completes at once.
    /// </param>
    /// <returns>
    /// A completed task.
    /// </returns>
    public ValueTask WriteAsync(ReadOnlyMemory<byte> packet, CancellationToken cancellationToken)
    {
        ReadOnlySpan<byte> bytes = packet.Span;
        if (PacketHeader.TryRead(bytes, out PacketHeader header) != OperationStatus.Done)
        {
            return default;
        }

        switch (header.Type)
        {
            case PacketType.Connect:
                Deliver(s_connAck);
                break;

            case PacketType.Publish when (header.Flags & 0x06) == 0x02:
                // The identifier follows the topic: its two-byte length, then its bytes.
                int topicLength = BinaryPrimitives.ReadUInt16BigEndian(bytes[header.HeaderLength..]);
                ushort packetIdentifier = BinaryPrimitives.ReadUInt16BigEndian(bytes[(header.HeaderLength + 2 + topicLength)..]);
                Span<byte> pubAck = [0x40, 0x02, (byte)(packetIdentifier >> 8), (byte)packetIdentifier];
                DeliverLater(pubAck);
                break;

            case PacketType.PingReq:
                Deliver(PingCodec.PingResp);
                break;
        }

        return default;
    }

    /// <summary>
    /// Takes the client's close and answers it at once: the link ends, and the client's next read returns 0.
    /// </summary>
    /// <param name="status">
    /// The close status; not used.
    /// </param>
    /// <param name="cancellationToken">
    /// Not used: the close completes at once.
    /// </param>
    /// <returns>
    /// A completed task.
    /// </returns>
    public ValueTask CloseAsync(WebSocketCloseStatus status, CancellationToken cancellationToken)
    {
        bool wake;
        lock (_gate)
        {
            _ended = true;
            wake = TakeWaitLocked();
        }

        if (wake)
        {
            _core.SetResult(true);
        }

        return default;
    }

    /// <summary>
    /// Ends the link at once: a pending wait, and every later wait or read, fails with
    /// <see cref="OperationCanceledException"/>.
    /// </summary>
    public void Abort()
    {
        bool wake;
        lock (_gate)
        {
            _aborted = true;
            _ended = true;
            wake = TakeWaitLocked();
        }

        if (wake)
        {
            _core.SetException(new OperationCanceledException());
        }
    }

    /// <summary>
    /// Hands the PUBACKs buffered by <see cref="DeliverLater"/> to the read loop, if it waits; run on the thread
    /// pool. A read loop that is busy finds them at its next wait.
    /// </summary>
    void IThreadPoolWorkItem.Execute()
    {
        bool wake;
        lock (_gate)
        {
            _handOverQueued = false;
            wake = _count > 0 && TakeWaitLocked();
        }

        if (wake)
        {
            _core.SetResult(true);
        }
    }

    /// <inheritdoc/>
    void IValueTaskSource.GetResult(short token)
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

    /// <inheritdoc/>
    ValueTaskSourceStatus IValueTaskSource.GetStatus(short token) => _core.GetStatus(token);

    /// <inheritdoc/>
    void IValueTaskSource.OnCompleted(
        Action<object?> continuation,
        object? state,
        short token,
        ValueTaskSourceOnCompletedFlags flags)
        => _core.OnCompleted(continuation, state, token, flags);

    /// <summary>
    /// Buffers a packet from the server and queues this link on the thread pool to hand it to the read loop, so the
    /// client's write completes before the answer is handled.
    /// </summary>
    /// <param name="packet">
    /// The whole packet.
    /// </param>
    private void DeliverLater(ReadOnlySpan<byte> packet)
    {
        lock (_gate)
        {
            Append(packet);
            if (_handOverQueued)
            {
                return;
            }

            _handOverQueued = true;
        }

        ThreadPool.UnsafeQueueUserWorkItem(this, preferLocal: false);
    }

    /// <summary>
    /// Takes the pending wait off the link, for the caller to complete outside the lock. The caller holds the lock.
    /// </summary>
    /// <returns>
    /// <see langword="true"/> when a wait was pending.
    /// </returns>
    private bool TakeWaitLocked()
    {
        bool waiting = _waiting;
        _waiting = false;
        return waiting;
    }

    /// <summary>
    /// Appends bytes to the unread bytes, moving these to the start of the buffer when the end is reached. The
    /// caller holds the lock.
    /// </summary>
    /// <param name="bytes">
    /// The bytes.
    /// </param>
    /// <exception cref="InvalidOperationException">
    /// The client has fallen more than <see cref="IncomingCapacity"/> bytes behind.
    /// </exception>
    private void Append(ReadOnlySpan<byte> bytes)
    {
        if (_count + bytes.Length > IncomingCapacity)
        {
            throw new InvalidOperationException("The client has fallen too far behind the loopback server.");
        }

        if (_head + _count + bytes.Length > IncomingCapacity)
        {
            _incoming.AsSpan(_head, _count).CopyTo(_incoming);
            _head = 0;
        }

        bytes.CopyTo(_incoming.AsSpan(_head + _count));
        _count += bytes.Length;
    }
}
