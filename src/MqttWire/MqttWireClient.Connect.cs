using MqttWire.Client;
using MqttWire.Codec;
using MqttWire.Link;

namespace MqttWire;

/// <content>
/// Connecting: opening the link, the CONNECT and CONNACK handshake, and the outcome of the connect.
/// </content>
public sealed partial class MqttWireClient
{
    /// <summary>
    /// Ends a connect that is still waiting when its deadline or the caller's token fires. The state is always the
    /// client that registered it.
    /// </summary>
    private static readonly Action<object?> s_connectCanceled =
        static state => ((MqttWireClient)state!).OnConnectCanceled();

    /// <summary>
    /// Takes the link for this connection, unless the client was disposed while it opened.
    /// </summary>
    /// <param name="link">
    /// The open link.
    /// </param>
    /// <param name="result">
    /// Completes with the outcome of the connect.
    /// </param>
    /// <returns>
    /// <see langword="false"/> when the client was disposed meanwhile; it is then finished and the caller aborts
    /// the link.
    /// </returns>
    private bool TryStart(IMqttLink link, TaskCompletionSource<MqttConnectAck> result)
    {
        lock (_gate)
        {
            if (_phase != Phase.Connecting)
            {
                _phase = Phase.Closed;
                return false;
            }

            _connectResult = result;
            _ended = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _link = link;
            return true;
        }
    }

    /// <summary>
    /// Opens the link and runs the handshake over it, within the connect deadline.
    /// </summary>
    /// <param name="disposing">
    /// Cancelled when the client is disposed during the connect.
    /// </param>
    /// <param name="cancellationToken">
    /// The caller's token.
    /// </param>
    /// <returns>
    /// The outcome of the connect, as <see cref="ConnectAsync"/> reports it.
    /// </returns>
    /// <exception cref="ObjectDisposedException">
    /// The client was disposed during the connect, whatever the link did meanwhile.
    /// </exception>
    /// <exception cref="OperationCanceledException">
    /// The caller's token was cancelled before the connect was decided.
    /// </exception>
    private async Task<MqttConnectAck> ConnectCoreAsync(CancellationToken disposing,
        CancellationToken cancellationToken)
    {
        using CancellationTokenSource deadline = new(ClientLimits.ConnectTimeout, _time);
        using CancellationTokenSource linked =
            CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, deadline.Token, disposing);
        LinkConnectResult opened;
        try
        {
            opened = await _connector.ConnectAsync(Options, linked.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (linked.IsCancellationRequested)
        {
            SetClosed();
            ThrowIfDisposed();
            cancellationToken.ThrowIfCancellationRequested();
            return new MqttConnectAck(MqttClientCloseReason.ConnectTimedOut, MqttReasonCode.UnspecifiedError, null,
                null, null);
        }
        catch
        {
            SetClosed();
            throw;
        }

        if (opened.Link is not { } link)
        {
            SetClosed();
            ThrowIfDisposed();
            return new MqttConnectAck(MqttClientCloseReason.Unreachable, MqttReasonCode.UnspecifiedError, null, null,
                opened.Error);
        }

        MqttConnectAck ack = await HandshakeAsync(link, linked.Token).ConfigureAwait(false);
        if (ack.Failure == MqttClientCloseReason.ConnectTimedOut)
        {
            cancellationToken.ThrowIfCancellationRequested();
        }

        return ack;
    }

    /// <summary>
    /// Starts the read loop over the open link, sends CONNECT and waits for the read loop to decide the connect.
    /// </summary>
    /// <param name="link">
    /// The open link.
    /// </param>
    /// <param name="cancellationToken">
    /// The connect deadline and the caller's token together; when it fires before the connect is decided, the
    /// link is aborted and the connect times out.
    /// </param>
    /// <returns>
    /// The outcome of the connect.
    /// </returns>
    /// <exception cref="ObjectDisposedException">
    /// The client was disposed while the link opened or while it waited for CONNACK.
    /// </exception>
    private async Task<MqttConnectAck> HandshakeAsync(IMqttLink link, CancellationToken cancellationToken)
    {
        TaskCompletionSource<MqttConnectAck> result = new(TaskCreationOptions.RunContinuationsAsynchronously);
        if (!TryStart(link, result))
        {
            link.Abort();
            throw new ObjectDisposedException(GetType().FullName);
        }

        CancellationTokenRegistration registration = cancellationToken.UnsafeRegister(s_connectCanceled, this);
        try
        {
            _ = ReadLoopAsync(link);
            byte[] connect = RentConnect(out int length);

            // Not awaited: a failed write ends the connection, and the read loop then fails the connect.
            Send(connect, length, SendKind.Connect);
            return await result.Task.ConfigureAwait(false);
        }
        finally
        {
            // Unregister rather than Dispose: it does not wait for a callback running on another thread.
            registration.Unregister();
        }
    }

    /// <summary>
    /// Throws when <see cref="DisposeAsync"/> has been called.
    /// </summary>
    /// <exception cref="ObjectDisposedException">
    /// The client was disposed.
    /// </exception>
    private void ThrowIfDisposed()
    {
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
        }
    }

    /// <summary>
    /// Marks a connect that never opened a link as finished.
    /// </summary>
    private void SetClosed()
    {
        lock (_gate)
        {
            _phase = Phase.Closed;
        }
    }

    /// <summary>
    /// Encodes the CONNECT into a rented buffer.
    /// </summary>
    /// <param name="length">
    /// The packet's length.
    /// </param>
    /// <returns>
    /// The rented buffer, which the caller hands to <see cref="Send"/>.
    /// </returns>
    private byte[] RentConnect(out int length)
    {
        ConnectFields fields = new()
        {
            ClientId = Options.ClientId,
            UserName = Options.UserName,
            Password = _password,
            KeepAliveSeconds = (ushort)KeepAlive.TotalSeconds,
            ReceiveMaximum = ClientLimits.ReceiveMaximum,
            MaximumPacketSize = (uint)Options.MaxIncomingPacketBytes,
        };
        byte[] buffer = _pool.Rent(ConnectCodec.GetSize(fields));
        length = ConnectCodec.Write(buffer, fields);
        return buffer;
    }

    /// <summary>
    /// Ends a connect that is still waiting, because its deadline or the caller's token fired: records
    /// <see cref="MqttClientCloseReason.ConnectTimedOut"/> and aborts the link. Does nothing once the connect is
    /// decided.
    /// </summary>
    private void OnConnectCanceled()
    {
        IMqttLink? link;
        lock (_gate)
        {
            if (_phase != Phase.Connecting
                || !TryRecordEndLocked(
                    new MqttClientCloseInfo(MqttClientCloseReason.ConnectTimedOut, MqttReasonCode.Success, null, null),
                    EndMode.Abort))
            {
                return;
            }

            link = _link;
        }

        link?.Abort();
    }

    /// <summary>
    /// Handles the first packet of the connection, which must be a CONNACK that accepts it or refuses it.
    /// </summary>
    /// <param name="header">
    /// The packet's fixed header.
    /// </param>
    /// <param name="packet">
    /// The whole packet.
    /// </param>
    /// <param name="disconnect">
    /// The DISCONNECT's reason code when the result is <see cref="EndStep.DisconnectAndClose"/>.
    /// </param>
    /// <returns>
    /// <see cref="EndStep.None"/> when the connection is accepted (the connect completes now), or when another
    /// reason ended it first; <see cref="EndStep.Close"/> for a refusal this call recorded;
    /// <see cref="EndStep.DisconnectAndClose"/> for a packet that is not CONNACK, a malformed CONNACK, or one with
    /// Session Present or a reason code that is neither success nor a refusal.
    /// </returns>
    private EndStep HandleConnAck(in PacketHeader header, ReadOnlySpan<byte> packet, out MqttReasonCode disconnect)
    {
        disconnect = MqttReasonCode.ProtocolError;
        if (header.Type != PacketType.ConnAck)
        {
            return EndStep.DisconnectAndClose;
        }

        MqttReasonCode result = ConnAckCodec.TryDecode(packet, out ConnAckPacket connAck);
        if (result != MqttReasonCode.Success)
        {
            disconnect = result;
            return EndStep.DisconnectAndClose;
        }

        // The client asked for a clean start, so the server cannot have a session (MQTT 5, 3.2.2.1.1), and a refusal
        // must not claim one either (MQTT-3.2.2-6): either way the CONNACK is malformed, not a refusal to retry.
        if (connAck.SessionPresent)
        {
            return EndStep.DisconnectAndClose;
        }

        if (connAck.ReasonCode >= MqttReasonCode.UnspecifiedError)
        {
            return HandleConnAckRefusal(connAck);
        }

        if (connAck.ReasonCode != MqttReasonCode.Success)
        {
            return EndStep.DisconnectAndClose;
        }

        OutgoingRecords records =
            ReadServerLimits(connAck.Properties, out MqttQoS maximumQoS, out uint maximumPacketSize);
        TimeSpan keepAlive = ReadKeepAliveInterval(connAck.Properties);
        MqttConnectAck ack = new(failure: null, MqttReasonCode.Success, ReadReasonString(connAck.Properties),
            ReadUserProperties(connAck.Properties), exception: null);
        lock (_gate)
        {
            if (_phase != Phase.Connecting)
            {
                return EndStep.None;
            }

            // In place before the connection counts as connected, so a publish that sees it connected finds them.
            _records = records;
            _serverMaximumQoS = maximumQoS;
            _serverMaximumPacketSize = maximumPacketSize;
            _phase = Phase.Connected;

            // Created by the read loop, before it can finish the connection, which disposes it.
            StartKeepAliveLocked(keepAlive);
        }

        // Set before the read loop started, and the read loop is the caller.
        _connectResult!.TrySetResult(ack);
        return EndStep.None;
    }

    /// <summary>
    /// Records a CONNACK that refuses the connection and begins closing the client, unless another reason ended
    /// the connection first. The refusal is stored in either case.
    /// </summary>
    /// <param name="connAck">
    /// The decoded CONNACK, whose reason code is a refusal (0x80 or above).
    /// </param>
    /// <returns>
    /// <see cref="EndStep.Close"/> when this call recorded the end; <see cref="EndStep.None"/> when another reason
    /// ended the connection first.
    /// </returns>
    private EndStep HandleConnAckRefusal(in ConnAckPacket connAck)
    {
        string? reasonString = ReadReasonString(connAck.Properties);
        _refusal = new MqttConnectAck(
            MqttClientCloseReason.ConnectRefused,
            connAck.ReasonCode,
            reasonString,
            ReadUserProperties(connAck.Properties),
            exception: null);
        return TryBeginEnd(
            new MqttClientCloseInfo(MqttClientCloseReason.ConnectRefused, connAck.ReasonCode, reasonString, null),
            EndMode.Close)
            ? EndStep.Close
            : EndStep.None;
    }

    /// <summary>
    /// Reads the server's limits on what the client sends from an accepting CONNACK (MQTT 5, 3.2.2.3.2, 3.2.2.3.4
    /// and 3.2.2.3.6). Absent, they are a Receive Maximum of 65,535, QoS 2 and no packet size limit.
    /// </summary>
    /// <param name="properties">
    /// The CONNACK's validated properties.
    /// </param>
    /// <param name="maximumQoS">
    /// The highest QoS the server accepts.
    /// </param>
    /// <param name="maximumPacketSize">
    /// The largest packet the server accepts, or <see cref="uint.MaxValue"/> when it sets none.
    /// </param>
    /// <returns>
    /// The connection's outgoing records, limited to the server's Receive Maximum and never more than 1,024
    /// publishes in flight.
    /// </returns>
    private static OutgoingRecords ReadServerLimits(in PropertyBlock properties, out MqttQoS maximumQoS,
        out uint maximumPacketSize)
    {
        int publishLimit = properties.TryGetInteger(PropertyId.ReceiveMaximum, out uint receiveMaximum)
            ? (int)Math.Min(receiveMaximum, ClientLimits.MaxPublishesInFlight)
            : ClientLimits.MaxPublishesInFlight;
        maximumQoS = properties.TryGetInteger(PropertyId.MaximumQoS, out uint qos) ? (MqttQoS)qos : MqttQoS.ExactlyOnce;
        maximumPacketSize =
            properties.TryGetInteger(PropertyId.MaximumPacketSize, out uint size) ? size : uint.MaxValue;
        return new OutgoingRecords(publishLimit);
    }

    /// <summary>
    /// Completes the connect with its failure, once the end of a connection that never connected is recorded.
    /// Does nothing for a connection that connected, or a connect already completed.
    /// </summary>
    private void CompleteFailedConnect()
    {
        MqttClientCloseInfo info;
        lock (_gate)
        {
            if (_phase < Phase.Closing || _wasConnected)
            {
                return;
            }

            info = _closeInfo;
        }

        TaskCompletionSource<MqttConnectAck>? result = _connectResult;
        if (result?.Task.IsCompleted != false)
        {
            return;
        }

        switch (info.Reason)
        {
            case MqttClientCloseReason.Disposed:
                result.TrySetException(new ObjectDisposedException(GetType().FullName));
                break;
            case MqttClientCloseReason.ConnectRefused:
                // The read loop builds the refusal before it records ConnectRefused.
                result.TrySetResult(_refusal!);
                break;
            case MqttClientCloseReason.ConnectTimedOut or MqttClientCloseReason.ProtocolError:
                result.TrySetResult(new MqttConnectAck(info.Reason, MqttReasonCode.UnspecifiedError, null, null, null));
                break;
            default:
                // The link ended, or a read or the CONNECT's write failed, before any CONNACK.
                result.TrySetResult(new MqttConnectAck(MqttClientCloseReason.Unreachable,
                    MqttReasonCode.UnspecifiedError, null, null, info.Exception));
                break;
        }
    }
}
