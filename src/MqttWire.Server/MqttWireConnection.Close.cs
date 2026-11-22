using System.Diagnostics;
using System.Net.WebSockets;
using Microsoft.Extensions.Logging;
using MqttWire.Codec;
using MqttWire.Server.Internal;

namespace MqttWire.Server;

/// <content>
/// Ending a connection: the bounded close, the sweep's checks of the deadlines, the report to the host.
/// </content>
public sealed partial class MqttWireConnection
{
    /// <summary>
    /// Begins the close (design 6.6, "Closing, bounded"), with WebSocket status 1000: see
    /// <see cref="BeginCloseWithStatus"/>. Safe from any thread; a call after the first changes nothing.
    /// </summary>
    /// <param name="reason">
    /// Why the connection ends.
    /// </param>
    /// <param name="code">
    /// The reason code the end carries; <see cref="MqttReasonCode.Success"/> when it carries none.
    /// </param>
    /// <param name="reasonString">
    /// The reason string, or <see langword="null"/>; validated where the host handed it over, so it can be encoded.
    /// It is cut, and left out where the client's Request Problem Information or Maximum Packet Size calls for it.
    /// </param>
    internal void BeginClose(MqttCloseReason reason, MqttReasonCode code, string? reasonString) =>
        BeginCloseWithStatus(reason, code, reasonString, WebSocketCloseStatus.NormalClosure);

    /// <summary>
    /// Begins the close (design 6.6, "Closing, bounded"). In one hold of the send path's lock it moves the connection
    /// to Closing, once, records the reason, and looks at the send path; from that hold on nothing is added to what
    /// waits. When a write is in progress, which includes the send path reserved for a CONNACK during and after the
    /// admission step, the link is aborted now and nothing else is done: a socket that is not taking data is never
    /// asked to carry its own notice, and no packet precedes a CONNACK. When no write is in progress, this call takes
    /// the send path, hands the packet that carries the reason code to the link, and then the WebSocket close,
    /// without waiting for the peer; the sweep aborts the link at the close deadline. The packet is DISCONNECT for a
    /// connection that was Active, except when the client disconnected or the link was lost; CONNACK for one that was
    /// not admitted, when the code is a failure (plan decision 7); otherwise none. Aborting returns nothing: the
    /// write in progress ends by itself and returns its own buffer (design 6.8). A fault inside MqttWire while the
    /// packet is encoded (the pool throws) aborts the link instead, logged with event 20; nothing is thrown. Safe from
    /// any thread; a call after the first changes nothing.
    /// </summary>
    /// <param name="reason">
    /// Why the connection ends.
    /// </param>
    /// <param name="code">
    /// The reason code the end carries; <see cref="MqttReasonCode.Success"/> when it carries none.
    /// </param>
    /// <param name="reasonString">
    /// The reason string, or <see langword="null"/>; validated where the host handed it over, so it can be encoded.
    /// It is cut, and left out where the client's Request Problem Information or Maximum Packet Size calls for it.
    /// </param>
    /// <param name="status">
    /// The WebSocket status the peer sees when the close is handed to the link.
    /// </param>
    private void BeginCloseWithStatus(MqttCloseReason reason, MqttReasonCode code, string? reasonString,
        WebSocketCloseStatus status)
    {
        ConnectionState left;
        bool writing;
        lock (_sendLock)
        {
            if (!TryEnterClosing(reason, out left))
            {
                return;
            }

            writing = _writing;
            if (!writing)
            {
                // This close takes the send path, in the hold that found it free, and keeps it until its WebSocket
                // close has been handed to the link.
                _writing = true;
                _closeStatus = (ushort)status;
            }
        }

        if (writing)
        {
            RunningLink.Abort();
            return;
        }

        byte[]? buffer = null;
        int length;
        try
        {
            length = EncodeClosingPacket(left, reason, code, reasonString, out buffer);
        }
        catch (Exception exception)
        {
            // This close holds the send path: the fault gives it up, with the buffer if one was rented (it was never
            // handed to the link), and aborts the link instead of the WebSocket close. Nothing is thrown at the
            // caller, which may be the sweep's timer or a host's Close.
            FaultSendPath(exception, buffer);
            return;
        }

        // The packet, when there is one, then the WebSocket close, both through the send path.
        if (buffer is not null && length > 0)
        {
            StartSending(buffer.AsMemory(start: 0, length), buffer, InlineWrites);
        }
        else
        {
            ContinueSending(returned: buffer, failed: false, InlineWrites);
        }
    }

    /// <summary>
    /// Encodes the packet that carries a close's reason code to the client, into a buffer rented from the server's
    /// pool: DISCONNECT for a connection that was Active, except when the client disconnected or the link was lost;
    /// CONNACK with the code for one that was not admitted, when the code is a failure (plan decision 7); otherwise
    /// none. The reason string follows the rule of <see cref="ShapeReasonString"/>.
    /// </summary>
    /// <param name="left">
    /// The state the connection left for Closing.
    /// </param>
    /// <param name="reason">
    /// Why the connection ends.
    /// </param>
    /// <param name="code">
    /// The reason code.
    /// </param>
    /// <param name="reasonString">
    /// The reason string, or <see langword="null"/>; validated where the host handed it over.
    /// </param>
    /// <param name="buffer">
    /// The rented buffer that holds the packet, which the completion of its write returns; <see langword="null"/>
    /// when the close carries no packet.
    /// </param>
    /// <returns>
    /// The packet's length; 0 when the close carries no packet.
    /// </returns>
    private int EncodeClosingPacket(ConnectionState left, MqttCloseReason reason, MqttReasonCode code,
        string? reasonString, out byte[]? buffer)
    {
        buffer = null;
        if (left == ConnectionState.Active)
        {
            if (reason is MqttCloseReason.ClientDisconnected or MqttCloseReason.ConnectionLost)
            {
                return 0;
            }

            string? carried = ShapeReasonString(PacketType.Disconnect, code, count: 0, reasonString);
            buffer = _server.Pool.Rent(DisconnectCodec.GetSize(code, carried));
            return DisconnectCodec.Write(buffer, code, carried);
        }

        if ((byte)code < 0x80)
        {
            return 0;
        }

        // Not admitted: this close's CONNACK is the first and only packet.
        ConnAckFields fields = new()
        {
            ReasonCode = code, ReasonString = ShapeReasonString(PacketType.ConnAck, code, count: 0, reasonString),
        };
        buffer = _server.Pool.Rent(ConnAckCodec.GetSize(fields));
        return ConnAckCodec.Write(buffer, fields);
    }

    /// <summary>
    /// Tells the host that the admitted connection ended, with the reason of the first <see cref="BeginClose"/>.
    /// Called once, by the end of the run. An <see cref="IMqttWireHandler.OnClosed"/> that throws is logged and
    /// nothing else: the connection ends all the same.
    /// </summary>
    private void ReportClosed()
    {
        try
        {
            _server.Handler.OnClosed(this, CloseReason);
        }
        catch (Exception exception)
        {
            ServerLog.HandlerThrew(_server.Logger, ClientId, nameof(IMqttWireHandler.OnClosed), exception);
        }
    }

    /// <summary>
    /// Refuses a first packet that is not a CONNECT, or whose fixed header is malformed: no MQTT packet can answer
    /// it, so the connection begins to close with no packet and the WebSocket is closed with status 1002 (plan
    /// decision 7), through the send path like every close. Does nothing when the connection is Closing already.
    /// </summary>
    /// <param name="reason">
    /// Why the connection ends.
    /// </param>
    private void RefuseFirstPacket(MqttCloseReason reason) =>
        BeginCloseWithStatus(reason, MqttReasonCode.Success, reasonString: null, WebSocketCloseStatus.ProtocolError);

    /// <summary>
    /// Acts on the deadlines that have passed; called by the sweep, from the timer's thread. An Accepted
    /// connection is aborted at the connect deadline; an Authenticating one has its cancellation source taken at the
    /// authentication deadline and handed to the thread pool, which cancels it (see
    /// <see cref="CancelAuthentication"/>); an Active one is aborted at the write deadline
    /// (<see cref="AbortStalledWrite"/>), and begins its close at the acknowledgement deadline (0x82, see
    /// <see cref="CheckAcknowledgementDeadline"/>), at the partial-packet deadline (0x82) and at the keep-alive
    /// deadline (0x8D); a Closing one is aborted at the close deadline. Each check acts only on the state
    /// it read: the move out of Accepted is a compare-exchange that expects Accepted, the cancellation source exists
    /// only while the connection is Authenticating and is taken from its field by one exchange, the write deadline
    /// acts only on the write whose stamp it read, the close of an Active one is <see cref="BeginClose"/>, which
    /// moves to Closing once, and the close deadline is measured from the stamp that only the winner of the move to
    /// Closing writes. On the sweep's thread it runs no host code; beyond an abort, the only part of the connection's
    /// end it runs is the start of a close through the send path: the DISCONNECT handed to the link and the WebSocket
    /// close, neither awaited. It never throws; it allocates only when it hands a source to the thread pool or a
    /// write or close does not complete at once. An abort ends the link's pending reads, whose continuations may run
    /// on the thread that aborts, as with any abort.
    /// </summary>
    /// <param name="now">
    /// The sweep's timestamp, from the server's clock.
    /// </param>
    internal void CheckDeadlines(long now)
    {
        if (_state == ConnectionState.Accepted)
        {
            long accepted = Volatile.Read(ref _stateEnteredAt);
            if (accepted != 0 && now - accepted >= _server.ConnectDeadlineLength)
            {
                AbortFromAccepted(MqttCloseReason.ConnectionLost);
            }
        }
        else if (_state == ConnectionState.Authenticating)
        {
            // Stamped before the move to Authenticating, so this is that state's stamp.
            long entered = Volatile.Read(ref _stateEnteredAt);
            if (now - entered >= _server.AuthenticationDeadlineLength)
            {
                CancelAuthentication();
            }
        }
        else if (_state == ConnectionState.Active)
        {
            // 0 while no write is with the link: one that completed at once was never stamped.
            long writing = Volatile.Read(ref _writeStartedAt);
            if (writing != 0 && now - writing >= _server.WriteDeadlineLength)
            {
                AbortStalledWrite(writing);
                return;
            }

            if (CheckAcknowledgementDeadline(now))
            {
                return;
            }

            CheckReadDeadlines(now);
        }
        else if (_state == ConnectionState.Closing)
        {
            // 0 until the winner of the move to Closing has stamped it: that sweep skips the connection.
            long closing = Volatile.Read(ref _closingAt);
            if (closing != 0 && now - closing >= _server.CloseDeadlineLength)
            {
                _link?.Abort();
            }
        }
    }

    /// <summary>
    /// Ends a connection that has sent no complete CONNECT, at the connect deadline or when the server stops: moves
    /// it to Closing, only from Accepted, and aborts the link. Nothing is handed to the link, and the send path is not
    /// touched: nothing was ever written.
    /// </summary>
    /// <param name="reason">
    /// Why the connection ends; no <see cref="IMqttWireHandler.OnClosed"/> reports it, since the connection was
    /// never admitted.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when the connection was Accepted and is now Closing, its link aborted;
    /// <see langword="false"/> when it had left Accepted, and nothing changed.
    /// </returns>
    private bool AbortFromAccepted(MqttCloseReason reason)
    {
        lock (_sendLock)
        {
            if (!TryEnterClosingFrom(ConnectionState.Accepted, reason))
            {
                return false;
            }
        }

        // The connection was never admitted, so no OnClosed reports the reason; the link failing is the end.
        _link?.Abort();
        return true;
    }

    /// <summary>
    /// Ends a connection whose write has been with the link for <see cref="ServerLimits.WriteDeadline"/>: moves it
    /// to Closing with <see cref="MqttCloseReason.WriteTimedOut"/> and aborts the link. No DISCONNECT is attempted:
    /// a write is in progress, and it is the one that does not move. It acts only on the write whose stamp the sweep
    /// read: when that write has ended meanwhile, or the connection is closing already, nothing is done. Aborting
    /// releases nothing: the write ends with an error and returns its own buffer, and what waited behind it is
    /// dropped then.
    /// </summary>
    /// <param name="started">
    /// The stamp of the write the sweep found past its deadline.
    /// </param>
    private void AbortStalledWrite(long started)
    {
        lock (_sendLock)
        {
            // The stamp is set only while a piece is with the link, so the send path is taken: by that write.
            if (Volatile.Read(ref _writeStartedAt) != started
                || !TryEnterClosingFrom(ConnectionState.Active, MqttCloseReason.WriteTimedOut))
            {
                return;
            }
        }

        _link?.Abort();
    }

    /// <summary>
    /// Acts on the two deadlines of reading for an Active connection: begins the close with 0x82
    /// (<see cref="MqttCloseReason.PartialPacketTimedOut"/>) when a packet has stayed incomplete for
    /// <see cref="ServerLimits.PartialPacketDeadline"/> from its first byte, and with 0x8D
    /// (<see cref="MqttCloseReason.KeepAliveTimedOut"/>) when no complete packet has come for 1.5 times the
    /// keep-alive. A stamp the code that reads changes meanwhile is at worst one sweep old, so a close that races a
    /// packet just completed ends a connection that was a moment from its deadline anyway.
    /// </summary>
    /// <param name="now">
    /// The sweep's timestamp, from the server's clock.
    /// </param>
    private void CheckReadDeadlines(long now)
    {
        long partial = Volatile.Read(ref _partialPacketAt);
        if (partial != 0 && now - partial >= _server.PartialPacketDeadlineLength)
        {
            BeginClose(MqttCloseReason.PartialPacketTimedOut, MqttReasonCode.ProtocolError, reasonString: null);
            return;
        }

        long keepAlive = _keepAliveSeconds * _server.Time.TimestampFrequency * ServerLimits.KeepAliveFactorNumerator
                         / ServerLimits.KeepAliveFactorDenominator;
        if (now - Volatile.Read(ref _lastPacketAt) >= keepAlive)
        {
            BeginClose(MqttCloseReason.KeepAliveTimedOut, MqttReasonCode.KeepAliveTimeout, reasonString: null);
        }
    }

    /// <summary>
    /// Ends the host's wait at the authentication deadline: takes the cancellation source out of its field, on the
    /// sweep's thread, and hands it to the thread pool, which cancels and disposes it. When the connect phase took it
    /// first, the host has answered and nothing is done. So the source is still cancelled and disposed exactly once,
    /// and the token's callbacks (the host's, and the connect phase's continuation with the CONNACK 0x88 and the end
    /// of the run) never run on the sweep's thread.
    /// </summary>
    private void CancelAuthentication()
    {
        CancellationTokenSource? source = Interlocked.Exchange(ref _authentication, null);
        if (source is null)
        {
            return;
        }

        ThreadPool.UnsafeQueueUserWorkItem(static state => CancelOnThreadPool(state.Source, state.Logger, state.ClientId),
            (Source: source, Logger: (ILogger)_server.Logger, ClientId), preferLocal: false);
    }

    /// <summary>
    /// Cancels and disposes a cancellation source of the Authenticating state, on a thread-pool thread. A token
    /// callback that throws is the host's bug: it is logged as an error of <see cref="IMqttWireHandler.OnConnectAsync"/>
    /// and the token is cancelled all the same.
    /// </summary>
    /// <param name="source">
    /// The source; nothing else cancels or disposes it.
    /// </param>
    /// <param name="logger">
    /// The server's logger.
    /// </param>
    /// <param name="clientId">
    /// The client identifier of the connection.
    /// </param>
    private static void CancelOnThreadPool(CancellationTokenSource source, ILogger logger, string clientId)
    {
        try
        {
            source.Cancel();
        }
        catch (AggregateException exception)
        {
            ServerLog.HandlerThrew(logger, clientId, nameof(IMqttWireHandler.OnConnectAsync), exception);
        }
        finally
        {
            source.Dispose();
        }
    }

    /// <summary>
    /// Moves the connection to Closing unless it is there or beyond, records the reason and stamps the time. The
    /// caller holds the send path's lock, as for <see cref="TryEnterClosingFrom"/>.
    /// </summary>
    /// <param name="reason">
    /// Why the connection ends.
    /// </param>
    /// <param name="left">
    /// The state the connection left when the result is <see langword="true"/>; otherwise the state it was in.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when this call made the move; <see langword="false"/> when another had made it, and
    /// nothing changed.
    /// </returns>
    private bool TryEnterClosing(MqttCloseReason reason, out ConnectionState left)
    {
        left = _state;
        while (left < ConnectionState.Closing)
        {
            if (TryEnterClosingFrom(left, reason))
            {
                return true;
            }

            left = _state;
        }

        return false;
    }

    /// <summary>
    /// Moves the connection to Closing only from one state; the winner records the reason and writes
    /// <see cref="_closingAt"/>, which nothing else writes. The caller holds the send path's lock: every move to
    /// Closing is made under it, so that the state read under that lock tells the send path exactly whether anything
    /// may still be added or written.
    /// </summary>
    /// <param name="expected">
    /// The state the move must start from; it is below Closing.
    /// </param>
    /// <param name="reason">
    /// Why the connection ends.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when this call made the move; <see langword="false"/> when the connection was in
    /// another state, and nothing changed.
    /// </returns>
    private bool TryEnterClosingFrom(ConnectionState expected, MqttCloseReason reason)
    {
        Debug.Assert(_sendLock.IsHeldByCurrentThread, "Every move to Closing is made under the send path's lock.");
        if (Interlocked.CompareExchange(ref _state, ConnectionState.Closing, expected) != expected)
        {
            return false;
        }

        CloseReason = reason;
        Volatile.Write(ref _closingAt, _server.Time.GetTimestamp());
        return true;
    }
}
