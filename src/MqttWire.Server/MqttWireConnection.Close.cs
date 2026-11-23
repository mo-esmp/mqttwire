using System.Diagnostics;
using System.Net;
using System.Net.WebSockets;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using MqttWire.Codec;
using MqttWire.Link;
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
    /// <param name="cause">
    /// Why the connection ends.
    /// </param>
    /// <param name="code">
    /// The reason code the end carries; <see cref="MqttReasonCode.Success"/> when it carries none.
    /// </param>
    /// <param name="reasonString">
    /// The reason string, or <see langword="null"/>; validated where the host handed it over, so it can be encoded.
    /// It is cut, and left out where the client's Request Problem Information or Maximum Packet Size calls for it.
    /// </param>
    internal void BeginClose(CloseCause cause, MqttReasonCode code, string? reasonString) =>
        BeginCloseWithStatus(cause, code, reasonString, WebSocketCloseStatus.NormalClosure, payloadLength: 0);

    /// <summary>
    /// Begins the close (design 6.6, "Closing, bounded"). In one hold of the send path's lock it moves the connection
    /// to Closing, once, records the reason, and looks at the send path; from that hold on nothing is added to what
    /// waits. When a write is in progress, which includes the send path reserved for a CONNACK during and after the
    /// admission step, the link is aborted now and nothing else is done: a socket that is not taking data is never
    /// asked to carry its own notice, and no packet precedes a CONNACK. The abort is registered in the same hold
    /// (<see cref="BeginAbort"/>), so the run cannot end before it has been issued. When no write is in progress, this
    /// call takes the send path, hands the packet that carries the reason code to the link, and then the WebSocket close,
    /// without waiting for the peer; the sweep aborts the link at the close deadline. The packet is DISCONNECT for a
    /// connection that was Active, except when the client disconnected, the link was lost or the send path faulted;
    /// CONNACK for one that was not admitted, when the code is a failure and the first packet was a CONNECT (plan
    /// decision 7); otherwise none. Aborting returns nothing: the write in progress ends by itself and returns its own
    /// buffer (design 6.8). A fault inside MqttWire while the packet is encoded (the pool throws) aborts the link
    /// instead, logged with event 20; nothing is thrown. The call that makes the move logs the end, once the lock is
    /// released and before it acts (<see cref="LogEnd"/>); a call that finds the move made logs nothing. Safe from any
    /// thread; a call after the first changes nothing.
    /// </summary>
    /// <param name="cause">
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
    /// The WebSocket status the peer sees when the close is handed to the link; with
    /// <see cref="WebSocketCloseStatus.ProtocolError"/>, a first packet that was not a CONNECT, no MQTT packet is
    /// written whatever the code.
    /// </param>
    /// <param name="payloadLength">
    /// The length of the payload of the message that was too large to send, for the log of
    /// <see cref="CloseCause.MessageTooLargeToSend"/>; 0 for any other end.
    /// </param>
    private void BeginCloseWithStatus(CloseCause cause, MqttReasonCode code, string? reasonString,
        WebSocketCloseStatus status, int payloadLength)
    {
        ConnectionState left;
        bool writing;
        IMqttLink? aborted = null;
        lock (_sendLock)
        {
            if (!TryEnterClosing(cause, out left))
            {
                return;
            }

            writing = _writing;
            if (writing)
            {
                // Registered in the hold that decided on the abort: the run does not end before it is issued.
                aborted = BeginAbort();
            }
            else
            {
                // This close takes the send path, in the hold that found it free, and keeps it until its WebSocket
                // close has been handed to the link.
                _writing = true;
                _closeStatus = (ushort)status;
            }
        }

        LogEnd(cause, code, payloadLength, method: null, exception: null);
        if (writing)
        {
            FinishAbort(aborted);
            return;
        }

        byte[]? buffer = null;
        int length;
        try
        {
            length = status == WebSocketCloseStatus.ProtocolError
                ? 0
                : EncodeClosingPacket(left, cause, code, reasonString, out buffer);
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
    /// pool: DISCONNECT for a connection that was Active, except when the client disconnected, the link was lost or
    /// the send path faulted; CONNACK with the code for one that was not admitted, when the code is a failure (plan
    /// decision 7); otherwise none. The reason string follows the rule of <see cref="ShapeReasonString"/>.
    /// </summary>
    /// <param name="left">
    /// The state the connection left for Closing.
    /// </param>
    /// <param name="cause">
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
    private int EncodeClosingPacket(ConnectionState left, CloseCause cause, MqttReasonCode code,
        string? reasonString, out byte[]? buffer)
    {
        buffer = null;
        if (left == ConnectionState.Active)
        {
            if (cause is CloseCause.ClientDisconnected or CloseCause.ConnectionLost or CloseCause.SendPathFaulted)
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
    /// Tells the host that the admitted connection ended, with the reason of the first <see cref="BeginClose"/>,
    /// narrowed to <see cref="MqttCloseReason"/> (<see cref="TryNarrow"/>). Called once, by the end of the run. An
    /// <see cref="IMqttWireHandler.OnClosed"/> that throws is logged and nothing else: the connection ends all the
    /// same, and so does a logging provider that throws then.
    /// </summary>
    private void ReportClosed()
    {
        bool narrowed = TryNarrow(CloseCause, out MqttCloseReason reason);
        Debug.Assert(narrowed, "An end before the admission is never reported to OnClosed.");
        try
        {
            _server.Handler.OnClosed(this, reason);
        }
        catch (Exception exception)
        {
            LogEnd(CloseCause.HandlerThrew, MqttReasonCode.Success, payloadLength: 0,
                nameof(IMqttWireHandler.OnClosed), exception);
        }
    }

    /// <summary>
    /// Narrows why a connection ended to what <see cref="IMqttWireHandler.OnClosed"/> reports: every cause that
    /// <see cref="MqttCloseReason"/> names is itself, and a fault of the send path is
    /// <see cref="MqttCloseReason.ConnectionLost"/>. The three ends before an admission
    /// (<see cref="CloseCause.RefusedByHost"/>, <see cref="CloseCause.AuthenticationTimedOut"/> and
    /// <see cref="CloseCause.ConnectTimedOut"/>) are never narrowed: no <see cref="IMqttWireHandler.OnClosed"/>
    /// reports them.
    /// </summary>
    /// <param name="cause">
    /// Why the connection ended.
    /// </param>
    /// <param name="reason">
    /// The reason the host is told; <see cref="MqttCloseReason.ConnectionLost"/> when the result is
    /// <see langword="false"/>.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when the cause can reach <see cref="IMqttWireHandler.OnClosed"/>;
    /// <see langword="false"/> for an end before the admission.
    /// </returns>
    internal static bool TryNarrow(CloseCause cause, out MqttCloseReason reason)
    {
        if (cause <= CloseCause.ServerStopping)
        {
            reason = (MqttCloseReason)cause;
            return true;
        }

        reason = MqttCloseReason.ConnectionLost;
        return cause == CloseCause.SendPathFaulted;
    }

    /// <summary>
    /// Refuses a first packet that is not a CONNECT, or whose fixed header is malformed: no MQTT packet can answer
    /// it, so the connection begins to close with no packet and the WebSocket is closed with status 1002 (plan
    /// decision 7), through the send path like every close. Does nothing when the connection is Closing already.
    /// </summary>
    /// <param name="cause">
    /// Why the connection ends.
    /// </param>
    /// <param name="code">
    /// The reason code of the refusal, for the log only: no packet carries it.
    /// </param>
    private void RefuseFirstPacket(CloseCause cause, MqttReasonCode code) =>
        BeginCloseWithStatus(cause, code, reasonString: null, WebSocketCloseStatus.ProtocolError, payloadLength: 0);

    /// <summary>
    /// Acts on the deadlines that have passed; called by the sweep, from the timer's thread. An Accepted
    /// connection is aborted at the connect deadline; an Authenticating one has its cancellation source taken at the
    /// authentication deadline and handed to the thread pool, which cancels it (see
    /// <see cref="CancelAuthentication"/>), and is aborted when its host's call has not come back one close deadline
    /// later (<see cref="EndUnanswered"/>); an Active one is aborted at the write deadline
    /// (<see cref="AbortStalledWrite"/>), and begins its close at the acknowledgement deadline (0x82, see
    /// <see cref="CheckAcknowledgementDeadline"/>), at the partial-packet deadline (0x82) and at the keep-alive
    /// deadline (0x8D); a Closing one is aborted at the close deadline. Each check acts only on the state
    /// it read: the move out of Accepted is a compare-exchange that expects Accepted, the cancellation source exists
    /// only while the connection is Authenticating and is taken from its field by one exchange, the write deadline
    /// acts only on the write whose stamp it read, the close of an Active one is <see cref="BeginClose"/>, which
    /// moves to Closing once, and the close deadline is measured from the stamp that only the winner of the move to
    /// Closing writes. On the sweep's thread it runs no host code; beyond an abort, the only part of the connection's
    /// end it runs is the start of a close through the send path: the DISCONNECT handed to the link and the WebSocket
    /// close, neither awaited. It does not throw, and should it all the same, the sweep ends the connection as a
    /// fault and goes on (<see cref="MqttWireServer.Sweep"/>); it allocates only when it hands a source to the thread
    /// pool or a write or close does not complete at once. An abort ends the link's pending reads, whose
    /// continuations may run on the thread that aborts, as with any abort; the end of the run never does, because the
    /// run waits for the abort to return and then carries on a thread-pool thread (<see cref="TryEndRun"/>).
    /// </summary>
    /// <param name="now">
    /// The sweep's timestamp, from the server's clock.
    /// </param>
    internal void CheckDeadlines(long now)
    {
        switch (_state)
        {
            case ConnectionState.Accepted:

                long accepted = Volatile.Read(ref _stateEnteredAt);
                if (accepted != 0 && now - accepted >= _server.ConnectDeadlineLength)
                {
                    AbortFromAccepted(CloseCause.ConnectTimedOut);
                }

                break;

            case ConnectionState.Authenticating:
                CheckAuthenticationDeadline(now);
                break;
            case ConnectionState.Active:

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
                break;
            case ConnectionState.Closing:
                // 0 until the winner of the move to Closing has stamped it: that sweep skips the connection.
                long closing = Volatile.Read(ref _closingAt);
                if (closing != 0 && now - closing >= _server.CloseDeadlineLength)
                {
                    AbortLink();
                }

                // A connection ended because its host's call did not come back keeps its slot until the call returns:
                // a stop that began since does not wait for it either.
                if (IsHostCallCancelled &&
                    _server.IsStopping &&
                    now - Volatile.Read(ref _stateEnteredAt) >= _server.CloseDeadlineLength)
                {
                    LeaveToTheHost();
                }

                break;
        }
    }

    /// <summary>
    /// Acts on the deadlines of an Authenticating connection: at the authentication deadline, cancels the token of
    /// the host's <see cref="IMqttWireHandler.OnConnectAsync"/> (<see cref="CancelAuthentication"/>); and when the
    /// call has not come back one close deadline after its token was cancelled, by this or by the stop, ends the
    /// connection without it (<see cref="EndUnanswered"/>).
    /// </summary>
    /// <param name="now">
    /// The sweep's timestamp, from the server's clock.
    /// </param>
    private void CheckAuthenticationDeadline(long now)
    {
        // The mark is read before the stamp: whoever cancels writes the stamp first, then the mark.
        if (IsHostCallCancelled)
        {
            if (now - Volatile.Read(ref _stateEnteredAt) >= _server.CloseDeadlineLength)
            {
                EndUnanswered();
            }

            return;
        }

        // Stamped before the move to Authenticating, so this is that state's stamp.
        long entered = Volatile.Read(ref _stateEnteredAt);
        if (now - entered >= _server.AuthenticationDeadlineLength)
        {
            CancelAuthentication();
        }
    }

    /// <summary>
    /// Ends a connection whose host's <see cref="IMqttWireHandler.OnConnectAsync"/> has not come back one close
    /// deadline after its token was cancelled (design 6.6, "<c>OnConnectAsync</c> and its deadline"): a host that
    /// blocks the calling thread and ignores the token. The run's thread is inside the host, so nothing of the
    /// connect phase can run: the connection is moved to Closing, only from Authenticating and only while the call
    /// is still out, with <see cref="CloseCause.ServerStopping"/> when the server is stopping and
    /// <see cref="CloseCause.AuthenticationTimedOut"/> otherwise, its end is logged, and its link is aborted; no
    /// CONNACK is written. The decision is made in a hold of the send path's lock, which the admission step takes
    /// too: a call that comes back at this moment either finds the connection Closing and is refused, or came back
    /// first, and then this does nothing and the connect phase ends the connection itself.
    /// </summary>
    /// <remarks>
    /// What stays held until the host's call returns is what only the run can let go of: the connection's slot, the
    /// read buffer when bytes came behind the CONNECT (design 6.8: only the code that awaits a read returns its
    /// buffer), and the request with its <see cref="HttpContext"/>. When the server is stopping, the stop no longer
    /// waits for that slot (<see cref="LeaveToTheHost"/>).
    /// </remarks>
    private void EndUnanswered()
    {
        bool stopping = _server.IsStopping;
        CloseCause cause = stopping ? CloseCause.ServerStopping : CloseCause.AuthenticationTimedOut;
        IMqttLink? aborted;
        lock (_sendLock)
        {
            if (!IsHostCallCancelled || !TryEnterClosingFrom(ConnectionState.Authenticating, cause))
            {
                return;
            }

            aborted = BeginAbort();
        }

        // The connection was never admitted, so no OnClosed reports the reason.
        LogEnd(cause, stopping ? MqttReasonCode.ServerShuttingDown : MqttReasonCode.ServerUnavailable,
            payloadLength: 0, method: null, exception: null);
        FinishAbort(aborted);
        if (stopping)
        {
            LeaveToTheHost();
        }
    }

    /// <summary>
    /// Tells the stop not to wait for this connection, whose host's <see cref="IMqttWireHandler.OnConnectAsync"/>
    /// has not returned one close deadline after its token was cancelled (design 6.6, "When the host stops"): its
    /// link has been aborted, and its run cannot end before the call returns. Logged once, as an error (event 22).
    /// The connection keeps its slot, which it frees when the call returns, whenever that is.
    /// </summary>
    private void LeaveToTheHost()
    {
        if (!_server.Registry.TryAbandon(this))
        {
            return;
        }

        // Swallowed for the reason LogEnd gives: the sweep has nowhere to report a logging provider that throws.
#pragma warning disable RCS1075 // Avoid empty catch clause that catches System.Exception: see the comment above.
        try
        {
            ServerLog.HostCallDidNotReturn(_server.Logger, ClientId, nameof(IMqttWireHandler.OnConnectAsync));
        }
        catch (Exception)
        {
            // Nowhere left to report it.
        }
#pragma warning restore RCS1075
    }

    /// <summary>
    /// Ends a connection that has sent no complete CONNECT, at the connect deadline or when the server stops: moves
    /// it to Closing, only from Accepted, logs the end and aborts the link. Nothing is handed to the link, and the
    /// send path is not touched: nothing was ever written. The abort is registered in the hold that makes the move
    /// (<see cref="BeginAbort"/>), so a run that ends meanwhile, because the peer closed, waits until the abort has
    /// been issued.
    /// </summary>
    /// <param name="cause">
    /// Why the connection ends; no <see cref="IMqttWireHandler.OnClosed"/> reports it, since the connection was
    /// never admitted.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when the connection was Accepted and is now Closing, its link aborted;
    /// <see langword="false"/> when it had left Accepted, and nothing changed.
    /// </returns>
    private bool AbortFromAccepted(CloseCause cause)
    {
        IMqttLink? aborted;
        lock (_sendLock)
        {
            if (!TryEnterClosingFrom(ConnectionState.Accepted, cause))
            {
                return false;
            }

            aborted = BeginAbort();
        }

        // The connection was never admitted, so no OnClosed reports the reason; the link failing is the end.
        LogEnd(cause, MqttReasonCode.Success, payloadLength: 0, method: null, exception: null);
        FinishAbort(aborted);
        return true;
    }

    /// <summary>
    /// Ends a connection whose write has been with the link for <see cref="ServerLimits.WriteDeadline"/>: moves it
    /// to Closing with <see cref="CloseCause.WriteTimedOut"/>, logs the end and aborts the link. No DISCONNECT is attempted:
    /// a write is in progress, and it is the one that does not move. It acts only on the write whose stamp the sweep
    /// read: when that write has ended meanwhile, or the connection is closing already, nothing is done. Aborting
    /// releases nothing: the write ends with an error and returns its own buffer, and what waited behind it is
    /// dropped then. The abort is registered in the hold that makes the move (<see cref="BeginAbort"/>).
    /// </summary>
    /// <param name="started">
    /// The stamp of the write the sweep found past its deadline.
    /// </param>
    private void AbortStalledWrite(long started)
    {
        IMqttLink? aborted;
        lock (_sendLock)
        {
            // The stamp is set only while a piece is with the link, so the send path is taken: by that write.
            if (Volatile.Read(ref _writeStartedAt) != started
                || !TryEnterClosingFrom(ConnectionState.Active, CloseCause.WriteTimedOut))
            {
                return;
            }

            aborted = BeginAbort();
        }

        LogEnd(CloseCause.WriteTimedOut, MqttReasonCode.Success, payloadLength: 0, method: null, exception: null);
        FinishAbort(aborted);
    }

    /// <summary>
    /// Aborts the link, unless the run has ended or an abort is in flight already. Every abort of the link goes
    /// through here, or through <see cref="BeginAbort"/> and <see cref="FinishAbort"/>, which this is for a caller
    /// that has no hold of its own to decide in. It never throws: a link that throws when it is aborted is logged as
    /// a fault (event 20) and nothing else.
    /// </summary>
    private void AbortLink()
    {
        if (TryAbortLink() is { } thrown)
        {
            LogFault(thrown, secondary: null);
        }
    }

    /// <summary>
    /// Aborts the link as <see cref="AbortLink"/> does, for a caller that logs what the abort threw itself.
    /// </summary>
    /// <returns>
    /// What the link threw when it was aborted; <see langword="null"/> when it threw nothing, or was not aborted by
    /// this call.
    /// </returns>
    private Exception? TryAbortLink()
    {
        IMqttLink? link;
        lock (_sendLock)
        {
            link = BeginAbort();
        }

        return link is null ? null : IssueAbort(link);
    }

    /// <summary>
    /// Registers an abort of the link, in the caller's hold of the send path's lock: the one rule that keeps an abort
    /// from ever reaching a link whose request has ended, when its first abort would throw (ASP.NET Core's WebSocket
    /// aborts its <see cref="HttpContext"/>, which throws once the request is over). An abort is issued only by a
    /// thread that found, in a hold of the lock, the link still there (the run has not ended) and no abort in flight,
    /// and that set <see cref="_aborting"/> in that hold; the run ends only in a hold that finds the flag clear, and
    /// lets go of the link in it (<see cref="TryEndRun"/>). So the two are ordered by the lock: either the run waits
    /// until the abort has returned, or the abort is never issued.
    /// </summary>
    /// <returns>
    /// The link to hand to <see cref="FinishAbort"/> or <see cref="IssueAbort"/> once the lock is released, which
    /// the caller must then do; <see langword="null"/> when nothing is to be aborted: the run has ended, or an abort
    /// is in flight on another thread, or lower on this one's stack, and does this caller's work.
    /// </returns>
    private IMqttLink? BeginAbort()
    {
        Debug.Assert(_sendLock.IsHeldByCurrentThread, "An abort is registered under the send path's lock.");
        IMqttLink? link = _link;
        if (link is null || _aborting)
        {
            return null;
        }

        _aborting = true;
        return link;
    }

    /// <summary>
    /// Issues an abort that <see cref="BeginAbort"/> registered, outside the lock, and logs what the link threw, if
    /// anything, as a fault (event 20). It never throws.
    /// </summary>
    /// <param name="link">
    /// What <see cref="BeginAbort"/> returned; <see langword="null"/> does nothing.
    /// </param>
    private void FinishAbort(IMqttLink? link)
    {
        if (link is not null && IssueAbort(link) is { } thrown)
        {
            LogFault(thrown, secondary: null);
        }
    }

    /// <summary>
    /// Aborts the link for a registered abort, outside the lock, then clears <see cref="_aborting"/> in a hold of
    /// its own and, when the send path is not taken then, tells whoever waits for the connection to be idle (the end
    /// of the run), never inline. When the path is taken, the hold that gives it up tells them. It never throws.
    /// </summary>
    /// <param name="link">
    /// The link <see cref="BeginAbort"/> returned.
    /// </param>
    /// <returns>
    /// What the link threw; <see langword="null"/> when it threw nothing. A link that threw may not have been
    /// aborted: the sweep aborts it again at the close deadline.
    /// </returns>
    private Exception? IssueAbort(IMqttLink link)
    {
        Exception? thrown = null;
        try
        {
            link.Abort();
        }
        catch (Exception exception)
        {
            thrown = exception;
        }

        TaskCompletionSource? idle = null;
        lock (_sendLock)
        {
            _aborting = false;
            if (!_writing)
            {
                idle = _sendIdle;
                _sendIdle = null;
            }
        }

        idle?.TrySetResult();
        return thrown;
    }

    /// <summary>
    /// Acts on the two deadlines of reading for an Active connection: begins the close with 0x82
    /// (<see cref="CloseCause.PartialPacketTimedOut"/>) when a read buffer has been held without a complete packet
    /// for <see cref="ServerLimits.PartialPacketDeadline"/> (from its rent, or, for a packet that follows another in
    /// the same read, from that read), and with 0x8D
    /// (<see cref="CloseCause.KeepAliveTimedOut"/>) when no complete packet has come for 1.5 times the
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
            BeginClose(CloseCause.PartialPacketTimedOut, MqttReasonCode.ProtocolError, reasonString: null);
            return;
        }

        long keepAlive = _keepAliveSeconds * _server.Time.TimestampFrequency * ServerLimits.KeepAliveFactorNumerator
                         / ServerLimits.KeepAliveFactorDenominator;
        if (now - Volatile.Read(ref _lastPacketAt) >= keepAlive)
        {
            BeginClose(CloseCause.KeepAliveTimedOut, MqttReasonCode.KeepAliveTimeout, reasonString: null);
        }
    }

    /// <summary>
    /// Ends the host's wait at the authentication deadline, or when the server stops: takes the cancellation source
    /// out of its field, on the calling thread, leaving the mark that the host's call was cancelled
    /// (<see cref="s_hostCallCancelled"/>) in its place, and hands the source to the thread pool, which cancels and
    /// disposes it. When the connect phase took it first, the host has answered and nothing is done. So the source
    /// is still cancelled and disposed exactly once, and the token's callbacks (the host's, and the connect phase's
    /// continuation with the CONNACK 0x88 and the end of the run) do not run on the sweep's thread or on the thread
    /// that stops the server; the one exception is a thread pool that cannot take the work item (out of memory),
    /// when the source is cancelled and disposed on the calling thread, because nothing else would. The time is
    /// written to <see cref="_stateEnteredAt"/> before the mark is set, so whoever sees the mark reads the time of
    /// this cancellation: the sweep measures from it how long the host's call has stayed out
    /// (<see cref="EndUnanswered"/>).
    /// </summary>
    private void CancelAuthentication()
    {
        CancellationTokenSource? source = Volatile.Read(ref _authentication);
        if (source is null || ReferenceEquals(source, s_hostCallCancelled))
        {
            return;
        }

        // Should the exchange lose (the host answered at this moment), the stamp is unused: the connect phase is on
        // its way out of Authenticating.
        Volatile.Write(ref _stateEnteredAt, _server.Time.GetTimestamp());
        if (!ReferenceEquals(Interlocked.CompareExchange(ref _authentication, s_hostCallCancelled, source), source))
        {
            return;
        }

        try
        {
            ThreadPool.UnsafeQueueUserWorkItem(static state => state.Connection.CancelOnThreadPool(state.Source),
                (Source: source, Connection: this), preferLocal: false);
        }
        catch (OutOfMemoryException)
        {
            // The pool could not take the work item, and the mark is already in the field, so nothing else would
            // cancel or dispose the source: a host that waits on its token would wait for ever. It is cancelled
            // here, the one case in which the token's callbacks run on the calling thread.
            CancelOnThreadPool(source);
        }
    }

    /// <summary>
    /// Cancels and disposes a cancellation source of the Authenticating state, on a thread-pool thread (or on the
    /// thread of <see cref="CancelAuthentication"/> when the pool could not take the work item). A token
    /// callback that throws is the host's bug: it is logged as an error of <see cref="IMqttWireHandler.OnConnectAsync"/>
    /// and the token is cancelled all the same; a logging provider that throws then is swallowed by
    /// <see cref="LogEnd"/>, so the work item never fails.
    /// </summary>
    /// <param name="source">
    /// The source; nothing else cancels or disposes it.
    /// </param>
    private void CancelOnThreadPool(CancellationTokenSource source)
    {
        try
        {
            source.Cancel();
        }
        catch (AggregateException exception)
        {
            LogEnd(CloseCause.HandlerThrew, MqttReasonCode.Success, payloadLength: 0,
                nameof(IMqttWireHandler.OnConnectAsync), exception);
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
    /// <param name="cause">
    /// Why the connection ends.
    /// </param>
    /// <param name="left">
    /// The state the connection left when the result is <see langword="true"/>; otherwise the state it was in.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when this call made the move; <see langword="false"/> when another had made it, and
    /// nothing changed.
    /// </returns>
    private bool TryEnterClosing(CloseCause cause, out ConnectionState left)
    {
        left = _state;
        while (left < ConnectionState.Closing)
        {
            if (TryEnterClosingFrom(left, cause))
            {
                return true;
            }

            left = _state;
        }

        return false;
    }

    /// <summary>
    /// Moves the connection to Closing only from one state; the winner records the cause and writes
    /// <see cref="_closingAt"/>, which nothing else writes. The caller holds the send path's lock: every move to
    /// Closing is made under it, so that the state read under that lock tells the send path exactly whether anything
    /// may still be added or written. The winner's caller logs the end once it has released the lock.
    /// </summary>
    /// <param name="expected">
    /// The state the move must start from; it is below Closing.
    /// </param>
    /// <param name="cause">
    /// Why the connection ends.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when this call made the move; <see langword="false"/> when the connection was in
    /// another state, and nothing changed.
    /// </returns>
    private bool TryEnterClosingFrom(ConnectionState expected, CloseCause cause)
    {
        Debug.Assert(_sendLock.IsHeldByCurrentThread, "Every move to Closing is made under the send path's lock.");
        if (Interlocked.CompareExchange(ref _state, ConnectionState.Closing, expected) != expected)
        {
            return false;
        }

        CloseCause = cause;
        Volatile.Write(ref _closingAt, _server.Time.GetTimestamp());
        return true;
    }

    /// <summary>
    /// Writes the one log line of an end of this connection (design 6.12), or of an exception a handler, the send
    /// path or the run threw: the one place that maps why a connection ended to its event. Called, outside the send
    /// path's lock, by the call that won the move to Closing, with no exception; and by the code that caught an
    /// exception, with it: <see cref="CloseCause.HandlerThrew"/> for event 15, <see cref="CloseCause.ConnectionLost"/>
    /// for event 21 (the run ended with an exception that is not a failure of its link), and
    /// <see cref="CloseCause.SendPathFaulted"/> for event 20.
    /// A line names the client by its client id; a connection that has none yet is named by its remote end point,
    /// through <see cref="WriteRemoteEndLine"/>. Each line carries the fields of its event, never a password, a
    /// payload, a topic or the host's reason string. With the event's level off nothing is read, built or boxed. It
    /// never throws: a logging provider that throws is swallowed here, and only here.
    /// </summary>
    /// <remarks>
    /// A call that loses the move to Closing logs nothing, with one deliberate exception: the lines that carry an
    /// exception, <see cref="CloseCause.HandlerThrew"/> (event 15), <see cref="CloseCause.SendPathFaulted"/>
    /// (event 20) and the run's (event 21), are written only by the code that caught the exception, after its own
    /// close has begun and whether or not that close won. The winner of the move has no exception in hand, and an
    /// exception that is the host's bug, or MqttWire's, must never be lost because another end came first. So an end
    /// can show two lines: the winner's (for example 14, the host closed the connection, 6, the keep-alive deadline,
    /// or 18, a lost connection) and then 15, 20 or 21.
    /// </remarks>
    /// <param name="cause">
    /// Why the connection ended.
    /// </param>
    /// <param name="code">
    /// The reason code that went on the wire, or would have; for <see cref="CloseCause.ClientDisconnected"/>, the
    /// client's own.
    /// </param>
    /// <param name="payloadLength">
    /// The length of the payload that was too large to send, for <see cref="CloseCause.MessageTooLargeToSend"/>;
    /// otherwise 0.
    /// </param>
    /// <param name="method">
    /// The name of the handler method that threw, with <see cref="CloseCause.HandlerThrew"/> and an exception;
    /// otherwise <see langword="null"/>.
    /// </param>
    /// <param name="exception">
    /// What was thrown, from the code that caught it; <see langword="null"/> from the winner of the move.
    /// </param>
    private void LogEnd(CloseCause cause, MqttReasonCode code, int payloadLength, string? method, Exception? exception)
    {
        ILogger logger = _server.Logger;

        // Deliberately swallowed below, and the only catch around a log call that does nothing: the end has been
        // decided and its state has moved, so a logging provider that throws has nowhere to report to, and throwing on
        // would break the close, the sweep or the fault path that called this.
#pragma warning disable RCS1075 // Avoid empty catch clause that catches System.Exception: see the comment above.
        try
        {
            if (ClientId.Length == 0)
            {
                WriteRemoteEndLine(logger, cause, code, exception);
            }
            else if (exception is null)
            {
                WriteEndLine(logger, cause, code, payloadLength);
            }
            else if (cause == CloseCause.HandlerThrew)
            {
                ServerLog.HandlerThrew(logger, ClientId, method ?? string.Empty, exception);
            }
            else if (cause == CloseCause.ConnectionLost)
            {
                ServerLog.RunFaulted(logger, ClientId, exception);
            }
            else
            {
                ServerLog.SendPathFaulted(logger, ClientId, exception);
            }
        }
        catch (Exception)
        {
            // Nowhere left to report it.
        }
#pragma warning restore RCS1075
    }

    /// <summary>
    /// Writes the line of an end of a connection that has no client id yet: it sent no CONNECT that was read, so the
    /// remote end point names it. Only these ends can happen then: a malformed or refused first packet or CONNECT
    /// (events 1 and 2), the connect deadline (5), the server's stop (16), a lost link (18), a fault of the send
    /// path (20) and a run that faulted (21). The level is checked first, so that the end point is read and turned into text only for a line
    /// that is written. Called only by <see cref="LogEnd"/>, which swallows what a logging provider throws.
    /// </summary>
    /// <param name="logger">
    /// The server's logger.
    /// </param>
    /// <param name="cause">
    /// Why the connection ended.
    /// </param>
    /// <param name="code">
    /// The reason code of the end.
    /// </param>
    /// <param name="exception">
    /// What the send path threw, with <see cref="CloseCause.SendPathFaulted"/>, or the run, with
    /// <see cref="CloseCause.ConnectionLost"/>, from the code that caught it; otherwise <see langword="null"/>.
    /// </param>
    private void WriteRemoteEndLine(ILogger logger, CloseCause cause, MqttReasonCode code, Exception? exception)
    {
        if (!logger.IsEnabled(exception is null ? LogLevel.Debug : LogLevel.Error))
        {
            return;
        }

        string remote = GetRemoteEndPoint();
        switch (cause)
        {
            case CloseCause.MalformedPacket:
                ServerLog.MalformedPacketFrom(logger, remote, code);
                break;

            case CloseCause.ProtocolError:
                ServerLog.ProtocolErrorFrom(logger, remote, code);
                break;

            case CloseCause.ConnectTimedOut:
                ServerLog.ConnectTimedOut(logger, remote);
                break;

            case CloseCause.ServerStopping:
                ServerLog.ServerStoppingFrom(logger, remote);
                break;

            case CloseCause.ConnectionLost when exception is not null:
                ServerLog.RunFaultedFrom(logger, remote, exception);
                break;

            case CloseCause.ConnectionLost:
                ServerLog.ConnectionLostFrom(logger, remote);
                break;

            case CloseCause.SendPathFaulted when exception is not null:
                ServerLog.SendPathFaultedFrom(logger, remote, exception);
                break;

            default:
                // The winner of a fault's close: the code that caught the exception writes the line, with it.
                Debug.Assert(exception is null && cause is CloseCause.HandlerThrew or CloseCause.SendPathFaulted,
                    "No other end can happen before the CONNECT is read.");
                break;
        }
    }

    /// <summary>
    /// Gets the remote end point of the request as text, for a line of a connection that has no client id yet; read
    /// only once the line's level is known to be on.
    /// </summary>
    /// <returns>
    /// The address and port (for example <c>192.0.2.7:50123</c>, or <c>[2001:db8::1]:50123</c>), or "unknown" when
    /// the request has no remote address, or the run has ended and let go of the request's context.
    /// </returns>
    private string GetRemoteEndPoint()
    {
        // Read once: a line written from another thread can meet the end of the run.
        if (HttpContext is not { } httpContext)
        {
            return "unknown";
        }

        ConnectionInfo connection = httpContext.Connection;
        return connection.RemoteIpAddress is { } address
            ? new IPEndPoint(address, connection.RemotePort).ToString()
            : "unknown";
    }

    /// <summary>
    /// Writes the line of an end decided by the winner of the move to Closing, for a connection with a client id and
    /// an end whose line carries more than the client id: a reason code or a payload's length; the others go on to
    /// <see cref="WriteClientIdEndLine"/>. Called only by <see cref="LogEnd"/>, which swallows what a logging provider
    /// throws.
    /// </summary>
    /// <param name="logger">
    /// The server's logger.
    /// </param>
    /// <param name="cause">
    /// Why the connection ended.
    /// </param>
    /// <param name="code">
    /// The reason code that went on the wire, or would have; for <see cref="CloseCause.ClientDisconnected"/>, the
    /// client's own.
    /// </param>
    /// <param name="payloadLength">
    /// The length of the payload that was too large to send, for <see cref="CloseCause.MessageTooLargeToSend"/>.
    /// </param>
    private void WriteEndLine(ILogger logger, CloseCause cause, MqttReasonCode code, int payloadLength)
    {
        switch (cause)
        {
            case CloseCause.MalformedPacket:
                ServerLog.MalformedPacket(logger, ClientId, code);
                break;

            case CloseCause.ProtocolError:
                ServerLog.ProtocolError(logger, ClientId, code);
                break;

            case CloseCause.RefusedByHost:
                ServerLog.RefusedByHost(logger, ClientId, code);
                break;

            case CloseCause.AuthenticationTimedOut:
                ServerLog.AuthenticationTimedOut(logger, ClientId);
                break;

            case CloseCause.MessageTooLargeToSend:
                ServerLog.MessageTooLargeToSend(logger, ClientId, payloadLength);
                break;

            case CloseCause.ClosedByHost:
                ServerLog.ClosedByHost(logger, ClientId, code);
                break;

            case CloseCause.ClientDisconnected:
                ServerLog.ClientDisconnected(logger, ClientId, code);
                break;

            default:
                WriteClientIdEndLine(logger, cause);
                break;
        }
    }

    /// <summary>
    /// Writes the line of an end decided by the winner of the move to Closing whose line carries the client id alone.
    /// Writes nothing for <see cref="CloseCause.HandlerThrew"/> and <see cref="CloseCause.SendPathFaulted"/>: the code
    /// that caught the exception writes their line, with it (<see cref="LogEnd"/>).
    /// </summary>
    /// <param name="logger">
    /// The server's logger.
    /// </param>
    /// <param name="cause">
    /// Why the connection ended.
    /// </param>
    private void WriteClientIdEndLine(ILogger logger, CloseCause cause)
    {
        switch (cause)
        {
            case CloseCause.KeepAliveTimedOut:
                ServerLog.KeepAliveTimedOut(logger, ClientId);
                break;

            case CloseCause.PartialPacketTimedOut:
                ServerLog.PartialPacketTimedOut(logger, ClientId);
                break;

            case CloseCause.WriteTimedOut:
                ServerLog.WriteTimedOut(logger, ClientId);
                break;

            case CloseCause.AcknowledgementTimedOut:
                ServerLog.AcknowledgementTimedOut(logger, ClientId);
                break;

            case CloseCause.OutgoingLimitPassed:
                ServerLog.OutgoingLimitPassed(logger, ClientId);
                break;

            case CloseCause.ReceiveMaximumPassed:
                ServerLog.ReceiveMaximumPassed(logger, ClientId);
                break;

            case CloseCause.TakenOver:
                ServerLog.TakenOver(logger, ClientId);
                break;

            case CloseCause.ServerStopping:
                ServerLog.ServerStopping(logger, ClientId);
                break;

            case CloseCause.ConnectionLost:
                ServerLog.ConnectionLost(logger, ClientId);
                break;

            default:
                // Written with the exception by the code that caught it; a new member must get its own arm above.
                Debug.Assert(cause is CloseCause.HandlerThrew or CloseCause.SendPathFaulted,
                    "Every other cause has its own line.");
                break;
        }
    }
}
