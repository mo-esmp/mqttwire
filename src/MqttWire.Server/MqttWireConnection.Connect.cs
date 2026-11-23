using System.Diagnostics;
using System.Net;
using System.Runtime.CompilerServices;
using System.Text;
using Microsoft.Extensions.Logging;
using MqttWire.Codec;
using MqttWire.Server.Internal;

namespace MqttWire.Server;

/// <content>
/// The connect phase: MqttWire's own checks on the CONNECT, the host's <see cref="IMqttWireHandler.OnConnectAsync"/>
/// with its deadline, and the CONNACK.
/// </content>
public sealed partial class MqttWireConnection
{
    /// <summary>
    /// The reason string of the CONNACK that refuses a Will.
    /// </summary>
    private const string WillNotSupported = "Will messages are not supported.";

    /// <summary>
    /// The connect phase: reads the first packet, which must be a complete CONNECT, checks it, asks the host's
    /// <see cref="IMqttWireHandler.OnConnectAsync"/> and, when the host accepts, runs the admission step, which makes
    /// the connection Active with the send path reserved for its CONNACK, and writes the CONNACK as the send path's
    /// first write. A refusal anywhere is answered with a CONNACK carrying its code, then the close. The read buffer
    /// goes back to the pool before the host is asked unless bytes lie behind the CONNECT (plan decision 8); then it
    /// is kept for the loop, which handles them after the CONNACK. Every other way out returns it.
    /// </summary>
    /// <remarks>
    /// No byte of the CONNECT goes back to the pool, which hands an array to its next renter as it is, the password
    /// included (design 6.8, rule 7): the CONNECT's bytes are cleared in the buffer as soon as it has been decoded
    /// and what the host is given has been copied out of it, whether it was accepted, refused or malformed, so a
    /// buffer kept for the packets behind it holds none of it while the host decides; the array a growth leaves is
    /// cleared (<see cref="FrameConnect"/>); and a phase that ends without an Active connection, which may hold
    /// part of a CONNECT that never completed, clears the whole array before it returns it. This is the connect
    /// phase's alone: the read loop of an Active connection clears nothing.
    /// </remarks>
    /// <returns>
    /// <see langword="true"/> when the connection is Active and the loop is to run; <see langword="false"/> when
    /// the phase refused or lost the connection, the close has begun and no buffer is held.
    /// </returns>
    /// <exception cref="Exception">
    /// The wait or a read failed; the run records it. No buffer is held.
    /// </exception>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<bool> ConnectPhaseAsync()
    {
        bool active = false;
        try
        {
            int length = await ReadConnectAsync(RunningLink).ConfigureAwait(false);
            if (length == 0)
            {
                return false;
            }

            MqttConnectRequest? request =
                DecodeConnect(length, out bool announceKeepAlive, out bool sessionExpiryAsked);
            _readBuffer.ConsumeCleared(length);
            if (request is null)
            {
                return false;
            }

            if (_readBuffer.Count == 0)
            {
                // Nothing lies behind the CONNECT: no buffer is held while the host decides.
                _readBuffer.Return(_server.Pool);
            }

            MqttConnectResult? accepted = await AuthenticateAsync(request).ConfigureAwait(false);
            if (accepted is not { } result)
            {
                return false;
            }

            State = result.State;
            if (!Admit())
            {
                // Refused, or closed in or after its admission: no CONNACK; an admitted one gets OnClosed at its end.
                return false;
            }

            SendConnAck(new ConnAckFields
            {
                ReasonCode = MqttReasonCode.Success,
                SessionExpiryInterval = sessionExpiryAsked ? 0u : null,
                ReceiveMaximum = ServerLimits.ReceiveMaximum,
                MaximumQoS = _server.Options.MaxQoS == MqttQoS.AtLeastOnce ? 1 : null,
                SubscriptionIdentifiersAvailable = false,
                ServerKeepAlive = announceKeepAlive ? _keepAliveSeconds : null,
                UserProperties = result.UserProperties,
            });

            // A CONNACK whose write failed at once has moved the connection on to Closing.
            active = _state == ConnectionState.Active;
            return active;
        }
        finally
        {
            if (!active)
            {
                // Every read has ended here, so the buffer can go back, cleared; one not held does nothing.
                _readBuffer.ReturnCleared(_server.Pool);
            }
        }
    }

    /// <summary>
    /// Decodes the complete CONNECT at the start of the read buffer, applies MqttWire's own checks in the order of
    /// design 6.6, records what the connection keeps of it, and copies what the host is given out of the buffer. The
    /// Maximum Packet Size the client announced is recorded first, before any of those checks, so that the CONNACK
    /// of a refusal respects it too (MQTT-3.1.2-24, <see cref="RefuseConnect"/>). A CONNECT the codec refuses is
    /// answered without that limit, which cannot be known then.
    /// </summary>
    /// <param name="length">
    /// The CONNECT's total length.
    /// </param>
    /// <param name="announceKeepAlive">
    /// <see langword="true"/> when the client asked for no keep-alive or for more than 60 seconds, so the CONNACK
    /// carries Server Keep Alive; meaningless when the result is <see langword="null"/>.
    /// </param>
    /// <param name="sessionExpiryAsked">
    /// <see langword="true"/> when the client asked for a Session Expiry Interval over 0, so the CONNACK carries 0;
    /// meaningless when the result is <see langword="null"/>.
    /// </param>
    /// <returns>
    /// The request for the host; <see langword="null"/> when the CONNECT is malformed or not acceptable, and the
    /// close has begun with a CONNACK that carries why.
    /// </returns>
    private MqttConnectRequest? DecodeConnect(int length, out bool announceKeepAlive, out bool sessionExpiryAsked)
    {
        announceKeepAlive = false;
        sessionExpiryAsked = false;
        MqttReasonCode code = ConnectCodec.TryDecode(_readBuffer.GetData()[..length], out ConnectPacket connect);
        if (code != MqttReasonCode.Success)
        {
            CloseUndecodable(code);
            return null;
        }

        // First of all: every packet written from here on, a refusal's CONNACK included, is held to it.
        PropertyBlock properties = connect.Properties;
        _clientMaxPacketSize = properties.TryGetInteger(PropertyId.MaximumPacketSize, out uint maximumPacketSize)
            ? (int)Math.Min(maximumPacketSize, int.MaxValue)
            : int.MaxValue;
        code = CheckConnect(connect, out string? reasonString);
        if (code != MqttReasonCode.Success)
        {
            RefuseConnect(code, reasonString);
            return null;
        }

        _keepAliveSeconds = connect.KeepAliveSeconds is 0 or > ServerLimits.MaxKeepAliveSeconds
            ? (ushort)ServerLimits.MaxKeepAliveSeconds
            : connect.KeepAliveSeconds;
        announceKeepAlive = _keepAliveSeconds != connect.KeepAliveSeconds;
        _clientWindow = properties.TryGetInteger(PropertyId.ReceiveMaximum, out uint receiveMaximum)
            ? (int)Math.Min(receiveMaximum, ServerLimits.ReceiveMaximum)
            : ServerLimits.ReceiveMaximum;
        _problemInformation =
            !properties.TryGetInteger(PropertyId.RequestProblemInformation, out uint problemInformation)
            || problemInformation != 0;
        sessionExpiryAsked = properties.TryGetInteger(PropertyId.SessionExpiryInterval, out uint sessionExpiry)
                             && sessionExpiry > 0;

        ClientId = Encoding.UTF8.GetString(connect.ClientId);
        return CreateRequest(connect);
    }

    /// <summary>
    /// Refuses a CONNECT that failed one of MqttWire's own checks, within the Maximum Packet Size it announced,
    /// which <see cref="_clientMaxPacketSize"/> already holds (MQTT-3.1.2-24): the close begins with a CONNACK that
    /// carries the code, and the reason string when the packet with it fits (<see cref="ShapeReasonString"/>). When
    /// not even the bare CONNACK fits (five bytes, so a client that announced 1 to 4), no MQTT packet can answer:
    /// the WebSocket is closed with status 1002, as for a first packet that is not a CONNECT, and the log line still
    /// carries the code.
    /// </summary>
    /// <param name="code">
    /// The reason code of the refusal, a failure.
    /// </param>
    /// <param name="reasonString">
    /// The reason string the CONNACK carries when it fits, or <see langword="null"/> for none.
    /// </param>
    private void RefuseConnect(MqttReasonCode code, string? reasonString)
    {
        if (ConnAckCodec.GetSize(new ConnAckFields { ReasonCode = code }) > _clientMaxPacketSize)
        {
            RefuseFirstPacket(CloseCause.ProtocolError, code);
            return;
        }

        BeginClose(CloseCause.ProtocolError, code, reasonString);
    }

    /// <summary>
    /// Applies MqttWire's own checks to a CONNECT the codec accepted, in the order of design 6.6: authentication,
    /// the Will, the client's Maximum Packet Size, the client id. Receive Maximum 0 and Maximum Packet Size 0 need
    /// no check here: the codec already answers both with protocol error.
    /// </summary>
    /// <param name="connect">
    /// The decoded CONNECT.
    /// </param>
    /// <param name="reasonString">
    /// The reason string the refusing CONNACK carries, or <see langword="null"/> for none.
    /// </param>
    /// <returns>
    /// <see cref="MqttReasonCode.Success"/> when the CONNECT may go to the host; otherwise the reason code the
    /// CONNACK refuses it with.
    /// </returns>
    private static MqttReasonCode CheckConnect(in ConnectPacket connect, out string? reasonString)
    {
        reasonString = null;
        PropertyBlock properties = connect.Properties;
        if (properties.Contains(PropertyId.AuthenticationMethod) || properties.Contains(PropertyId.AuthenticationData))
        {
            return MqttReasonCode.BadAuthenticationMethod;
        }

        if (connect.HasWill)
        {
            reasonString = WillNotSupported;
            return MqttReasonCode.NotAuthorized;
        }

        if (properties.TryGetInteger(PropertyId.MaximumPacketSize, out uint maximumPacketSize)
            && maximumPacketSize < ServerLimits.MinClientMaximumPacketSize)
        {
            return MqttReasonCode.PacketTooLarge;
        }

        if (connect.ClientId.IsEmpty
            || connect.ClientId.Length > ServerLimits.MaxClientIdBytes
            || HasControlCharacter(connect.ClientId))
        {
            return MqttReasonCode.ClientIdentifierNotValid;
        }

        return MqttReasonCode.Success;
    }

    /// <summary>
    /// Tells whether a client id holds a character that can end or forge a line where the id is written as text: a
    /// control character (U+0001 to U+001F and U+007F to U+009F, which MQTT lets a receiver refuse) or a line or
    /// paragraph separator (U+2028, U+2029). The client id goes into every log line of its connection and to the
    /// host, so such an id is refused at the CONNECT and never has to be escaped afterwards. It reads the bytes
    /// as they came and allocates nothing.
    /// </summary>
    /// <param name="clientId">
    /// The client id as well-formed UTF-8, which the codec has checked: so a byte below 0x20 or equal to 0x7F is a
    /// character of its own, 0xC2 begins a character of two bytes and 0xE2 one of three.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when the client id holds such a character.
    /// </returns>
    private static bool HasControlCharacter(ReadOnlySpan<byte> clientId)
    {
        for (int i = 0; i < clientId.Length; i++)
        {
            byte first = clientId[i];
            if (first < 0x20 || first == 0x7F)
            {
                return true;
            }

            // U+0080 to U+009F are C2 80 to C2 9F.
            if (first == 0xC2 && i + 1 < clientId.Length && clientId[i + 1] <= 0x9F)
            {
                return true;
            }

            // U+2028 and U+2029 are E2 80 A8 and E2 80 A9.
            if (first == 0xE2 && i + 2 < clientId.Length && clientId[i + 1] == 0x80
                && (clientId[i + 2] == 0xA8 || clientId[i + 2] == 0xA9))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Creates the request for the host, with copies of the user name, the password and the user properties, so
    /// that nothing in it points into the read buffer.
    /// </summary>
    /// <param name="connect">
    /// The decoded CONNECT; its client id is already in <see cref="ClientId"/>.
    /// </param>
    /// <returns>
    /// The request.
    /// </returns>
    private MqttConnectRequest CreateRequest(in ConnectPacket connect)
    {
        string? userName = connect.HasUserName ? Encoding.UTF8.GetString(connect.UserName) : null;
        byte[]? password = connect.HasPassword ? connect.Password.ToArray() : null;
        List<MqttWireUserProperty>? userProperties = null;
        foreach (Utf8Pair pair in connect.Properties.UserProperties)
        {
            userProperties ??= [];
            userProperties.Add(new MqttWireUserProperty(Encoding.UTF8.GetString(pair.Name),
                Encoding.UTF8.GetString(pair.Value)));
        }

        IPAddress? address = RunningContext.Connection.RemoteIpAddress;
        IPEndPoint? remoteEndPoint =
            address is null ? null : new IPEndPoint(address, RunningContext.Connection.RemotePort);
        return new MqttConnectRequest(ClientId, userName, password,
            userProperties ?? (IReadOnlyList<MqttWireUserProperty>)[], remoteEndPoint, RunningContext.Request.IsHttps,
            RunningContext);
    }

    /// <summary>
    /// Moves the connection to Authenticating and asks the host's <see cref="IMqttWireHandler.OnConnectAsync"/>,
    /// with a token that fires at the authentication deadline or when the request is aborted. The cancellation
    /// source exists only for this call. A refusal, a handler that throws, the deadline, the server's stop and an
    /// abort each begin the close here: CONNACK with the host's code, <c>0x83</c>, <c>0x88</c>, <c>0x8B</c>, and
    /// nothing, respectively. When the server is stopping already (the stop's walk, which ends this connection, has
    /// begun but has not reached it yet), the host is not asked: CONNACK <c>0x8B</c>, or the walk's abort, whichever
    /// comes first. A host task abandoned to the token is
    /// observed later and its outcome logged and discarded.
    /// </summary>
    /// <param name="request">
    /// The request for the host.
    /// </param>
    /// <returns>
    /// The host's acceptance; <see langword="null"/> when the connection is not to go on, and the close has begun.
    /// </returns>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<MqttConnectResult?> AuthenticateAsync(MqttConnectRequest request)
    {
        if (RefuseIfStopping())
        {
            return null;
        }

        CancellationTokenSource source = CancellationTokenSource.CreateLinkedTokenSource(RunningContext.RequestAborted);
        CancellationToken token = source.Token;
        Volatile.Write(ref _authentication, source);

        // Stamped before the move, so a sweep never sees Authenticating with the accept's stamp. Should the move
        // lose to the connect deadline, the stamp is unused: Closing is measured from its own.
        Volatile.Write(ref _stateEnteredAt, _server.Time.GetTimestamp());
        if (Interlocked.CompareExchange(ref _state, ConnectionState.Authenticating, ConnectionState.Accepted)
            != ConnectionState.Accepted)
        {
            // The connect deadline passed while the CONNECT was decoded.
            ReleaseAuthentication();
            return null;
        }

        Task<MqttConnectResult>? pending = null;
        MqttConnectResult result;
        try
        {
            ValueTask<MqttConnectResult> answer = _server.Handler.OnConnectAsync(request, token);
            if (answer.IsCompleted)
            {
                // A host that answers at once: completes here, without a task.
                result = await answer.ConfigureAwait(false);
            }
            else
            {
                pending = answer.AsTask();
                result = await pending.WaitAsync(token).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            ReleaseAuthentication();
            RefuseUnanswered(pending);
            return null;
        }
        catch (Exception exception)
        {
            ReleaseAuthentication();
            BeginClose(CloseCause.HandlerThrew, MqttReasonCode.ImplementationSpecificError, reasonString: null);
            LogEnd(CloseCause.HandlerThrew, MqttReasonCode.ImplementationSpecificError, payloadLength: 0,
                nameof(IMqttWireHandler.OnConnectAsync), exception);
            return null;
        }

        ReleaseAuthentication();
        if (!result.IsAccepted)
        {
            BeginClose(CloseCause.RefusedByHost, result.ReasonCode, result.ReasonString);
            return null;
        }

        return result;
    }

    /// <summary>
    /// The admission step, between the host's acceptance and the CONNACK (design 6.6, "Admission and takeover"):
    /// asks the server to admit the connection, which calls the host's <see cref="IMqttWireHandler.OnAdmit"/> under
    /// the registry's lock; then, with the lock released and before the CONNACK is handed to the link, closes the
    /// connection this one replaced with <c>0x8E</c>. A replaced connection that is already closing keeps its own
    /// reason. A refusal begins the close with a CONNACK carrying its code; an <see cref="IMqttWireHandler.OnAdmit"/>
    /// that throws is logged and begins it with <c>0x83</c>. Neither changes the client-id table, and neither
    /// connection gets <see cref="IMqttWireHandler.OnClosed"/>. A connection that was admitted but closed during or
    /// after the step (by the host from inside <see cref="IMqttWireHandler.OnAdmit"/>, or by a takeover) gets no
    /// CONNACK: that close found the send path reserved for the CONNACK and aborted the link, so no packet was
    /// written, and the send path is given up here. The reservation never outlives this method unless the CONNACK is
    /// to be written: should the close of the replaced connection throw, which it does not, this connection ends as
    /// a fault (event 20) and gives the path up like one closed in the step.
    /// </summary>
    /// <returns>
    /// <see langword="true"/> when the connection was admitted and is Active: the send path is reserved for its
    /// CONNACK, which the caller writes now. <see langword="false"/> when it was refused, or was admitted and has
    /// begun to close: the close has begun, the send path is not reserved, and no CONNACK is to be written; an
    /// admitted connection gets <see cref="IMqttWireHandler.OnClosed"/> at its end.
    /// </returns>
    private bool Admit()
    {
        MqttAdmitResult admission;
        MqttWireConnection? replaced;
        try
        {
            admission = _server.Admit(this, out replaced);
        }
        catch (Exception exception)
        {
            BeginClose(CloseCause.HandlerThrew, MqttReasonCode.ImplementationSpecificError, reasonString: null);
            LogEnd(CloseCause.HandlerThrew, MqttReasonCode.ImplementationSpecificError, payloadLength: 0,
                nameof(IMqttWireHandler.OnAdmit), exception);
            return false;
        }

        if (!admission.IsAccepted)
        {
            CloseCause cause = _server.IsStopping ? CloseCause.ServerStopping : CloseCause.RefusedByHost;
            BeginClose(cause, admission.ReasonCode, admission.ReasonString);
            return false;
        }

        try
        {
            replaced?.BeginClose(CloseCause.TakenOver, MqttReasonCode.SessionTakenOver, reasonString: null);
        }
        catch (Exception exception)
        {
            // The reserved send path must not be left taken, or this connection would never end: it ends as a
            // fault, which finds the path reserved and aborts the link, and gives the path up below.
            EndFaulted(exception);
        }

        if (_state == ConnectionState.Active)
        {
            // A close from here on finds the send path taken until the CONNACK's write has ended, and aborts.
            return true;
        }

        ContinueSending(returned: null, failed: false, InlineWrites);
        return false;
    }

    /// <summary>
    /// Enters the admission step: reserves the send path for the connection's CONNACK, in the hold of the send
    /// path's lock that finds the connection still Authenticating. From here until the CONNACK's write has ended (or
    /// <see cref="LeaveRefusedAdmission"/>), a close finds the send path taken and aborts the link instead of writing,
    /// and a message a host sends once the connection is Active waits behind the CONNACK: no MQTT packet ever
    /// precedes it. Called by the admission step, under the registry's lock, before the host's
    /// <see cref="IMqttWireHandler.OnAdmit"/>.
    /// </summary>
    /// <returns>
    /// <see langword="true"/> when the send path was reserved; <see langword="false"/> when the connection is no
    /// longer Authenticating: it has begun to close and must not be admitted. Nothing changed then.
    /// </returns>
    internal bool TryEnterAdmission()
    {
        lock (_sendLock)
        {
            // Only a close takes the send path before the admission, and it leaves Authenticating in the same hold.
            if (_state != ConnectionState.Authenticating)
            {
                return false;
            }

            _writing = true;
            return true;
        }
    }

    /// <summary>
    /// Gives the send path reserved by <see cref="TryEnterAdmission"/> up when the host refused or threw, so that
    /// the close that follows writes the CONNACK carrying the refusal. Called under the registry's lock. A connection
    /// that was closed during the step stays as that close left it: its link aborted, and no packet written. The
    /// general step that carries the send path on is safe under the registry's lock only because it finds nothing to
    /// do here and gives the path up in its hold: nothing can wait (the connection was never Active, so nothing was
    /// taken or added) and no WebSocket close is pending (a close during the step found the path reserved and only
    /// aborted). Both are asserted.
    /// </summary>
    internal void LeaveRefusedAdmission()
    {
        AssertNothingToSendInAdmission();
        ContinueSending(returned: null, failed: false, InlineWrites);
    }

    /// <summary>
    /// Asserts, in debug builds only, what <see cref="LeaveRefusedAdmission"/> relies on: the send path is reserved,
    /// nothing waits for it (no delivery, no control packet, no piece of a payload, no exchange record) and no
    /// WebSocket close is pending, so carrying it on gives it up without a write, a close or a host call.
    /// </summary>
    [Conditional("DEBUG")]
    private void AssertNothingToSendInAdmission()
    {
        lock (_sendLock)
        {
            Debug.Assert(_writing, "The admission step reserved the send path.");
            Debug.Assert(_state <= ConnectionState.Closing, "The connection's run has not ended.");
            Debug.Assert(_waiting is null && _control.PacketCount == 0 && _remainingPayload.IsEmpty && _records is null,
                "Nothing can wait for a connection that was never Active.");
            Debug.Assert(_closeStatus == 0,
                "A close during the admission step found the path reserved and only aborted.");
        }
    }

    /// <summary>
    /// Records that the host's <see cref="IMqttWireHandler.OnAdmit"/> accepted the connection, so that its end calls
    /// <see cref="IMqttWireHandler.OnClosed"/>, and moves it from Authenticating to Active. Called by the admission
    /// step, under the registry's lock. The send path stays reserved for the CONNACK, which the connect phase writes
    /// next.
    /// </summary>
    /// <remarks>
    /// The keep-alive runs from this moment: it is stamped immediately before the move to Active, so neither a slow
    /// <see cref="IMqttWireHandler.OnConnectAsync"/> nor a slow <see cref="IMqttWireHandler.OnAdmit"/> uses it up,
    /// and the first sweep that sees Active never reads an older stamp. When the connection has moved to Closing
    /// meanwhile, it stays there: it is admitted all the same, and its <see cref="IMqttWireHandler.OnClosed"/>
    /// reports the reason it is closing with. It cannot throw: the caller read the clock before it changed the
    /// table.
    /// </remarks>
    /// <param name="now">
    /// The time of the admission, a timestamp of the server's clock.
    /// </param>
    internal void MarkAdmitted(long now)
    {
        _admitted = true;
        Volatile.Write(ref _lastPacketAt, now);
        Interlocked.CompareExchange(ref _state, ConnectionState.Active, ConnectionState.Authenticating);
    }

    /// <summary>
    /// Ends a connection whose host did not answer before its token fired. Three causes cancel the token, told apart
    /// in this order: the request was aborted, and the close begins alone, since no CONNACK can reach the client;
    /// the server is stopping, and the close begins with CONNACK 0x8B; otherwise the authentication deadline passed,
    /// and the close begins with CONNACK 0x88. A host task still running is observed later. What tells an abort from
    /// the other two is the request's own token: only the sweep, the stop and an abort cancel the linked source, and
    /// an abort cancels the request's token before the linked one. When the request is aborted after the deadline
    /// or the stop fired but before this runs, the socket is gone and no CONNACK is written either; when the stop
    /// begins after the deadline fired but before this runs, the CONNACK carries 0x8B.
    /// </summary>
    /// <param name="pending">
    /// The host's task, or <see langword="null"/> when the host answered at once (with a cancellation).
    /// </param>
    private void RefuseUnanswered(Task<MqttConnectResult>? pending)
    {
        if (pending is not null)
        {
            ObserveLateResult(pending);
        }

        if (RunningContext.RequestAborted.IsCancellationRequested)
        {
            BeginClose(CloseCause.ConnectionLost, MqttReasonCode.Success, reasonString: null);
            return;
        }

        if (RefuseIfStopping())
        {
            return;
        }

        BeginClose(CloseCause.AuthenticationTimedOut, MqttReasonCode.ServerUnavailable, reasonString: null);
    }

    /// <summary>
    /// Begins the close with CONNACK 0x8B when the server is stopping. The connection had its slot before the stop
    /// began (one that took it after aborts itself), so the stop's walk ends it too: whichever comes first wins, and
    /// a stop that begins after this read finds the connection Accepted or Authenticating, and ends it from there.
    /// </summary>
    /// <returns>
    /// <see langword="true"/> when the server is stopping and the close has begun; <see langword="false"/> when it
    /// is not, and nothing changed.
    /// </returns>
    private bool RefuseIfStopping()
    {
        if (!_server.IsStopping)
        {
            return false;
        }

        BeginClose(CloseCause.ServerStopping, MqttReasonCode.ServerShuttingDown, reasonString: null);
        return true;
    }

    /// <summary>
    /// Takes the cancellation source of the Authenticating state out of its field and disposes it, unless the sweep
    /// or the stop took it first and disposes it itself; the mark that one of them left
    /// (<see cref="s_hostCallCancelled"/>) is taken out too, which says that the host's call has come back.
    /// </summary>
    private void ReleaseAuthentication()
    {
        CancellationTokenSource? source = Interlocked.Exchange(ref _authentication, value: null);
        if (source is not null && !ReferenceEquals(source, s_hostCallCancelled))
        {
            source.Dispose();
        }
    }

    /// <summary>
    /// Attaches to a host task the connection no longer waits for a continuation that observes its outcome and logs
    /// it; the outcome is discarded. The continuation holds the logger and the client id, not the connection.
    /// </summary>
    /// <param name="pending">
    /// The host's task.
    /// </param>
    private void ObserveLateResult(Task<MqttConnectResult> pending) =>
        _ = pending.ContinueWith(
            static (task, state) =>
            {
                (ILogger logger, string clientId) =
                    ((ILogger, string))state!; // The state is always this tuple, passed below.
                if (task.IsFaulted)
                {
                    ServerLog.LateConnectResult(logger, LogLevel.Error, clientId, task.Exception?.InnerException);
                }
                else
                {
                    ServerLog.LateConnectResult(logger, LogLevel.Debug, clientId, exception: null);
                }
            },
            ((ILogger)_server.Logger, ClientId),
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
}
