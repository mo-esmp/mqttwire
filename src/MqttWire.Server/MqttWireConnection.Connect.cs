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
    /// <see cref="IMqttWireHandler.OnConnectAsync"/> and, when the host accepts, writes the CONNACK and makes the
    /// connection Active. A refusal anywhere is answered with a CONNACK carrying its code, then the close. The
    /// read buffer goes back to the pool before the host is asked unless bytes lie behind the CONNECT (plan
    /// decision 8); then it is kept for the loop, which handles them after the CONNACK. Every other way out
    /// returns it.
    /// </summary>
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

            MqttConnectRequest? request = DecodeConnect(length, out bool announceKeepAlive, out bool sessionExpiryAsked);
            _readBuffer.Consume(length);
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

            // The admission step of a later task goes here, between the host's acceptance and the CONNACK.
            if (Interlocked.CompareExchange(ref _state, ConnectionState.Active, ConnectionState.Authenticating)
                != ConnectionState.Authenticating)
            {
                return false;
            }

            SendConnAck(new ConnAckFields
            {
                ReasonCode = MqttReasonCode.Success,
                SessionExpiryInterval = sessionExpiryAsked ? 0u : null,
                ReceiveMaximum = ServerLimits.ReceiveMaximum,
                MaximumQoS = _server.Options.MaxQoS == MqttQoS.AtLeastOnce ? (byte)1 : null,
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
                // Every read has ended here, so the buffer can go back; returning one not held does nothing.
                _readBuffer.Return(_server.Pool);
            }
        }
    }

    /// <summary>
    /// Decodes the complete CONNECT at the start of the read buffer, applies MqttWire's own checks in the order of
    /// design 6.6, records what the connection keeps of it, and copies what the host is given out of the buffer.
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
            MqttCloseReason reason = code == MqttReasonCode.MalformedPacket
                ? MqttCloseReason.MalformedPacket
                : MqttCloseReason.ProtocolError;
            BeginClose(reason, code, reasonString: null);
            return null;
        }

        code = CheckConnect(connect, out string? reasonString);
        if (code != MqttReasonCode.Success)
        {
            BeginClose(MqttCloseReason.ProtocolError, code, reasonString);
            return null;
        }

        PropertyBlock properties = connect.Properties;
        _keepAliveSeconds = connect.KeepAliveSeconds is 0 or > ServerLimits.MaxKeepAliveSeconds
            ? (ushort)ServerLimits.MaxKeepAliveSeconds
            : connect.KeepAliveSeconds;
        announceKeepAlive = _keepAliveSeconds != connect.KeepAliveSeconds;
        _clientWindow = properties.TryGetInteger(PropertyId.ReceiveMaximum, out uint receiveMaximum)
            ? (int)Math.Min(receiveMaximum, ServerLimits.ReceiveMaximum)
            : ServerLimits.ReceiveMaximum;
        _clientMaxPacketSize = properties.TryGetInteger(PropertyId.MaximumPacketSize, out uint maximumPacketSize)
            ? (int)Math.Min(maximumPacketSize, int.MaxValue)
            : int.MaxValue;
        _problemInformation = !properties.TryGetInteger(PropertyId.RequestProblemInformation, out uint problemInformation)
            || problemInformation != 0;
        sessionExpiryAsked = properties.TryGetInteger(PropertyId.SessionExpiryInterval, out uint sessionExpiry)
            && sessionExpiry > 0;

        ClientId = Encoding.UTF8.GetString(connect.ClientId);
        return CreateRequest(connect);
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

        if (connect.ClientId.IsEmpty || connect.ClientId.Length > ServerLimits.MaxClientIdBytes)
        {
            return MqttReasonCode.ClientIdentifierNotValid;
        }

        return MqttReasonCode.Success;
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

        IPAddress? address = HttpContext.Connection.RemoteIpAddress;
        IPEndPoint? remoteEndPoint = address is null ? null : new IPEndPoint(address, HttpContext.Connection.RemotePort);
        return new MqttConnectRequest(ClientId, userName, password,
            userProperties ?? (IReadOnlyList<MqttWireUserProperty>)[], remoteEndPoint, HttpContext.Request.IsHttps,
            HttpContext);
    }

    /// <summary>
    /// Moves the connection to Authenticating and asks the host's <see cref="IMqttWireHandler.OnConnectAsync"/>,
    /// with a token that fires at the authentication deadline or when the request is aborted. The cancellation
    /// source exists only for this call. A refusal, a handler that throws, the deadline and an abort each begin
    /// the close here: CONNACK with the host's code, <c>0x83</c>, <c>0x88</c>, and nothing, respectively. A host
    /// task abandoned to the token is observed later and its outcome logged and discarded.
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
        CancellationTokenSource source = CancellationTokenSource.CreateLinkedTokenSource(HttpContext.RequestAborted);
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
            ServerLog.HandlerThrew(_server.Logger, ClientId, nameof(IMqttWireHandler.OnConnectAsync), exception);
            BeginClose(MqttCloseReason.HandlerThrew, MqttReasonCode.ImplementationSpecificError, reasonString: null);
            return null;
        }

        ReleaseAuthentication();
        if (!result.IsAccepted)
        {
            BeginClose(MqttCloseReason.ClosedByHost, result.ReasonCode, result.ReasonString);
            return null;
        }

        return result;
    }

    /// <summary>
    /// Ends a connection whose host did not answer before its token fired: CONNACK 0x88 and the close when the
    /// authentication deadline passed; the close alone when the request was aborted, since no CONNACK can reach the
    /// client then. A host task still running is observed later.
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

        if (HttpContext.RequestAborted.IsCancellationRequested)
        {
            BeginClose(MqttCloseReason.ConnectionLost, MqttReasonCode.Success, reasonString: null);
            return;
        }

        ServerLog.AuthenticationTimedOut(_server.Logger, ClientId);
        BeginClose(MqttCloseReason.ConnectionLost, MqttReasonCode.ServerUnavailable, reasonString: null);
    }

    /// <summary>
    /// Takes the cancellation source of the Authenticating state out of its field and disposes it, unless the
    /// sweep took it first and disposes it itself.
    /// </summary>
    private void ReleaseAuthentication() => Interlocked.Exchange(ref _authentication, null)?.Dispose();

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
                (ILogger logger, string clientId) = ((ILogger, string))state!; // The state is always this tuple, passed below.
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
