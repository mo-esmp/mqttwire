using System.Net.WebSockets;
using Microsoft.Extensions.Logging;
using MqttWire.Server;
using MqttWire.Server.Internal;
using MqttWire.Tests.Client;
using MqttWire.Tests.Codec;

namespace MqttWire.Tests.Server;

/// <summary>
/// Tests of the connect phase: MqttWire's own checks on the CONNECT, the host's <c>OnConnectAsync</c> with its
/// deadline, and the CONNACK.
/// </summary>
public sealed class ServerConnectTests
{
    /// <summary>
    /// The event id of the log entry for a host that did not answer in time.
    /// </summary>
    private const int AuthenticationTimedOutEvent = 4;

    /// <summary>
    /// The event id of the log entry for a handler that threw.
    /// </summary>
    private const int HandlerThrewEvent = 15;

    /// <summary>
    /// The event id of the log entry for an answer that came after the deadline.
    /// </summary>
    private const int LateConnectResultEvent = 19;

    /// <summary>
    /// Gets the CONNECT packets that MqttWire refuses itself, each with the CONNACK it answers.
    /// </summary>
    public static TheoryData<byte[], byte[]> NotAcceptable { get; } = new()
    {
        { ServerPackets.ConnectVersion4, ServerPackets.ConnAckRefused(MqttReasonCode.UnsupportedProtocolVersion) },
        { ServerPackets.ConnectEmptyClientId, ServerPackets.ConnAckRefused(MqttReasonCode.ClientIdentifierNotValid) },
        { ServerPackets.ConnectWithAuthMethod, ServerPackets.ConnAckRefused(MqttReasonCode.BadAuthenticationMethod) },
        { ServerPackets.ConnectWithWill, ServerPackets.ConnAckWillRefused },
        {
            ServerPackets.BuildConnect(flags: 0x02, properties: [], payload: ServerPackets.Field(Enumerable.Repeat((byte)'a', 257).ToArray())),
            ServerPackets.ConnAckRefused(MqttReasonCode.ClientIdentifierNotValid)
        },
        {
            ServerPackets.BuildConnect(flags: 0x02, properties: Hex.Parse("27 00 00 0F FF"), payload: ServerPackets.Field("c1"u8)),
            ServerPackets.ConnAckRefused(MqttReasonCode.PacketTooLarge)
        },
        {
            ServerPackets.BuildConnect(flags: 0x02, properties: Hex.Parse("21 00 00"), payload: ServerPackets.Field("c1"u8)),
            ServerPackets.ConnAckRefused(MqttReasonCode.ProtocolError)
        },
        {
            ServerPackets.BuildConnect(flags: 0x02, properties: [], payload: ServerPackets.Field([0xC3, 0x28])),
            ServerPackets.ConnAckRefused(MqttReasonCode.MalformedPacket)
        },
        { Hex.Parse("10 81 80 01"), ServerPackets.ConnAckRefused(MqttReasonCode.PacketTooLarge) },
    };

    /// <summary>
    /// Proves that a valid CONNECT is answered with the fixed CONNACK: success, Receive Maximum 256 and no
    /// subscription identifiers.
    /// </summary>
    [Fact]
    public void Connect_ValidConnect_AnswersConnAckWithTheFixedProperties()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Accept();

        // Act
        connection.Deliver(ServerPackets.Connect);

        // Assert
        connection.TakeWritten().ShouldBe(ServerPackets.ConnAck);
        connection.Link.ClosedWith.ShouldBeNull();
        connection.Run.IsCompleted.ShouldBeFalse();
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a client that asks for no keep-alive is told 60 seconds through Server Keep Alive.
    /// </summary>
    [Fact]
    public void Connect_KeepAliveZero_AnnouncesServerKeepAlive60()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Accept();

        // Act
        connection.Deliver(ServerPackets.ConnectKeepAlive0);

        // Assert
        connection.TakeWritten().ShouldBe(ServerPackets.ConnAckKeepAlive60);
    }

    /// <summary>
    /// Proves that a client that asks for a keep-alive over 60 seconds is told 60 seconds through Server Keep Alive.
    /// </summary>
    [Fact]
    public void Connect_KeepAlive61_AnnouncesServerKeepAlive60()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Accept();

        // Act
        connection.Deliver(ServerPackets.ConnectKeepAlive61);

        // Assert
        connection.TakeWritten().ShouldBe(ServerPackets.ConnAckKeepAlive60);
    }

    /// <summary>
    /// Proves that a server whose highest QoS is 1 announces Maximum QoS 1 in the CONNACK.
    /// </summary>
    [Fact]
    public void Connect_MaxQoS1_AnnouncesMaximumQoS1()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create(static options => options.MaxQoS = MqttQoS.AtLeastOnce);
        TestConnection connection = harness.Accept();

        // Act
        connection.Deliver(ServerPackets.Connect);

        // Assert
        connection.TakeWritten().ShouldBe(ServerPackets.ConnAckMaxQoS1);
    }

    /// <summary>
    /// Proves that a client that asks for a session that outlives the connection is told Session Expiry Interval 0.
    /// </summary>
    [Fact]
    public void Connect_SessionExpiryAsked_AnswersSessionExpiry0()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Accept();

        // Act
        connection.Deliver(ServerPackets.ConnectSessionExpiry60);

        // Assert
        connection.TakeWritten().ShouldBe(ServerPackets.ConnAckSessionExpiry0);
    }

    /// <summary>
    /// Proves that the user properties the host returns with its acceptance are carried, last, in the CONNACK.
    /// </summary>
    [Fact]
    public void Connect_HostReturnsUserProperties_CarriesThemInConnAck()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        harness.Handler.Connect = static (_, _) =>
            ValueTask.FromResult(MqttConnectResult.Accept(userProperties: [new MqttWireUserProperty("gw-player", "p1")]));
        TestConnection connection = harness.Accept();
        byte[] expected =
        [
            .. Hex.Parse("20 18 00 00 15 21 01 00 29 00 26 00 09"), .. "gw-player"u8, .. Hex.Parse("00 02"), .. "p1"u8,
        ];

        // Act
        connection.Deliver(ServerPackets.Connect);

        // Assert
        connection.TakeWritten().ShouldBe(expected);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that the user name and the password reach the host as copies that stay readable after the read
    /// buffer is back in the pool.
    /// </summary>
    [Fact]
    public void Connect_UserNameAndPassword_ReachTheHostAsCopies()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        MqttConnectRequest? seen = null;
        harness.Handler.Connect = (request, _) =>
        {
            seen = request;
            return ValueTask.FromResult(MqttConnectResult.Accept());
        };
        TestConnection connection = harness.Accept();

        // Act
        connection.Deliver(ServerPackets.ConnectUserAndPassword);

        // Assert
        harness.Pool.Outstanding.ShouldBe(0);
        seen.ShouldNotBeNull();
        seen.ClientId.ShouldBe("c1");
        seen.UserName.ShouldBe("u");
        seen.HasPassword.ShouldBeTrue();
        seen.Password.ToArray().ShouldBe([0x70]);
        harness.Pool.IsViewIntoRentedBuffer(seen.Password.Span).ShouldBeFalse();
        connection.TakeWritten().ShouldBe(ServerPackets.ConnAck);
    }

    /// <summary>
    /// Proves that the request the host is given carries the HTTP context of the WebSocket request.
    /// </summary>
    [Fact]
    public void Connect_RequestCarriesTheHttpContext()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        MqttConnectRequest? seen = null;
        harness.Handler.Connect = (request, _) =>
        {
            seen = request;
            return ValueTask.FromResult(MqttConnectResult.Accept());
        };
        TestConnection connection = harness.Accept();

        // Act
        connection.Deliver(ServerPackets.Connect);

        // Assert
        seen.ShouldNotBeNull();
        ReferenceEquals(seen.HttpContext, connection.HttpContext).ShouldBeTrue();
    }

    /// <summary>
    /// Proves that a CONNECT that fails one of MqttWire's own checks is answered with a CONNACK carrying the check's
    /// reason code and closed, without the host being asked and with every buffer back in the pool.
    /// </summary>
    /// <param name="connect">
    /// The CONNECT, or only the fixed header of one.
    /// </param>
    /// <param name="connAck">
    /// The CONNACK it is answered with.
    /// </param>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Theory]
    [MemberData(nameof(NotAcceptable))]
    public async Task Connect_NotAcceptable_IsRefusedWithoutCallingTheHost(byte[] connect, byte[] connAck)
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Accept();

        // Act
        connection.Deliver(connect);

        // Assert
        connection.TakeWritten().ShouldBe(connAck);
        connection.Link.ClosedWith.ShouldBe(WebSocketCloseStatus.NormalClosure);
        await connection.RunEnded();
        harness.Handler.Calls.ShouldBeEmpty();
        harness.Pool.Outstanding.ShouldBe(0);
        harness.Pool.LargestRent.ShouldBeLessThanOrEqualTo(ServerLimits.MaxControlPacketBytes);
    }

    /// <summary>
    /// Proves that a host's refusal is answered with its reason code and reason string, and that a refused
    /// connection is neither admitted nor reported closed.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Connect_HostRefuses_AnswersItsCodeAndReasonString()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        harness.Handler.Connect = static (_, _) =>
            ValueTask.FromResult(MqttConnectResult.Refuse(MqttReasonCode.BadUserNameOrPassword, "no"));
        TestConnection connection = harness.Accept();

        // Act
        connection.Deliver(ServerPackets.Connect);

        // Assert
        connection.TakeWritten().ShouldBe(Hex.Parse("20 08 00 86 05 1F 00 02 6E 6F"));
        connection.Link.ClosedWith.ShouldBe(WebSocketCloseStatus.NormalClosure);
        await connection.RunEnded();
        harness.Handler.Calls.ShouldBe(["OnConnectAsync c1"]);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a host whose <c>OnConnectAsync</c> throws gets the connection refused with CONNACK 0x83 and the
    /// exception logged as an error.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Connect_HostThrows_AnswersConnAck0x83AndLogsAnError()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        InvalidOperationException thrown = new("The host failed.");
        harness.Handler.Connect = (_, _) => throw thrown;
        TestConnection connection = harness.Accept();

        // Act
        connection.Deliver(ServerPackets.Connect);

        // Assert
        connection.TakeWritten().ShouldBe(ServerPackets.ConnAckRefused(MqttReasonCode.ImplementationSpecificError));
        connection.Link.ClosedWith.ShouldBe(WebSocketCloseStatus.NormalClosure);
        await connection.RunEnded();
        harness.Log.Entries.ShouldContain(entry =>
            entry.Id.Id == HandlerThrewEvent && entry.Level == LogLevel.Error && entry.Exception == thrown);
        harness.Handler.Calls.ShouldBe(["OnConnectAsync c1"]);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a host that changes its accepted user properties into one that cannot be carried (U+0000) before
    /// the CONNACK is written gets the connection refused with CONNACK 0x83 in the success CONNACK's place, its bug
    /// logged as an error of <c>OnConnectAsync</c> with the exception, and one <c>OnClosed</c> with
    /// <see cref="MqttCloseReason.HandlerThrew"/>; nothing is logged as a lost link.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Connect_HostsUserPropertiesCannotBeEncoded_ClosesWith0x83AndLogsTheHostsBug()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        MqttWireUserProperty[] properties = [new MqttWireUserProperty("k", "v")];
        harness.Handler.Connect = (_, _) =>
        {
            MqttConnectResult accepted = MqttConnectResult.Accept(userProperties: properties);

            // The host's bug: the array the server keeps until the CONNACK is written is changed after the accept.
            properties[0] = new MqttWireUserProperty("k", "bad\0value");
            return ValueTask.FromResult(accepted);
        };
        TestConnection connection = harness.Accept();

        // Act
        connection.Deliver(ServerPackets.Connect);
        await connection.RunEnded();

        // Assert
        connection.TakeWritten().ShouldBe(ServerPackets.ConnAckRefused(MqttReasonCode.ImplementationSpecificError));
        connection.Link.ClosedWith.ShouldBe(WebSocketCloseStatus.NormalClosure);
        harness.Handler.Calls.Where(static call => call.StartsWith("OnClosed", StringComparison.Ordinal))
            .ShouldBe(["OnClosed c1 HandlerThrew"]);
        harness.Log.Entries.ShouldContain(static entry =>
            entry.Id.Id == HandlerThrewEvent && entry.Level == LogLevel.Error && entry.Exception is ArgumentException
            && entry.Message.Contains("OnConnectAsync", StringComparison.Ordinal));
        harness.Log.Entries.ShouldNotContain(static entry => entry.Id.Id == 18);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a host that does not answer within the authentication deadline has its token cancelled, and the
    /// connection is refused with CONNACK 0x88 and ends although the host's task never completes.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Connect_HostDoesNotAnswerInTenSeconds_AnswersConnAck0x88()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TaskCompletionSource<MqttConnectResult> never = new();
        CancellationToken given = default;
        harness.Handler.Connect = (_, cancellationToken) =>
        {
            given = cancellationToken;
            return new ValueTask<MqttConnectResult>(never.Task);
        };
        TestConnection connection = harness.Accept();
        connection.Deliver(ServerPackets.Connect);
        harness.Advance(ServerLimits.AuthenticationDeadline - ServerLimits.SweepInterval);
        bool cancelledEarly = given.IsCancellationRequested;

        // Act
        harness.Advance(ServerLimits.SweepInterval);
        await connection.RunEnded();

        // Assert
        cancelledEarly.ShouldBeFalse();
        given.IsCancellationRequested.ShouldBeTrue();
        connection.TakeWritten().ShouldBe(ServerPackets.ConnAckRefused(MqttReasonCode.ServerUnavailable));
        connection.Link.ClosedWith.ShouldBe(WebSocketCloseStatus.NormalClosure);
        harness.Log.Entries.ShouldContain(entry =>
            entry.Id.Id == AuthenticationTimedOutEvent && entry.Level == LogLevel.Error);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that an answer a host gives after the authentication deadline is observed, logged and discarded:
    /// nothing is admitted, nothing is reported closed and nothing throws.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Connect_HostIgnoresCancellation_LateResultIsDiscarded()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TaskCompletionSource<MqttConnectResult> late = new();
        harness.Handler.Connect = (_, _) => new ValueTask<MqttConnectResult>(late.Task);
        TestConnection connection = harness.Accept();
        connection.Deliver(ServerPackets.Connect);
        harness.Advance(ServerLimits.AuthenticationDeadline);
        await connection.RunEnded();
        connection.TakeWritten();

        // Act
        late.SetResult(MqttConnectResult.Accept());

        // Assert
        harness.Handler.Calls.ShouldBe(["OnConnectAsync c1"]);
        harness.Log.Entries.ShouldContain(entry =>
            entry.Id.Id == LateConnectResultEvent && entry.Level == LogLevel.Debug);
        connection.TakeWritten().ShouldBeEmpty();
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that an <c>OnConnectAsync</c> that fails after the authentication deadline has its exception observed
    /// and logged as an error with event 19, and changes nothing else: nothing is admitted and nothing is written.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Connect_HostFaultsAfterTheDeadline_LogsTheLateFaultAsAnError()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TaskCompletionSource<MqttConnectResult> late = new();
        harness.Handler.Connect = (_, _) => new ValueTask<MqttConnectResult>(late.Task);
        TestConnection connection = harness.Accept();
        connection.Deliver(ServerPackets.Connect);
        harness.Advance(ServerLimits.AuthenticationDeadline);
        await connection.RunEnded();
        connection.TakeWritten().ShouldBe(ServerPackets.ConnAckRefused(MqttReasonCode.ServerUnavailable));
        InvalidOperationException thrown = new("The host's late failure.");

        // Act
        late.SetException(thrown);
        await harness.Log.WhenLoggedAsync(LateConnectResultEvent);

        // Assert
        harness.Log.Entries.ShouldContain(entry =>
            entry.Id.Id == LateConnectResultEvent && entry.Level == LogLevel.Error && entry.Exception == thrown);
        harness.Handler.Calls.ShouldBe(["OnConnectAsync c1"]);
        connection.TakeWritten().ShouldBeEmpty();
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a host that answers at the very sweep that fires the authentication deadline gets exactly one
    /// CONNACK whichever comes first, with both orders forced: answering from inside the deadline's cancellation,
    /// before the connect phase sees it, the host wins and the CONNACK is success; answering just after the connect
    /// phase saw the cancellation, the deadline wins and the CONNACK is 0x88. In both, <c>OnClosed</c> is called as
    /// many times as <c>OnAdmit</c> accepted, and every buffer is back once the run has ended.
    /// </summary>
    /// <param name="hostFirst">
    /// Whether the host's answer comes before the connect phase sees the cancellation.
    /// </param>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Connect_HostAnswersAtTheSweepOfTheDeadline_WritesExactlyOneConnAck(bool hostFirst)
    {
        // Arrange
        // A token runs its callbacks in the reverse order of their registration. The host registers inside
        // OnConnectAsync, before the connect phase waits on the token, so its answer comes after the connect phase saw
        // the cancellation; the test registers once the connect phase waits, so its answer comes before. The test relies
        // on that order: in another, each case would get the other CONNACK, and the test would fail, not pass.
        ServerHarness harness = ServerHarness.Create();
        TaskCompletionSource<MqttConnectResult> answer = new();
        CancellationToken given = default;
        harness.Handler.Connect = (_, cancellationToken) =>
        {
            given = cancellationToken;
            if (!hostFirst)
            {
                cancellationToken.Register(() => answer.TrySetResult(MqttConnectResult.Accept()));
            }

            return new ValueTask<MqttConnectResult>(answer.Task);
        };
        TestConnection connection = harness.Accept();
        connection.Deliver(ServerPackets.Connect);
        if (hostFirst)
        {
            given.Register(() => answer.TrySetResult(MqttConnectResult.Accept()));
        }

        // Act
        harness.Advance(ServerLimits.AuthenticationDeadline);
        await connection.Link.WhenWrittenAsync(1);
        connection.Link.EndFromPeer();
        await connection.RunEnded();

        // Assert
        connection.TakeWritten().ShouldBe(
            hostFirst ? ServerPackets.ConnAck : ServerPackets.ConnAckRefused(MqttReasonCode.ServerUnavailable));
        int admitted = harness.Handler.Calls.Count(static call => call.StartsWith("OnAdmit", StringComparison.Ordinal));
        int closed = harness.Handler.Calls.Count(static call => call.StartsWith("OnClosed", StringComparison.Ordinal));
        admitted.ShouldBe(hostFirst ? 1 : 0);
        closed.ShouldBe(admitted);
        harness.Pool.Outstanding.ShouldBe(0);
        harness.Pool.FaultyReturns.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a token callback of the host's that throws at the authentication deadline is logged as an error
    /// of <c>OnConnectAsync</c>, and the connection still gets CONNACK 0x88 and ends.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Connect_HostTokenCallbackThrowsAtTheDeadline_LogsItAndAnswersConnAck0x88()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TaskCompletionSource<MqttConnectResult> never = new();
        InvalidOperationException thrown = new("The host's callback failed.");
        harness.Handler.Connect = (_, cancellationToken) =>
        {
            cancellationToken.Register(() => throw thrown);
            return new ValueTask<MqttConnectResult>(never.Task);
        };
        TestConnection connection = harness.Accept();
        connection.Deliver(ServerPackets.Connect);
        Task logged = harness.Log.WhenLoggedAsync(HandlerThrewEvent);

        // Act
        harness.Advance(ServerLimits.AuthenticationDeadline);
        await connection.RunEnded();
        await logged.Within("The log entry");

        // Assert
        connection.TakeWritten().ShouldBe(ServerPackets.ConnAckRefused(MqttReasonCode.ServerUnavailable));
        connection.Link.ClosedWith.ShouldBe(WebSocketCloseStatus.NormalClosure);
        harness.Log.Entries.ShouldContain(entry =>
            entry.Id.Id == HandlerThrewEvent && entry.Level == LogLevel.Error
            && entry.Message.Contains("OnConnectAsync", StringComparison.Ordinal)
            && entry.Exception is AggregateException
            && ((AggregateException)entry.Exception).InnerExceptions.Contains(thrown));
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that the host's token is cancelled as soon as the WebSocket request is aborted, before the
    /// authentication deadline, and that the connection then ends without a CONNACK.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Connect_SocketClosesWhileTheHostDecides_CancelsTheHostsToken()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TaskCompletionSource<MqttConnectResult> never = new();
        CancellationToken given = default;
        harness.Handler.Connect = (_, cancellationToken) =>
        {
            given = cancellationToken;
            return new ValueTask<MqttConnectResult>(never.Task);
        };
        TestConnection connection = harness.Accept();
        connection.Deliver(ServerPackets.Connect);

        // Act
        await connection.RequestAborted.CancelAsync();
        await connection.RunEnded();

        // Assert
        given.IsCancellationRequested.ShouldBeTrue();
        connection.TakeWritten().ShouldBeEmpty();
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that no buffer is held while the host decides when nothing lies behind the CONNECT.
    /// </summary>
    [Fact]
    public void Connect_WhileTheHostDecides_HoldsNoBuffer()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        int heldWhileDeciding = -1;
        harness.Handler.Connect = (_, _) =>
        {
            heldWhileDeciding = harness.Pool.Outstanding;
            return ValueTask.FromResult(MqttConnectResult.Accept());
        };
        TestConnection connection = harness.Accept();

        // Act
        connection.Deliver(ServerPackets.Connect);

        // Assert
        heldWhileDeciding.ShouldBe(0);
        connection.TakeWritten().ShouldBe(ServerPackets.ConnAck);
    }

    /// <summary>
    /// Proves that packets sent right behind the CONNECT, in the same WebSocket message, are handled in order after
    /// the CONNACK, neither lost nor answered before it.
    /// </summary>
    [Fact]
    public void Connect_PacketsBehindTheConnect_AreHandledAfterTheConnAck()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Accept();

        // Act
        connection.Deliver([.. ServerPackets.Connect, .. ServerPackets.Subscribe, .. ClientPackets.PingReq]);

        // Assert
        connection.TakeWritten().ShouldBe([.. ServerPackets.ConnAck, .. ServerPackets.SubAckGrantedQoS1, .. ClientPackets.PingResp]);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a CONNECT of about 6,000 bytes delivered in pieces of 1,000 grows the read buffer, reaches the
    /// host once with its password intact, and leaves every buffer back in the pool.
    /// </summary>
    [Fact]
    public void Connect_ConnectOver4096BytesInPieces_GrowsTheBufferAndReturnsIt()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        byte[] password = new byte[5_970];
        for (int i = 0; i < password.Length; i++)
        {
            password[i] = (byte)i;
        }

        byte[] connect = ServerPackets.BuildConnect(flags: 0x42, properties: [],
            payload: [.. ServerPackets.Field("c1"u8), .. ServerPackets.Field(password)]);
        byte[]? seen = null;
        harness.Handler.Connect = (request, _) =>
        {
            seen = request.Password.ToArray();
            return ValueTask.FromResult(MqttConnectResult.Accept());
        };
        TestConnection connection = harness.Accept();

        // Act
        connection.Link.DeliverInChunks(connect, chunkSize: 1_000);

        // Assert
        connect.Length.ShouldBeGreaterThan(ServerLimits.FirstReadBufferBytes);
        harness.Handler.Calls.ShouldBe(["OnConnectAsync c1", "OnAdmit c1 replaces=-"]);
        seen.ShouldBe(password);
        connection.TakeWritten().ShouldBe(ServerPackets.ConnAck);
        harness.Pool.Outstanding.ShouldBe(0);
        harness.Pool.LargestRent.ShouldBeLessThanOrEqualTo(ServerLimits.MaxControlPacketBytes);
    }
}
