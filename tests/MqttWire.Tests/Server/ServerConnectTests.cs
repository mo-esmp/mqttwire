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
    /// The event id of the log entry for a connection that broke the protocol.
    /// </summary>
    private const int ProtocolErrorEvent = 2;

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
    /// Whether the current thread is inside the call that runs the sweep; set and cleared by the test's own thread,
    /// and read by a callback to learn whether it runs inside that call.
    /// </summary>
    [ThreadStatic]
    private static bool t_insideTheSweep;

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
            // Maximum Packet Size 0.
            ServerPackets.BuildConnect(flags: 0x02, properties: Hex.Parse("27 00 00 00 00"), payload: ServerPackets.Field("c1"u8)),
            ServerPackets.ConnAckRefused(MqttReasonCode.ProtocolError)
        },
        {
            // Authentication Data without an Authentication Method: the codec's own answer.
            ServerPackets.BuildConnect(flags: 0x02, properties: Hex.Parse("16 00 01 AA"), payload: ServerPackets.Field("c1"u8)),
            ServerPackets.ConnAckRefused(MqttReasonCode.ProtocolError)
        },
        {
            // Topic Alias Maximum twice: ignored, but validated for form.
            ServerPackets.BuildConnect(flags: 0x02, properties: Hex.Parse("22 00 0A 22 00 0A"), payload: ServerPackets.Field("c1"u8)),
            ServerPackets.ConnAckRefused(MqttReasonCode.MalformedPacket)
        },
        {
            // Request Response Information twice.
            ServerPackets.BuildConnect(flags: 0x02, properties: Hex.Parse("19 01 19 01"), payload: ServerPackets.Field("c1"u8)),
            ServerPackets.ConnAckRefused(MqttReasonCode.MalformedPacket)
        },
        {
            ServerPackets.BuildConnect(flags: 0x02, properties: [], payload: ServerPackets.Field([0xC3, 0x28])),
            ServerPackets.ConnAckRefused(MqttReasonCode.MalformedPacket)
        },
        { Hex.Parse("10 81 80 01"), ServerPackets.ConnAckRefused(MqttReasonCode.PacketTooLarge) },
        {
            // A client id with a line feed.
            ServerPackets.BuildConnect(flags: 0x02, properties: [], payload: ServerPackets.Field([0x61, 0x0A, 0x62])),
            ServerPackets.ConnAckRefused(MqttReasonCode.ClientIdentifierNotValid)
        },
        {
            // With a carriage return, as its last character.
            ServerPackets.BuildConnect(flags: 0x02, properties: [], payload: ServerPackets.Field([0x61, 0x0D])),
            ServerPackets.ConnAckRefused(MqttReasonCode.ClientIdentifierNotValid)
        },
        {
            // With U+0001, the lowest control character the codec lets through.
            ServerPackets.BuildConnect(flags: 0x02, properties: [], payload: ServerPackets.Field([0x01])),
            ServerPackets.ConnAckRefused(MqttReasonCode.ClientIdentifierNotValid)
        },
        {
            // With U+001F and with U+007F.
            ServerPackets.BuildConnect(flags: 0x02, properties: [], payload: ServerPackets.Field([0x61, 0x1F])),
            ServerPackets.ConnAckRefused(MqttReasonCode.ClientIdentifierNotValid)
        },
        {
            ServerPackets.BuildConnect(flags: 0x02, properties: [], payload: ServerPackets.Field([0x61, 0x7F, 0x62])),
            ServerPackets.ConnAckRefused(MqttReasonCode.ClientIdentifierNotValid)
        },
        {
            // With U+0080 and with U+009F, the ends of the second block of control characters; U+0085 is the
            // next-line character.
            ServerPackets.BuildConnect(flags: 0x02, properties: [], payload: ServerPackets.Field([0x61, 0xC2, 0x80])),
            ServerPackets.ConnAckRefused(MqttReasonCode.ClientIdentifierNotValid)
        },
        {
            ServerPackets.BuildConnect(flags: 0x02, properties: [], payload: ServerPackets.Field([0xC2, 0x9F, 0x61])),
            ServerPackets.ConnAckRefused(MqttReasonCode.ClientIdentifierNotValid)
        },
        {
            // With the line separator U+2028 and with the paragraph separator U+2029.
            ServerPackets.BuildConnect(flags: 0x02, properties: [], payload: ServerPackets.Field([0xE2, 0x80, 0xA8])),
            ServerPackets.ConnAckRefused(MqttReasonCode.ClientIdentifierNotValid)
        },
        {
            ServerPackets.BuildConnect(flags: 0x02, properties: [], payload: ServerPackets.Field([0x61, 0xE2, 0x80, 0xA9])),
            ServerPackets.ConnAckRefused(MqttReasonCode.ClientIdentifierNotValid)
        },
    };

    /// <summary>
    /// Gets client ids that hold no control character and no line separator, among them the characters next to
    /// each one that is refused.
    /// </summary>
    public static TheoryData<string> PrintableClientIds { get; } =
    [
        "a b~",
        "caf\u00E9",
        "a\u00A0b",
        "a\u2027b",
        "a\u202Ab",
        "\u00C2\u00E2",
        "\U0001F600",
    ];

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
    /// Proves that a client id is refused only for a control character or a line separator: one that holds a space,
    /// a tilde, a letter outside ASCII, the characters just beside the refused ones or an emoji is accepted and
    /// is the id the server knows the connection by.
    /// </summary>
    /// <param name="clientId">
    /// The client id.
    /// </param>
    [Theory]
    [MemberData(nameof(PrintableClientIds))]
    public void Connect_ClientIdWithoutControlCharacters_IsAccepted(string clientId)
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();

        // Act
        harness.Connect(clientId);

        // Assert
        harness.Server.TryGetConnection(clientId, out MqttWireConnection? connection).ShouldBeTrue();
        connection.ShouldNotBeNull().ClientId.ShouldBe(clientId);
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
    /// Proves that a User Property of the CONNECT reaches the host in the request, as a copy that stays readable
    /// after the read buffer is back in the pool and has been overwritten.
    /// </summary>
    [Fact]
    public void Connect_UserProperty_ReachesTheHostAsACopy()
    {
        // Arrange
        OverwritingPool pool = new();
        ServerHarness harness = ServerHarness.Create(pool: pool);
        MqttConnectRequest? seen = null;
        harness.Handler.Connect = (request, _) =>
        {
            seen = request;
            return ValueTask.FromResult(MqttConnectResult.Accept());
        };
        TestConnection connection = harness.Accept();

        // Act: the property "k" = "v".
        connection.Deliver(ServerPackets.BuildConnect(flags: 0x02, properties: Hex.Parse("26 00 01 6B 00 01 76"),
            payload: ServerPackets.Field("c1"u8)));

        // Assert: every buffer is back, and the pool has filled each with its filler.
        pool.Outstanding.ShouldBe(0);
        seen.ShouldNotBeNull();
        MqttWireUserProperty property = seen.UserProperties.ShouldHaveSingleItem();
        property.Name.ShouldBe("k");
        property.Value.ShouldBe("v");
        connection.TakeWritten().ShouldBe(ServerPackets.ConnAck);
    }

    /// <summary>
    /// Proves that Topic Alias Maximum and Request Response Information in a CONNECT are ignored: validated for form,
    /// then without effect, so the CONNECT is accepted with the usual CONNACK.
    /// </summary>
    [Fact]
    public void Connect_TopicAliasMaximumAndRequestResponseInformation_AreIgnored()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Accept();

        // Act: Topic Alias Maximum 10 and Request Response Information 1.
        connection.Deliver(ServerPackets.BuildConnect(flags: 0x02, properties: Hex.Parse("22 00 0A 19 01"),
            payload: ServerPackets.Field("c1"u8)));

        // Assert
        connection.TakeWritten().ShouldBe(ServerPackets.ConnAck);
        harness.Handler.Calls.ShouldBe(["OnConnectAsync c1", "OnAdmit c1 replaces=-"]);
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
    /// Proves that a CONNECT MqttWire refuses itself, from a client that announced a Maximum Packet Size too small
    /// for even the bare five-byte CONNACK, gets no MQTT packet at all (MQTT-3.1.2-24): the WebSocket is closed with
    /// 1002, the host is not asked, and the log line carries the reason code the CONNACK would have carried.
    /// </summary>
    /// <param name="limit">
    /// The Maximum Packet Size the CONNECT announces, from 1 to 4.
    /// </param>
    /// <param name="will">
    /// Whether the CONNECT also carries a Will, which is refused first, with 0x87.
    /// </param>
    /// <param name="expected">
    /// The reason code of the refusal, which only the log carries.
    /// </param>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Theory]
    [InlineData(1, false, MqttReasonCode.PacketTooLarge)]
    [InlineData(3, false, MqttReasonCode.PacketTooLarge)]
    [InlineData(4, false, MqttReasonCode.PacketTooLarge)]
    [InlineData(4, true, MqttReasonCode.NotAuthorized)]
    public async Task Connect_RefusedWithAMaximumPacketSizeBelowTheBareConnAck_WritesNoPacketAndClosesWith1002(
        int limit, bool will, MqttReasonCode expected)
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Accept();

        // Act
        connection.Deliver(ConnectWithMaximumPacketSize(limit, will));

        // Assert
        connection.TakeWritten().ShouldBeEmpty();
        connection.Link.ClosedWith.ShouldBe(WebSocketCloseStatus.ProtocolError);
        connection.Link.IsAborted.ShouldBeFalse();
        await connection.RunEnded();
        harness.Handler.Calls.ShouldBeEmpty();
        harness.Pool.Outstanding.ShouldBe(0);
        (LogLevel Level, EventId Id, string Message, Exception? Exception) entry = harness.Log.Entries.ShouldHaveSingleItem();
        entry.Id.Id.ShouldBe(ProtocolErrorEvent);
        entry.Message.ShouldContain(expected.ToString());
    }

    /// <summary>
    /// Proves that a client that announced a Maximum Packet Size of exactly the bare refusing CONNACK's five bytes
    /// is answered with that CONNACK, 0x95, and closed with 1000, without the host being asked.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Connect_RefusedWithAMaximumPacketSizeOfFive_AnswersTheBareConnAck()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Accept();

        // Act
        connection.Deliver(ConnectWithMaximumPacketSize(limit: 5, will: false));

        // Assert
        connection.TakeWritten().ShouldBe(Hex.Parse("20 03 00 95 00"));
        connection.Link.ClosedWith.ShouldBe(WebSocketCloseStatus.NormalClosure);
        await connection.RunEnded();
        harness.Handler.Calls.ShouldBeEmpty();
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that the CONNACK that refuses a Will leaves its reason string out when the packet with it, 40 bytes,
    /// would pass the Maximum Packet Size the client announced, and still carries 0x87; the host is not asked.
    /// </summary>
    /// <param name="limit">
    /// The Maximum Packet Size the CONNECT announces: at least the bare CONNACK's 5 bytes, below the 40 of the one
    /// with its reason string.
    /// </param>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Theory]
    [InlineData(5)]
    [InlineData(39)]
    public async Task Connect_WillRefusedWithAMaximumPacketSizeBelowTheReasonString_AnswersWithoutTheReasonString(
        int limit)
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Accept();

        // Act
        connection.Deliver(ConnectWithMaximumPacketSize(limit, will: true));

        // Assert
        connection.TakeWritten().ShouldBe(ServerPackets.ConnAckRefused(MqttReasonCode.NotAuthorized));
        connection.Link.ClosedWith.ShouldBe(WebSocketCloseStatus.NormalClosure);
        await connection.RunEnded();
        harness.Handler.Calls.ShouldBeEmpty();
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that the CONNACK that refuses a Will keeps its reason string when the whole packet, 40 bytes, fits the
    /// Maximum Packet Size the client announced; the host is not asked.
    /// </summary>
    /// <param name="limit">
    /// The Maximum Packet Size the CONNECT announces, at least 40.
    /// </param>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Theory]
    [InlineData(40)]
    [InlineData(4_096)]
    [InlineData(268_435_460)]
    public async Task Connect_WillRefusedWithAMaximumPacketSizeThatFitsTheReasonString_CarriesTheReasonString(
        int limit)
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Accept();

        // Act
        connection.Deliver(ConnectWithMaximumPacketSize(limit, will: true));

        // Assert
        connection.TakeWritten().ShouldBe(ServerPackets.ConnAckWillRefused);
        ServerPackets.ConnAckWillRefused.Length.ShouldBe(40);
        connection.Link.ClosedWith.ShouldBe(WebSocketCloseStatus.NormalClosure);
        await connection.RunEnded();
        harness.Handler.Calls.ShouldBeEmpty();
        harness.Pool.Outstanding.ShouldBe(0);
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
    /// Proves that at the authentication deadline the host's token is cancelled on a thread-pool thread, not inside
    /// the sweep: a callback the host registered on the token does not run on the sweep's thread while the sweep
    /// runs.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Connect_AuthenticationDeadline_CancelsTheHostsTokenOffTheSweepsThread()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TaskCompletionSource<MqttConnectResult> never = new();
        TaskCompletionSource<bool> ranInsideTheSweep = new(TaskCreationOptions.RunContinuationsAsynchronously);
        harness.Handler.Connect = (_, cancellationToken) =>
        {
            cancellationToken.Register(() => ranInsideTheSweep.TrySetResult(t_insideTheSweep));
            return new ValueTask<MqttConnectResult>(never.Task);
        };
        TestConnection connection = harness.Accept();
        connection.Deliver(ServerPackets.Connect);

        // Act: the fake clock runs the sweep on this thread, inside Advance.
        t_insideTheSweep = true;
        try
        {
            harness.Advance(ServerLimits.AuthenticationDeadline);
        }
        finally
        {
            t_insideTheSweep = false;
        }

        Task<bool> callback = ranInsideTheSweep.Task;
        await callback.Within("The host's token callback");
        await connection.RunEnded();

        // Assert
        (await callback).ShouldBeFalse();
        connection.TakeWritten().ShouldBe(ServerPackets.ConnAckRefused(MqttReasonCode.ServerUnavailable));
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

    /// <summary>
    /// Builds a CONNECT with client id "c1" that announces a Maximum Packet Size, with or without a Will on topic
    /// "t" with an empty payload.
    /// </summary>
    /// <param name="limit">
    /// The Maximum Packet Size to announce, at least 1.
    /// </param>
    /// <param name="will">
    /// Whether the CONNECT carries a Will.
    /// </param>
    /// <returns>
    /// The packet's bytes, fixed header included.
    /// </returns>
    private static byte[] ConnectWithMaximumPacketSize(int limit, bool will)
    {
        byte[] properties = [0x27, (byte)(limit >> 24), (byte)(limit >> 16), (byte)(limit >> 8), (byte)limit];
        byte[] clientId = ServerPackets.Field("c1"u8);
        return will
            ? ServerPackets.BuildConnect(flags: 0x06, properties,
                payload: [.. clientId, 0x00, .. ServerPackets.Field("t"u8), .. ServerPackets.Field([])])
            : ServerPackets.BuildConnect(flags: 0x02, properties, payload: clientId);
    }
}
