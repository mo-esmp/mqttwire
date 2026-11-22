using System.Text;
using Microsoft.Extensions.Logging;
using MqttWire.Server;
using MqttWire.Tests.Client;
using MqttWire.Tests.Codec;

namespace MqttWire.Tests.Server;

/// <summary>
/// Tests of what each packet from a client does once the connection is Active (design 6.6, "What each packet does",
/// and 6.7): SUBSCRIBE, UNSUBSCRIBE, PINGREQ, DISCONNECT, the packets a client may not send, the checks on PUBLISH, a
/// handler that throws, and the reason strings the answers carry.
/// </summary>
public sealed class PacketTests
{
    /// <summary>
    /// The event id of the log entry for a handler that threw.
    /// </summary>
    private const int HandlerThrewEvent = 15;

    /// <summary>
    /// Proves that a SUBSCRIBE for one filter calls the host once with the filter and the QoS asked for, and is
    /// answered with one SUBACK at the QoS granted.
    /// </summary>
    [Fact]
    public void Subscribe_OneFilterGranted_AnswersSubAckAtTheGrantedQoS()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();

        // Act
        connection.Deliver(ServerPackets.Subscribe);

        // Assert
        connection.TakeWritten().ShouldBe(ServerPackets.SubAckGrantedQoS1);
        SubscribeCalls(harness).ShouldBe(["OnSubscribe c1 a/b q1"]);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a SUBSCRIBE for three filters calls the host once per filter, in order, and that the SUBACK carries
    /// one code per filter in the same order.
    /// </summary>
    [Fact]
    public void Subscribe_ThreeFilters_CallsTheHostOncePerFilterInOrder()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        harness.Handler.Subscribe = static (_, filter, requested) =>
            filter == "b" ? MqttSubscribeResult.NotAuthorized : MqttSubscribeResult.Granted(requested);
        TestConnection connection = harness.Connect();
        byte[] subscribe = ServerPackets.BuildSubscribe(packetIdentifier: 1, properties: [],
            [("a", (byte)0), ("b", (byte)1), ("c", (byte)2)]);

        // Act
        connection.Deliver(subscribe);

        // Assert
        connection.TakeWritten().ShouldBe(Hex.Parse("90 06 00 01 00 00 87 02"));
        SubscribeCalls(harness).ShouldBe(["OnSubscribe c1 a q0", "OnSubscribe c1 b q1", "OnSubscribe c1 c q2"]);
    }

    /// <summary>
    /// Proves that a QoS the host grants above <see cref="MqttWireServerOptions.MaxQoS"/> is lowered to it in the
    /// SUBACK.
    /// </summary>
    [Fact]
    public void Subscribe_GrantedAboveMaxQoS_IsLowered()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create(static options => options.MaxQoS = MqttQoS.AtLeastOnce);
        harness.Handler.Subscribe = static (_, _, _) => MqttSubscribeResult.Granted(MqttQoS.ExactlyOnce);
        TestConnection connection = harness.Connect(connect: ServerPackets.Connect);

        // Act
        connection.Deliver(ServerPackets.Subscribe);

        // Assert
        connection.TakeWritten().ShouldBe(ServerPackets.SubAckGrantedQoS1);
    }

    /// <summary>
    /// Proves that the SUBACK carries the reason string of the first refusal, and not that of a later one.
    /// </summary>
    [Fact]
    public void Subscribe_RefusedWithAReasonString_CarriesTheFirstRefusalsString()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        harness.Handler.Subscribe = static (_, filter, requested) => filter switch
        {
            "b" => MqttSubscribeResult.Refuse(MqttReasonCode.NotAuthorized, "no"),
            "c" => MqttSubscribeResult.Refuse(MqttReasonCode.UnspecifiedError, "other"),
            _ => MqttSubscribeResult.Granted(requested),
        };
        TestConnection connection = harness.Connect();
        byte[] subscribe = ServerPackets.BuildSubscribe(packetIdentifier: 1, properties: [],
            [("a", (byte)0), ("b", (byte)0), ("c", (byte)0)]);

        // Act
        connection.Deliver(subscribe);

        // Assert
        connection.TakeWritten().ShouldBe(Hex.Parse("90 0B 00 01 05 1F 00 02 6E 6F 00 87 80"));
    }

    /// <summary>
    /// Proves that an invalid topic filter is answered with 0x8F without calling the host, and that the filters
    /// around it are handled as usual.
    /// </summary>
    [Fact]
    public void Subscribe_InvalidFilter_Answers0x8FWithoutCallingTheHost()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        byte[] subscribe = ServerPackets.BuildSubscribe(packetIdentifier: 1, properties: [],
            [("a#", (byte)1), ("a/b", (byte)1)]);

        // Act
        connection.Deliver(subscribe);

        // Assert
        connection.TakeWritten().ShouldBe(Hex.Parse("90 05 00 01 00 8F 01"));
        SubscribeCalls(harness).ShouldBe(["OnSubscribe c1 a/b q1"]);
        connection.Run.IsCompleted.ShouldBeFalse();
    }

    /// <summary>
    /// Proves that a shared subscription's filter is passed to the host as the client sent it.
    /// </summary>
    [Fact]
    public void Subscribe_SharedFilter_IsPassedToTheHost()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        byte[] subscribe = ServerPackets.BuildSubscribe(packetIdentifier: 1, properties: [], [("$share/g/a", (byte)1)]);

        // Act
        connection.Deliver(subscribe);

        // Assert
        connection.TakeWritten().ShouldBe(ServerPackets.SubAckGrantedQoS1);
        SubscribeCalls(harness).ShouldBe(["OnSubscribe c1 $share/g/a q1"]);
    }

    /// <summary>
    /// Proves that a SUBSCRIBE with a subscription identifier closes the connection with DISCONNECT 0xA1 and
    /// <see cref="MqttCloseReason.ProtocolError"/>, without calling the host.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Subscribe_WithSubscriptionIdentifier_ClosesWith0xA1()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        byte[] subscribe = ServerPackets.BuildSubscribe(packetIdentifier: 1, properties: [0x0B, 0x01], [("a/b", (byte)1)]);

        // Act
        connection.Deliver(subscribe);
        await connection.Run;

        // Assert
        connection.TakeWritten().ShouldBe(ServerPackets.Disconnect(MqttReasonCode.SubscriptionIdentifiersNotSupported));
        SubscribeCalls(harness).ShouldBeEmpty();
        harness.Handler.Calls.ShouldContain("OnClosed c1 ProtocolError");
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that the subscription options no-local, retain-as-published and retain handling have no effect: the host
    /// is called and the client answered as for a plain subscription.
    /// </summary>
    [Fact]
    public void Subscribe_Options_AreIgnored()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();

        // QoS 1, no-local, retain-as-published and retain handling 2: 0b0010_1101.
        byte[] subscribe = ServerPackets.BuildSubscribe(packetIdentifier: 1, properties: [], [("a/b", (byte)0x2D)]);

        // Act
        connection.Deliver(subscribe);

        // Assert
        connection.TakeWritten().ShouldBe(ServerPackets.SubAckGrantedQoS1);
        SubscribeCalls(harness).ShouldBe(["OnSubscribe c1 a/b q1"]);
    }

    /// <summary>
    /// Proves that a SUBSCRIBE of 300 filters, more than the codes kept on the stack, calls the host 300 times and is
    /// answered with 300 codes, and that the buffer that held them is back in the pool.
    /// </summary>
    [Fact]
    public void Subscribe_ThreeHundredFilters_AnswersThreeHundredCodes()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        byte[] subscribe = ServerPackets.BuildSubscribe(packetIdentifier: 1, properties: [],
            Enumerable.Range(0, 300).Select(static index => ($"f{index}", (byte)0)));

        // Act
        connection.Deliver(subscribe);

        // Assert
        // Remaining Length 303: the identifier, the property length and 300 codes of 0x00.
        connection.TakeWritten().ShouldBe([.. Hex.Parse("90 AF 02 00 01 00"), .. new byte[300]]);
        SubscribeCalls(harness).Count.ShouldBe(300);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a SUBSCRIBE whose SUBACK would pass the client's Maximum Packet Size even without a reason string
    /// closes the connection with DISCONNECT 0x95 and <see cref="MqttCloseReason.ProtocolError"/>, without calling the
    /// host.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Subscribe_SubAckWouldPassTheClientsMaximumPacketSize_ClosesWith0x95()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect(connect: ServerPackets.ConnectMaxPacketSize4096);

        // The SUBACK would be 4,098 bytes: a fixed header of 3, the identifier, the property length and 4,092 codes.
        byte[] subscribe = ServerPackets.BuildSubscribe(packetIdentifier: 1, properties: [],
            Enumerable.Repeat(("a", (byte)0), 4_092));

        // Act
        connection.Deliver(subscribe);
        await connection.Run;

        // Assert
        connection.TakeWritten().ShouldBe(Hex.Parse("E0 01 95"));
        SubscribeCalls(harness).ShouldBeEmpty();
        harness.Handler.Calls.ShouldContain("OnClosed c1 ProtocolError");
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that an UNSUBSCRIBE whose UNSUBACK would pass the client's Maximum Packet Size closes the connection with
    /// DISCONNECT 0x95, without calling the host.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Unsubscribe_UnsubAckWouldPassTheClientsMaximumPacketSize_ClosesWith0x95()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect(connect: ServerPackets.ConnectMaxPacketSize4096);
        byte[] unsubscribe = ServerPackets.BuildUnsubscribe(packetIdentifier: 2, Enumerable.Repeat("a", 4_092));

        // Act
        connection.Deliver(unsubscribe);
        await connection.Run;

        // Assert
        connection.TakeWritten().ShouldBe(Hex.Parse("E0 01 95"));
        harness.Handler.Calls.ShouldNotContain(static call => call.StartsWith("OnUnsubscribe", StringComparison.Ordinal));
        harness.Handler.Calls.ShouldContain("OnClosed c1 ProtocolError");
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that an UNSUBSCRIBE of two filters calls the host twice, in order, and is answered with success for each.
    /// </summary>
    [Fact]
    public void Unsubscribe_TwoFilters_CallsTheHostTwiceAndAnswersSuccess()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        byte[] unsubscribe = ServerPackets.BuildUnsubscribe(packetIdentifier: 2, ["a/b", "c"]);

        // Act
        connection.Deliver(unsubscribe);

        // Assert
        connection.TakeWritten().ShouldBe(Hex.Parse("B0 05 00 02 00 00 00"));
        harness.Handler.Calls.Where(static call => call.StartsWith("OnUnsubscribe", StringComparison.Ordinal))
            .ShouldBe(["OnUnsubscribe c1 a/b", "OnUnsubscribe c1 c"]);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a host that closes the connection while it answers the first filter of a SUBSCRIBE is not asked
    /// about the filters that are left, and that the DISCONNECT of its close is the last packet: no SUBACK follows.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Subscribe_HostClosesTheConnectionAtTheFirstFilter_StopsCallingTheHostAndWritesNoSubAck()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        harness.Handler.Subscribe = static (subscriber, _, requested) =>
        {
            subscriber.Close(MqttReasonCode.AdministrativeAction);
            return MqttSubscribeResult.Granted(requested);
        };
        TestConnection connection = harness.Connect();
        byte[] subscribe = ServerPackets.BuildSubscribe(packetIdentifier: 1, properties: [],
            [("a", (byte)0), ("b", (byte)0), ("c", (byte)0)]);

        // Act
        connection.Deliver(subscribe);
        await connection.Run;

        // Assert
        SubscribeCalls(harness).ShouldBe(["OnSubscribe c1 a q0"]);
        connection.TakeWritten().ShouldBe(ServerPackets.Disconnect(MqttReasonCode.AdministrativeAction));
        harness.Handler.Calls.ShouldContain("OnClosed c1 ClosedByHost");
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a host that closes the connection while it handles the first filter of an UNSUBSCRIBE is not
    /// called for the filters that are left, and that the DISCONNECT of its close is the last packet: no UNSUBACK
    /// follows.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Unsubscribe_HostClosesTheConnectionAtTheFirstFilter_StopsCallingTheHostAndWritesNoUnsubAck()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        harness.Handler.Unsubscribe = static (subscriber, _) => subscriber.Close(MqttReasonCode.AdministrativeAction);
        TestConnection connection = harness.Connect();
        byte[] unsubscribe = ServerPackets.BuildUnsubscribe(packetIdentifier: 2, ["a", "b", "c"]);

        // Act
        connection.Deliver(unsubscribe);
        await connection.Run;

        // Assert
        harness.Handler.Calls.Where(static call => call.StartsWith("OnUnsubscribe", StringComparison.Ordinal))
            .ShouldBe(["OnUnsubscribe c1 a"]);
        connection.TakeWritten().ShouldBe(ServerPackets.Disconnect(MqttReasonCode.AdministrativeAction));
        harness.Handler.Calls.ShouldContain("OnClosed c1 ClosedByHost");
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a PINGREQ is answered with a PINGRESP.
    /// </summary>
    [Fact]
    public void PingReq_Always_AnswersPingResp()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();

        // Act
        connection.Deliver(ClientPackets.PingReq);

        // Assert
        connection.TakeWritten().ShouldBe(ClientPackets.PingResp);
        connection.Run.IsCompleted.ShouldBeFalse();
    }

    /// <summary>
    /// Proves that a PINGREQ with a Remaining Length other than 0 is malformed: DISCONNECT 0x81 and
    /// <see cref="MqttCloseReason.MalformedPacket"/>.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task PingReq_WithARemainingLength_ClosesWith0x81()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();

        // Act
        connection.Deliver(Hex.Parse("C0 01 00"));
        await connection.Run;

        // Assert
        connection.TakeWritten().ShouldBe(ServerPackets.Disconnect(MqttReasonCode.MalformedPacket));
        harness.Handler.Calls.ShouldContain("OnClosed c1 MalformedPacket");
    }

    /// <summary>
    /// Proves that a DISCONNECT with a reason code and properties ends the connection with
    /// <see cref="MqttCloseReason.ClientDisconnected"/> and that nothing is written back.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Disconnect_WithAReasonAndProperties_EndsWithClientDisconnected()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();

        // Reason 0x04, the reason string "bye" and the user property k=v.
        byte[] disconnect = Hex.Parse("E0 0F 04 0D 1F 00 03 62 79 65 26 00 01 6B 00 01 76");

        // Act
        connection.Deliver(disconnect);
        await connection.Run;

        // Assert
        connection.TakeWritten().ShouldBeEmpty();
        harness.Handler.Calls.ShouldContain("OnClosed c1 ClientDisconnected");
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a malformed DISCONNECT closes the connection with DISCONNECT 0x81, not as a client's own end.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Disconnect_Malformed_ClosesWith0x81()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();

        // A property length of 5 with no properties behind it.
        byte[] disconnect = Hex.Parse("E0 02 00 05");

        // Act
        connection.Deliver(disconnect);
        await connection.Run;

        // Assert
        connection.TakeWritten().ShouldBe(ServerPackets.Disconnect(MqttReasonCode.MalformedPacket));
        harness.Handler.Calls.ShouldContain("OnClosed c1 MalformedPacket");
    }

    /// <summary>
    /// Proves that a second CONNECT closes the connection with DISCONNECT 0x82 and
    /// <see cref="MqttCloseReason.ProtocolError"/>, without asking the host again.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Connect_Second_ClosesWith0x82()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();

        // Act
        connection.Deliver(ServerPackets.Connect);
        await connection.Run;

        // Assert
        connection.TakeWritten().ShouldBe(Hex.Parse("E0 01 82"));
        harness.Handler.Calls.Count(static call => call.StartsWith("OnConnectAsync", StringComparison.Ordinal)).ShouldBe(1);
        harness.Handler.Calls.ShouldContain("OnClosed c1 ProtocolError");
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that an AUTH packet closes the connection with DISCONNECT 0x82 and
    /// <see cref="MqttCloseReason.ProtocolError"/>.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Auth_Always_ClosesWith0x82()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();

        // Act
        connection.Deliver(ServerPackets.Auth);
        await connection.Run;

        // Assert
        connection.TakeWritten().ShouldBe(ServerPackets.Disconnect(MqttReasonCode.ProtocolError));
        harness.Handler.Calls.ShouldContain("OnClosed c1 ProtocolError");
    }

    /// <summary>
    /// Proves that a CONNACK from a client closes the connection with DISCONNECT 0x82 and
    /// <see cref="MqttCloseReason.ProtocolError"/>.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task ConnAck_FromAClient_ClosesWith0x82()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();

        // Act
        connection.Deliver(ClientPackets.ConnAckAccepted);
        await connection.Run;

        // Assert
        connection.TakeWritten().ShouldBe(ServerPackets.Disconnect(MqttReasonCode.ProtocolError));
        harness.Handler.Calls.ShouldContain("OnClosed c1 ProtocolError");
    }

    /// <summary>
    /// Proves that every other packet type the server never reads (SUBACK, UNSUBACK, PINGRESP and the reserved type
    /// 0) closes the connection with DISCONNECT 0x82 and <see cref="MqttCloseReason.ProtocolError"/>.
    /// </summary>
    /// <param name="packet">
    /// The packet, in hex.
    /// </param>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Theory]
    [InlineData("90 04 00 01 00 01")]
    [InlineData("B0 04 00 02 00 00")]
    [InlineData("D0 00")]
    [InlineData("00 00")]
    public async Task ServerPacket_FromAClient_ClosesWith0x82(string packet)
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();

        // Act
        connection.Deliver(Hex.Parse(packet));
        await connection.Run;

        // Assert
        connection.TakeWritten().ShouldBe(ServerPackets.Disconnect(MqttReasonCode.ProtocolError));
        harness.Handler.Calls.ShouldContain("OnClosed c1 ProtocolError");
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a PUBLISH whose topic is not UTF-8 closes the connection with DISCONNECT 0x81 and
    /// <see cref="MqttCloseReason.MalformedPacket"/>.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Publish_TopicNotUtf8_ClosesWith0x81()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();

        // Act
        connection.Deliver(ClientPackets.PublishTopicNotUtf8);
        await connection.Run;

        // Assert
        connection.TakeWritten().ShouldBe(Hex.Parse("E0 01 81"));
        harness.Handler.Calls.ShouldContain("OnClosed c1 MalformedPacket");
        PublishCalls(harness).ShouldBeEmpty();
    }

    /// <summary>
    /// Proves that a PUBLISH with a topic alias closes the connection with DISCONNECT 0x94 and
    /// <see cref="MqttCloseReason.ProtocolError"/>, without calling the host.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Publish_WithTopicAlias_ClosesWith0x94()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();

        // Act
        connection.Deliver(ClientPackets.PublishWithTopicAlias);
        await connection.Run;

        // Assert
        connection.TakeWritten().ShouldBe(ClientPackets.DisconnectTopicAliasInvalid);
        harness.Handler.Calls.ShouldContain("OnClosed c1 ProtocolError");
        PublishCalls(harness).ShouldBeEmpty();
    }

    /// <summary>
    /// Proves that a PUBLISH with a subscription identifier closes the connection with DISCONNECT 0x82 and
    /// <see cref="MqttCloseReason.ProtocolError"/>.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Publish_WithSubscriptionIdentifier_ClosesWith0x82()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        byte[] publish = ServerPackets.BuildPublish(MqttQoS.AtMostOnce, "a/b"u8, packetIdentifier: 0,
            properties: [0x0B, 0x01], payload: "hi"u8);

        // Act
        connection.Deliver(publish);
        await connection.Run;

        // Assert
        connection.TakeWritten().ShouldBe(ServerPackets.Disconnect(MqttReasonCode.ProtocolError));
        harness.Handler.Calls.ShouldContain("OnClosed c1 ProtocolError");
        PublishCalls(harness).ShouldBeEmpty();
    }

    /// <summary>
    /// Proves that a PUBLISH whose topic holds a wildcard closes the connection with DISCONNECT 0x90 and
    /// <see cref="MqttCloseReason.ProtocolError"/>.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Publish_WildcardInTheTopic_ClosesWith0x90()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        byte[] publish = ServerPackets.BuildPublish(MqttQoS.AtMostOnce, "a/+"u8, packetIdentifier: 0, properties: [],
            payload: "hi"u8);

        // Act
        connection.Deliver(publish);
        await connection.Run;

        // Assert
        connection.TakeWritten().ShouldBe(ServerPackets.Disconnect(MqttReasonCode.TopicNameInvalid));
        harness.Handler.Calls.ShouldContain("OnClosed c1 ProtocolError");
        PublishCalls(harness).ShouldBeEmpty();
    }

    /// <summary>
    /// Proves that a QoS 2 PUBLISH to a server whose <see cref="MqttWireServerOptions.MaxQoS"/> is 1 closes the
    /// connection with DISCONNECT 0x9B and <see cref="MqttCloseReason.ProtocolError"/>, without calling the host.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Publish_QoS2WithMaxQoS1_ClosesWith0x9B()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create(static options => options.MaxQoS = MqttQoS.AtLeastOnce);
        TestConnection connection = harness.Connect(connect: ServerPackets.Connect);

        // Act
        connection.Deliver(ClientPackets.PublishQoS2);
        await connection.Run;

        // Assert
        connection.TakeWritten().ShouldBe(ServerPackets.Disconnect(MqttReasonCode.QoSNotSupported));
        harness.Handler.Calls.ShouldContain("OnClosed c1 ProtocolError");
        PublishCalls(harness).ShouldBeEmpty();
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that the QoS is judged from the fixed header, so a QoS 2 PUBLISH in the skip band to a server whose
    /// <see cref="MqttWireServerOptions.MaxQoS"/> is 1 closes the connection with DISCONNECT 0x9B too, not with a
    /// refusal of its size.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Publish_QoS2WithMaxQoS1InTheSkipBand_ClosesWith0x9B()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create(static options =>
        {
            options.MaxQoS = MqttQoS.AtLeastOnce;
            options.MaxPublishPayloadBytes = 1_024;
        });
        TestConnection connection = harness.Connect(connect: ServerPackets.Connect);

        // Act
        connection.Deliver(ServerPackets.BuildPublishOfLength(MqttQoS.ExactlyOnce, 16_384));
        await connection.Run;

        // Assert
        connection.TakeWritten().ShouldBe(ServerPackets.Disconnect(MqttReasonCode.QoSNotSupported));
        harness.Handler.Calls.ShouldContain("OnClosed c1 ProtocolError");
        harness.Pool.LargestRent.ShouldBeLessThanOrEqualTo(4_096);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a QoS 0 PUBLISH reaches the host with its retain flag, and that nothing is written back.
    /// </summary>
    [Fact]
    public void Publish_QoS0_ReachesTheHostWithTheRetainFlag()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();

        // Act
        connection.Deliver(Hex.Parse("31 08 00 03 61 2F 62 00 68 69"));

        // Assert
        PublishCalls(harness).ShouldBe(["OnPublish c1 a/b hi q0 retain=1"]);
        connection.TakeWritten().ShouldBeEmpty();
    }

    /// <summary>
    /// Proves that a QoS 1 PUBLISH the host refuses is answered with a PUBACK that carries the host's code and reason
    /// string.
    /// </summary>
    [Fact]
    public void Publish_QoS1Refused_AnswersPubAckWithTheHostsCodeAndString()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        harness.Handler.Publish = static (_, in _) => MqttPublishResult.Refuse(MqttReasonCode.NotAuthorized, "no");
        TestConnection connection = harness.Connect();

        // Act
        connection.Deliver(ClientPackets.PublishQoS1);

        // Assert
        connection.TakeWritten().ShouldBe(Hex.Parse("40 09 00 01 87 05 1F 00 02 6E 6F"));
    }

    /// <summary>
    /// Proves that the payload the host is given is a view into the rented read buffer, not a copy.
    /// </summary>
    [Fact]
    public void Publish_Payload_IsAViewIntoTheReadBuffer()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        bool? isView = null;
        harness.Handler.Publish = (_, in publish) =>
        {
            isView = harness.Pool.IsViewIntoRentedBuffer(publish.Payload);
            return MqttPublishResult.Accepted;
        };
        TestConnection connection = harness.Connect();

        // Act
        connection.Deliver(ClientPackets.PublishQoS0);

        // Assert
        isView.ShouldBe(true);
    }

    /// <summary>
    /// Proves that the PUBLISH properties MqttWire ignores are validated and then have no effect: with content type,
    /// message expiry, response topic, correlation data and a user property the host sees the plain message, while a
    /// message expiry given twice is malformed (0x81).
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Publish_IgnoredProperties_AreValidatedAndWithoutEffect()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        byte[] ignored =
        [
            0x03, .. ServerPackets.Field("text/plain"u8), 0x02, 0x00, 0x00, 0x00, 0x3C,
            0x08, .. ServerPackets.Field("r/t"u8), 0x09, .. ServerPackets.Field([0x01, 0x02]),
            0x26, .. ServerPackets.Field("k"u8), .. ServerPackets.Field("v"u8),
        ];
        byte[] twice = [0x02, 0x00, 0x00, 0x00, 0x3C, 0x02, 0x00, 0x00, 0x00, 0x3C];

        // Act
        connection.Deliver(ServerPackets.BuildPublish(MqttQoS.AtLeastOnce, "a/b"u8, packetIdentifier: 1, ignored,
            payload: "hi"u8));
        byte[] accepted = connection.TakeWritten();
        connection.Deliver(ServerPackets.BuildPublish(MqttQoS.AtLeastOnce, "a/b"u8, packetIdentifier: 2, twice,
            payload: "hi"u8));
        await connection.Run;

        // Assert
        accepted.ShouldBe(ClientPackets.PubAck1);
        PublishCalls(harness).ShouldBe(["OnPublish c1 a/b hi q1 retain=0"]);
        connection.TakeWritten().ShouldBe(ServerPackets.Disconnect(MqttReasonCode.MalformedPacket));
    }

    /// <summary>
    /// Proves that a handler that throws closes the connection with DISCONNECT 0x83 and
    /// <see cref="MqttCloseReason.HandlerThrew"/>, logs an error that names the method, answers nothing for the
    /// packet, and leaves every buffer back in the pool.
    /// </summary>
    /// <param name="method">
    /// The handler method that throws.
    /// </param>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Theory]
    [InlineData(nameof(IMqttWireHandler.OnSubscribe))]
    [InlineData(nameof(IMqttWireHandler.OnUnsubscribe))]
    [InlineData(nameof(IMqttWireHandler.OnPublish))]
    public async Task Handler_Throws_ClosesWith0x83AndLogsAnError(string method)
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        InvalidOperationException thrown = new("The host's bug.");
        byte[] packet;
        switch (method)
        {
            case nameof(IMqttWireHandler.OnSubscribe):
                harness.Handler.Subscribe = (_, _, _) => throw thrown;
                packet = ServerPackets.Subscribe;
                break;
            case nameof(IMqttWireHandler.OnUnsubscribe):
                harness.Handler.Unsubscribe = (_, _) => throw thrown;
                packet = ServerPackets.Unsubscribe;
                break;
            default:
                harness.Handler.Publish = (_, in _) => throw thrown;
                packet = ClientPackets.PublishQoS1;
                break;
        }

        TestConnection connection = harness.Connect();

        // Act
        connection.Deliver(packet);
        await connection.Run;

        // Assert
        connection.TakeWritten().ShouldBe(Hex.Parse("E0 01 83"));
        harness.Handler.Calls.ShouldContain("OnClosed c1 HandlerThrew");
        harness.Log.Entries.ShouldContain(entry =>
            entry.Id.Id == HandlerThrewEvent && entry.Level == LogLevel.Error && entry.Exception == thrown
            && entry.Message.Contains(method, StringComparison.Ordinal));
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a client that sent Request Problem Information 0 gets PUBACK, SUBACK and UNSUBACK without a reason
    /// string, even after a refusal that has one.
    /// </summary>
    [Fact]
    public void Acks_RequestProblemInformationZero_CarryNoReasonString()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        harness.Handler.Publish = static (_, in _) => MqttPublishResult.Refuse(MqttReasonCode.NotAuthorized, "no");
        harness.Handler.Subscribe = static (_, _, _) => MqttSubscribeResult.Refuse(MqttReasonCode.NotAuthorized, "no");
        TestConnection connection = harness.Connect(connect: ServerPackets.ConnectNoProblemInfo);

        // Act
        connection.Deliver([.. ClientPackets.PublishQoS1, .. ServerPackets.Subscribe, .. ServerPackets.Unsubscribe]);

        // Assert
        connection.TakeWritten().ShouldBe(
            [.. Hex.Parse("40 03 00 01 87"), .. ServerPackets.SubAckNotAuthorized, .. ServerPackets.UnsubAck]);
    }

    /// <summary>
    /// Proves that a reason string over 256 bytes is cut to 256 bytes at a character boundary: 300 two-byte characters
    /// to 128, and 100 three-byte characters to 85 (255 bytes).
    /// </summary>
    [Fact]
    public void Acks_ReasonStringOf600Bytes_IsCutTo256BytesAtACharacterBoundary()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        int publishes = 0;
        harness.Handler.Publish = (_, in _) => MqttPublishResult.Refuse(MqttReasonCode.NotAuthorized,
            ++publishes == 1 ? new string('é', 300) : new string('€', 100));
        TestConnection connection = harness.Connect();

        // Act
        connection.Deliver(ClientPackets.PublishQoS1);
        byte[] twoByte = connection.TakeWritten();
        connection.Deliver(ClientPackets.PublishQoS1);
        byte[] threeByte = connection.TakeWritten();

        // Assert
        // Remaining Length 264 and property length 259: the identifier, the code, the reason string's 3 + 256 bytes.
        twoByte.ShouldBe([.. Hex.Parse("40 88 02 00 01 87 83 02 1F 01 00"), .. Encoding.UTF8.GetBytes(new string('é', 128))]);

        // Remaining Length 263 and property length 258, for 255 bytes of reason string.
        threeByte.ShouldBe([.. Hex.Parse("40 87 02 00 01 87 82 02 1F 00 FF"), .. Encoding.UTF8.GetBytes(new string('€', 85))]);
    }

    /// <summary>
    /// Proves that a reason string that would make an answer pass the client's Maximum Packet Size is left out, while
    /// the answer itself is sent.
    /// </summary>
    [Fact]
    public void Acks_ReasonStringWouldPassTheClientsMaximumPacketSize_IsLeftOut()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        harness.Handler.Subscribe = static (_, _, _) =>
            MqttSubscribeResult.Refuse(MqttReasonCode.NotAuthorized, new string('r', 256));
        TestConnection connection = harness.Connect(connect: ServerPackets.ConnectMaxPacketSize4096);

        // The SUBACK is 3,906 bytes without the reason string and 4,166 with it.
        byte[] subscribe = ServerPackets.BuildSubscribe(packetIdentifier: 1, properties: [],
            Enumerable.Repeat(("a", (byte)0), 3_900));

        // Act
        connection.Deliver(subscribe);

        // Assert
        byte[] expected = new byte[3_906];
        Hex.Parse("90 BF 1E 00 01 00").CopyTo(expected, 0);
        expected.AsSpan(6).Fill(0x87);
        connection.TakeWritten().ShouldBe(expected);
        connection.Run.IsCompleted.ShouldBeFalse();
    }

    /// <summary>
    /// Gets the calls of <c>OnSubscribe</c> the handler recorded, oldest first.
    /// </summary>
    /// <param name="harness">
    /// The harness.
    /// </param>
    /// <returns>
    /// The calls as text.
    /// </returns>
    private static List<string> SubscribeCalls(ServerHarness harness) =>
        [.. harness.Handler.Calls.Where(static call => call.StartsWith("OnSubscribe", StringComparison.Ordinal))];

    /// <summary>
    /// Gets the calls of <c>OnPublish</c> the handler recorded, oldest first.
    /// </summary>
    /// <param name="harness">
    /// The harness.
    /// </param>
    /// <returns>
    /// The calls as text.
    /// </returns>
    private static List<string> PublishCalls(ServerHarness harness) =>
        [.. harness.Handler.Calls.Where(static call => call.StartsWith("OnPublish", StringComparison.Ordinal))];
}
