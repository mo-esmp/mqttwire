using MqttWire.Server;
using MqttWire.Server.Internal;
using MqttWire.Tests.Client;
using MqttWire.Tests.Codec;

namespace MqttWire.Tests.Server;

/// <summary>
/// Tests of QoS 1 and 2 for what a client publishes (design 6.6, "QoS 1 and 2 inside a connection", plan decision
/// 13): the QoS 2 exchange PUBLISH, PUBREC, PUBREL, PUBCOMP, the count against the receive maximum of 256 the server
/// announced, the acknowledgement deadline, and the validation of the acknowledgements a client sends.
/// </summary>
public sealed class IncomingQoSTests
{
    /// <summary>
    /// Proves that a whole QoS 2 exchange calls the host once, answers PUBREC and PUBCOMP, and leaves no record and
    /// no records object behind it.
    /// </summary>
    [Fact]
    public void PublishQoS2_FullExchange_CallsTheHostOnceAndEndsWithNoRecord()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();

        // Act
        connection.Deliver(ClientPackets.PublishQoS2);
        byte[] pubRec = connection.TakeWritten();
        ConnectionCounts remembered = server.GetCounts();
        connection.Deliver(ClientPackets.PubRel1);
        byte[] pubComp = connection.TakeWritten();
        ConnectionCounts afterwards = server.GetCounts();

        // Assert
        pubRec.ShouldBe(ClientPackets.PubRec1);
        pubComp.ShouldBe(ClientPackets.PubComp1);
        remembered.IncomingRecords.ShouldBe(1);
        remembered.HoldsRecords.ShouldBeTrue();
        afterwards.IncomingRecords.ShouldBe(0);
        afterwards.IncomingInFlight.ShouldBe(0);
        afterwards.HoldsRecords.ShouldBeFalse();
        PublishCalls(harness).ShouldBe(["OnPublish c1 a/b hi q2 retain=0"]);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a QoS 2 publish the host refuses is answered with a PUBREC that carries the refusal and is not
    /// remembered: a PUBREL for its identifier is answered PUBCOMP 0x92.
    /// </summary>
    [Fact]
    public void PublishQoS2_Refused_AnswersAFailingPubRecAndRemembersNothing()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        harness.Handler.Publish = static (MqttWireConnection _, in MqttPublish _) =>
            MqttPublishResult.Refuse(MqttReasonCode.NotAuthorized);
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();

        // Act
        connection.Deliver(ClientPackets.PublishQoS2);
        byte[] pubRec = connection.TakeWritten();
        ConnectionCounts afterRefusal = server.GetCounts();
        connection.Deliver(ClientPackets.PubRel1);

        // Assert
        pubRec.ShouldBe(Hex.Parse("50 03 00 01 87"));
        afterRefusal.IncomingRecords.ShouldBe(0);
        afterRefusal.HoldsRecords.ShouldBeFalse();
        connection.TakeWritten().ShouldBe(Hex.Parse("70 03 00 01 92"));
        PublishCalls(harness).Count.ShouldBe(1);
    }

    /// <summary>
    /// Proves that a QoS 2 publish whose identifier is remembered, sent again with DUP 0, is not handed to the host
    /// again and is answered with PUBREC success again.
    /// </summary>
    [Fact]
    public void PublishQoS2_RepeatedIdentifierDup0_IsNotHandedOnAgain()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        connection.Deliver(ClientPackets.PublishQoS2Id7);

        // Act
        connection.Deliver(ClientPackets.PublishQoS2Id7);

        // Assert
        connection.TakeWritten().ShouldBe([.. ClientPackets.PubRec7, .. ClientPackets.PubRec7]);
        PublishCalls(harness).Count.ShouldBe(1);
        connection.Connection.ShouldNotBeNull().GetCounts().IncomingRecords.ShouldBe(1);
    }

    /// <summary>
    /// Proves that a QoS 2 publish whose identifier is remembered, sent again with DUP 1, is not handed to the host
    /// again and is answered with PUBREC success again.
    /// </summary>
    [Fact]
    public void PublishQoS2_RepeatedIdentifierDup1_IsNotHandedOnAgain()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        connection.Deliver(ClientPackets.PublishQoS2Id7);

        // Act
        connection.Deliver(ClientPackets.PublishQoS2Id7Dup);

        // Assert
        connection.TakeWritten().ShouldBe([.. ClientPackets.PubRec7, .. ClientPackets.PubRec7]);
        PublishCalls(harness).Count.ShouldBe(1);
        connection.Connection.ShouldNotBeNull().GetCounts().IncomingRecords.ShouldBe(1);
    }

    /// <summary>
    /// Proves that once PUBREL has ended an exchange its identifier is new again: the next QoS 2 publish with it is
    /// handed to the host.
    /// </summary>
    [Fact]
    public void PublishQoS2_AfterPubRel_TheIdentifierIsNewAgain()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        connection.Deliver(ClientPackets.PublishQoS2Id7);
        connection.Deliver(ClientPackets.PubRel7);

        // Act
        connection.Deliver(ClientPackets.PublishQoS2Id7);

        // Assert
        connection.TakeWritten().ShouldBe(
            [.. ClientPackets.PubRec7, .. ClientPackets.PubComp7, .. ClientPackets.PubRec7]);
        PublishCalls(harness).Count.ShouldBe(2);
    }

    /// <summary>
    /// Proves that a PUBREL for an identifier that is not remembered is answered PUBCOMP 0x92 (packet identifier
    /// not found), and the connection stays open.
    /// </summary>
    [Fact]
    public void PubRel_UnknownIdentifier_AnswersPubComp0x92()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();

        // Act
        connection.Deliver(ClientPackets.PubRel7);

        // Assert
        connection.TakeWritten().ShouldBe(ClientPackets.PubCompNotFound7);
        connection.Run.IsCompleted.ShouldBeFalse();
        connection.Connection.ShouldNotBeNull().GetCounts().HoldsRecords.ShouldBeFalse();
    }

    /// <summary>
    /// Proves that a QoS 2 exchange whose PUBREL never comes closes the connection with DISCONNECT 0x82 and
    /// <see cref="MqttCloseReason.AcknowledgementTimedOut"/> 30 seconds after the PUBREC, not before, while PINGREQs
    /// keep the keep-alive satisfied; the records object goes back with the end of the connection.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task PubRel_NeverComes_ClosesWith0x82AfterThirtySeconds()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        connection.Deliver(ClientPackets.PublishQoS2);
        connection.TakeWritten().ShouldBe(ClientPackets.PubRec1);

        // Act
        harness.Advance(TimeSpan.FromSeconds(20));
        connection.Deliver(ClientPackets.PingReq);
        harness.Advance(TimeSpan.FromSeconds(8));
        byte[] writtenAt28 = connection.TakeWritten();
        harness.Advance(TimeSpan.FromSeconds(2));
        byte[] writtenAt30 = connection.TakeWritten();
        connection.Link.EndFromPeer();
        await connection.Run;

        // Assert
        writtenAt28.ShouldBe(ClientPackets.PingResp);
        writtenAt30.ShouldBe(ServerPackets.Disconnect(MqttReasonCode.ProtocolError));
        harness.Handler.Calls.ShouldContain("OnClosed c1 AcknowledgementTimedOut");
        server.GetCounts().HoldsRecords.ShouldBeFalse();
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that 256 QoS 2 publishes waiting for their PUBREL, as many as the receive maximum the server
    /// announced, are all accepted and remembered.
    /// </summary>
    [Fact]
    public void PublishQoS2_256Unreleased_AreAllAccepted()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();

        // Act
        for (int identifier = 1; identifier <= 256; identifier++)
        {
            connection.Deliver(PublishQoS2((ushort)identifier));
            connection.TakeWritten().ShouldBe(PubRec((ushort)identifier));
        }

        // Assert
        ConnectionCounts counts = connection.Connection.ShouldNotBeNull().GetCounts();
        counts.IncomingRecords.ShouldBe(256);
        counts.IncomingInFlight.ShouldBe(256);
        PublishCalls(harness).Count.ShouldBe(256);
        connection.Run.IsCompleted.ShouldBeFalse();
    }

    /// <summary>
    /// Proves that with 256 QoS 2 publishes waiting for their PUBREL a repeated identifier takes no new place, and a
    /// 257th new one closes the connection with DISCONNECT 0x93 and <see cref="MqttCloseReason.ReceiveMaximumPassed"/>
    /// before the host sees it.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task PublishQoS2_The257th_ClosesWith0x93()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        for (int identifier = 1; identifier <= 256; identifier++)
        {
            connection.Deliver(PublishQoS2((ushort)identifier));
        }

        connection.TakeWritten();
        connection.Deliver(PublishQoS2(1));
        byte[] repeated = connection.TakeWritten();

        // Act
        connection.Deliver(PublishQoS2(257));
        await connection.Run;

        // Assert
        repeated.ShouldBe(PubRec(1));
        connection.TakeWritten().ShouldBe(ServerPackets.Disconnect(MqttReasonCode.ReceiveMaximumExceeded));
        harness.Handler.Calls.ShouldContain("OnClosed c1 ReceiveMaximumPassed");
        PublishCalls(harness).Count.ShouldBe(256);
        connection.Connection.ShouldNotBeNull().GetCounts().HoldsRecords.ShouldBeFalse();
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that PUBACKs produced but not yet handed to the link count against the receive maximum: behind a held
    /// write, 256 wait, and a 257th QoS 1 publish closes the connection with
    /// <see cref="MqttCloseReason.ReceiveMaximumPassed"/> before the host sees it.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task PublishQoS1_257UnwrittenPubAcks_ClosesWith0x93()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        connection.Link.HoldWrites();

        // The PINGRESP is the write with the link; every PUBACK after it waits.
        connection.Deliver(ClientPackets.PingReq);
        for (int identifier = 1; identifier <= 256; identifier++)
        {
            connection.Deliver(PublishQoS1((ushort)identifier));
        }

        ConnectionCounts full = server.GetCounts();

        // Act
        connection.Deliver(PublishQoS1(257));
        bool aborted = connection.Link.IsAborted;
        connection.CompleteHeldWrites();
        await connection.Run;

        // Assert
        full.IncomingInFlight.ShouldBe(256);
        full.WaitingControlPackets.ShouldBe(256);
        aborted.ShouldBeTrue();
        connection.TakeWritten().ShouldBe(ClientPackets.PingResp);
        harness.Handler.Calls.ShouldContain("OnClosed c1 ReceiveMaximumPassed");
        PublishCalls(harness).Count.ShouldBe(256);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a PUBACK written at once never counts against the receive maximum: after each of 1,000 QoS 1
    /// publishes on a socket that takes every write the count is 0.
    /// </summary>
    [Fact]
    public void PublishQoS1_AnsweredAtOnce_NeverCountsUp()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();

        for (int i = 1; i <= 1_000; i++)
        {
            // Act
            connection.Deliver(PublishQoS1((ushort)i));

            // Assert
            connection.TakeWritten().ShouldBe(PubAck((ushort)i));
            server.GetCounts().IncomingInFlight.ShouldBe(0, $"publish {i}");
        }

        PublishCalls(harness).Count.ShouldBe(1_000);
    }

    /// <summary>
    /// Proves that a PUBACK from the client for an identifier that has no record is ignored: nothing is written and
    /// the connection stays open.
    /// </summary>
    [Fact]
    public void PubAck_UnknownIdentifierFromTheClient_IsIgnored()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();

        // Act
        connection.Deliver(ClientPackets.PubAck1);
        byte[] written = connection.TakeWritten();
        connection.Deliver(ClientPackets.PingReq);

        // Assert
        written.ShouldBeEmpty();
        connection.TakeWritten().ShouldBe(ClientPackets.PingResp);
        connection.Run.IsCompleted.ShouldBeFalse();
    }

    /// <summary>
    /// Proves that an acknowledgement from the client is validated: one with packet identifier 0 closes the
    /// connection with 0x82 and <see cref="MqttCloseReason.ProtocolError"/>, one whose body is cut short with 0x81 and
    /// <see cref="MqttCloseReason.MalformedPacket"/>.
    /// </summary>
    /// <param name="packet">
    /// The acknowledgement, in hex.
    /// </param>
    /// <param name="code">
    /// The reason code the DISCONNECT carries.
    /// </param>
    /// <param name="reason">
    /// The reason <c>OnClosed</c> reports.
    /// </param>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Theory]
    [InlineData("40 02 00 00", MqttReasonCode.ProtocolError, MqttCloseReason.ProtocolError)]
    [InlineData("50 02 00 00", MqttReasonCode.ProtocolError, MqttCloseReason.ProtocolError)]
    [InlineData("62 02 00 00", MqttReasonCode.ProtocolError, MqttCloseReason.ProtocolError)]
    [InlineData("70 02 00 00", MqttReasonCode.ProtocolError, MqttCloseReason.ProtocolError)]
    [InlineData("40 01 00", MqttReasonCode.MalformedPacket, MqttCloseReason.MalformedPacket)]
    [InlineData("62 01 00", MqttReasonCode.MalformedPacket, MqttCloseReason.MalformedPacket)]
    public async Task Ack_InvalidFromTheClient_ClosesWithTheDecodersCode(string packet, MqttReasonCode code,
        MqttCloseReason reason)
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();

        // Act
        connection.Deliver(Hex.Parse(packet));
        await connection.Run;

        // Assert
        connection.TakeWritten().ShouldBe(ServerPackets.Disconnect(code));
        harness.Handler.Calls.ShouldContain($"OnClosed c1 {reason}");
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a QoS 2 publish in the skip band, refused as too large with a failing PUBREC, is not remembered:
    /// a PUBREL for its identifier is answered PUBCOMP 0x92.
    /// </summary>
    [Fact]
    public void PublishQoS2_SkippedAndRefused_RemembersNothing()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create(static options => options.MaxPublishPayloadBytes = 1_024);
        TestConnection connection = harness.Connect();
        connection.Deliver(ServerPackets.BuildPublishOfLength(MqttQoS.ExactlyOnce, 16 * 1_024));
        connection.TakeWritten().ShouldBe(ServerPackets.TooLargePubRec1);

        // Act
        connection.Deliver(ClientPackets.PubRel1);

        // Assert
        connection.TakeWritten().ShouldBe(Hex.Parse("70 03 00 01 92"));
        PublishCalls(harness).ShouldBeEmpty();
        connection.Connection.ShouldNotBeNull().GetCounts().HoldsRecords.ShouldBeFalse();
    }

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

    /// <summary>
    /// Builds a QoS 1 PUBLISH to "a/b" with the payload "hi".
    /// </summary>
    /// <param name="packetIdentifier">
    /// The packet identifier.
    /// </param>
    /// <returns>
    /// The packet's bytes.
    /// </returns>
    private static byte[] PublishQoS1(ushort packetIdentifier) =>
        ServerPackets.BuildPublish(MqttQoS.AtLeastOnce, "a/b"u8, packetIdentifier, properties: [], payload: "hi"u8);

    /// <summary>
    /// Builds a QoS 2 PUBLISH to "a/b" with the payload "hi".
    /// </summary>
    /// <param name="packetIdentifier">
    /// The packet identifier.
    /// </param>
    /// <returns>
    /// The packet's bytes.
    /// </returns>
    private static byte[] PublishQoS2(ushort packetIdentifier) =>
        ServerPackets.BuildPublish(MqttQoS.ExactlyOnce, "a/b"u8, packetIdentifier, properties: [], payload: "hi"u8);

    /// <summary>
    /// Builds a PUBACK success.
    /// </summary>
    /// <param name="packetIdentifier">
    /// The packet identifier.
    /// </param>
    /// <returns>
    /// The packet's bytes.
    /// </returns>
    private static byte[] PubAck(ushort packetIdentifier) =>
        [0x40, 0x02, (byte)(packetIdentifier >> 8), (byte)packetIdentifier];

    /// <summary>
    /// Builds a PUBREC success.
    /// </summary>
    /// <param name="packetIdentifier">
    /// The packet identifier.
    /// </param>
    /// <returns>
    /// The packet's bytes.
    /// </returns>
    private static byte[] PubRec(ushort packetIdentifier) =>
        [0x50, 0x02, (byte)(packetIdentifier >> 8), (byte)packetIdentifier];
}
