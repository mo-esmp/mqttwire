using MqttWire.Server;
using MqttWire.Server.Internal;
using MqttWire.Tests.Client;
using MqttWire.Tests.Codec;

namespace MqttWire.Tests.Server;

/// <summary>
/// Tests of the deliveries a host sends at QoS 1 and 2 (design 6.6, "QoS 1 and 2 inside a connection", "Deliveries
/// at QoS 1 or 2"): the exchanges and their records, the client's receive-maximum window, the acknowledgements from
/// the client, the acknowledgement deadline and the packet identifiers.
/// </summary>
public sealed class DeliveryQoSTests
{
    /// <summary>
    /// Proves that a QoS 1 delivery is written with packet identifier 1, holds one record until the client's PUBACK,
    /// and then holds none and no records object.
    /// </summary>
    [Fact]
    public void TrySendQoS1_FullExchange_EndsWithNoRecord()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();

        // Act
        MqttSendResult result = server.TrySend(ServerHarness.Topic, "hi"u8.ToArray(), MqttQoS.AtLeastOnce);
        byte[] written = connection.TakeWritten();
        ConnectionCounts inFlight = server.GetCounts();
        harness.AssertBounds();
        connection.Deliver(ClientPackets.PubAck1);

        // Assert
        result.ShouldBe(MqttSendResult.Taken);
        written.ShouldBe(Hex.Parse("32 0A 00 03 61 2F 62 00 01 00 68 69"));
        inFlight.DeliveryRecords.ShouldBe(1);
        inFlight.HoldsRecords.ShouldBeTrue();
        connection.TakeWritten().ShouldBeEmpty();
        server.GetCounts().ShouldBe(ServerHarness.Idle);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a QoS 2 delivery is written with packet identifier 1, that the client's PUBREC is answered with
    /// PUBREL, and that its PUBCOMP ends the exchange with no record left.
    /// </summary>
    [Fact]
    public void TrySendQoS2_FullExchange_EndsWithNoRecord()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();

        // Act
        server.TrySend(ServerHarness.Topic, "hi"u8.ToArray(), MqttQoS.ExactlyOnce).ShouldBe(MqttSendResult.Taken);
        byte[] publish = connection.TakeWritten();
        connection.Deliver(ClientPackets.PubRec1);
        byte[] pubRel = connection.TakeWritten();
        ConnectionCounts waitingForPubComp = server.GetCounts();
        connection.Deliver(ClientPackets.PubComp1);

        // Assert
        publish.ShouldBe(ClientPackets.PublishQoS2);
        pubRel.ShouldBe(ClientPackets.PubRel1);
        waitingForPubComp.DeliveryRecords.ShouldBe(1);
        connection.TakeWritten().ShouldBeEmpty();
        server.GetCounts().ShouldBe(ServerHarness.Idle);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a PUBREC with a failure code ends a QoS 2 delivery's exchange: no PUBREL is written, no record is
    /// left, and its place in a window of one is free for the next delivery.
    /// </summary>
    [Fact]
    public void TrySendQoS2_PubRecWithAFailureCode_EndsTheExchange()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect(connect: ServerPackets.ConnectReceiveMaximum1);
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        server.TrySend(ServerHarness.Topic, "hi"u8.ToArray(), MqttQoS.ExactlyOnce);
        connection.TakeWritten().ShouldBe(ClientPackets.PublishQoS2);

        // Act
        connection.Deliver(Hex.Parse("50 03 00 01 87"));
        byte[] afterTheFailure = connection.TakeWritten();
        ConnectionCounts counts = server.GetCounts();
        server.TrySend(ServerHarness.Topic, "b"u8.ToArray(), MqttQoS.ExactlyOnce);
        harness.AssertBounds();

        // Assert
        afterTheFailure.ShouldBeEmpty();
        counts.ShouldBe(ServerHarness.Idle);
        connection.TakeWritten().ShouldBe(ServerPackets.Delivery(MqttQoS.ExactlyOnce, packetIdentifier: 2, "b"u8));
        connection.Run.IsCompleted.ShouldBeFalse();
    }

    /// <summary>
    /// Proves that with the client's Receive Maximum 2 no more than two QoS 1 deliveries are ever unacknowledged: of
    /// five, two are written and three wait, and each PUBACK lets exactly one more out.
    /// </summary>
    [Fact]
    public void TrySendQoS1_WindowOfTwo_NeverHasThreeInFlight()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect(connect: ServerPackets.ConnectReceiveMaximum2);
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();

        // Act
        for (int i = 1; i <= 5; i++)
        {
            server.TrySend(ServerHarness.Topic, new byte[] { (byte)('0' + i) }, MqttQoS.AtLeastOnce).ShouldBe(MqttSendResult.Taken);
            harness.AssertBounds();
        }

        byte[] firstTwo = connection.TakeWritten();
        ConnectionCounts full = server.GetCounts();
        connection.Deliver(ServerPackets.PubAck(1));
        byte[] third = connection.TakeWritten();
        connection.Deliver(ServerPackets.PubAck(2));
        byte[] fourth = connection.TakeWritten();
        connection.Deliver(ServerPackets.PubAck(3));
        byte[] fifth = connection.TakeWritten();
        ConnectionCounts afterThreeAcks = server.GetCounts();

        // Assert
        firstTwo.ShouldBe([
            .. ServerPackets.Delivery(MqttQoS.AtLeastOnce, packetIdentifier: 1, "1"u8),
            .. ServerPackets.Delivery(MqttQoS.AtLeastOnce, packetIdentifier: 2, "2"u8),
        ]);
        full.DeliveryRecords.ShouldBe(2);
        full.WaitingDeliveries.ShouldBe(3);
        full.IsWriting.ShouldBeFalse();
        third.ShouldBe(ServerPackets.Delivery(MqttQoS.AtLeastOnce, packetIdentifier: 3, "3"u8));
        fourth.ShouldBe(ServerPackets.Delivery(MqttQoS.AtLeastOnce, packetIdentifier: 4, "4"u8));
        fifth.ShouldBe(ServerPackets.Delivery(MqttQoS.AtLeastOnce, packetIdentifier: 5, "5"u8));
        afterThreeAcks.DeliveryRecords.ShouldBe(2);
        afterThreeAcks.WaitingDeliveries.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a QoS 0 delivery sent behind a QoS 1 delivery that waits for the window waits too, and goes out
    /// right behind it, in the order of the <c>TrySend</c> calls.
    /// </summary>
    [Fact]
    public void TrySendQoS0_BehindAWaitingQoS1_KeepsItsPlaceInTheOrder()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect(connect: ServerPackets.ConnectReceiveMaximum1);
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();

        // Act
        server.TrySend(ServerHarness.Topic, "a"u8.ToArray(), MqttQoS.AtLeastOnce);
        server.TrySend(ServerHarness.Topic, "b"u8.ToArray(), MqttQoS.AtLeastOnce);
        server.TrySend(ServerHarness.Topic, "c"u8.ToArray(), MqttQoS.AtMostOnce);
        harness.AssertBounds();
        byte[] beforeTheAck = connection.TakeWritten();
        ConnectionCounts waiting = server.GetCounts();
        connection.Deliver(ClientPackets.PubAck1);

        // Assert
        beforeTheAck.ShouldBe(ServerPackets.Delivery(MqttQoS.AtLeastOnce, packetIdentifier: 1, "a"u8));
        waiting.WaitingDeliveries.ShouldBe(2);
        connection.TakeWritten().ShouldBe([
            .. ServerPackets.Delivery(MqttQoS.AtLeastOnce, packetIdentifier: 2, "b"u8),
            .. ServerPackets.Delivery(MqttQoS.AtMostOnce, packetIdentifier: 0, "c"u8),
        ]);
        server.GetCounts().DeliveryRecords.ShouldBe(1);
        server.GetCounts().WaitingDeliveries.ShouldBe(0);
    }

    /// <summary>
    /// Proves that deliveries that wait for the window count towards <see cref="MqttWireServerOptions.MaxOutgoingMessages"/>
    /// and <see cref="MqttWireServerOptions.MaxOutgoingBytes"/>: one more than the limit evicts the connection, and
    /// since no write is in progress, DISCONNECT 0x97 is written.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task TrySendQoS1_WaitingForTheWindow_CountsTowardsBothLimits()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create(static options => options.MaxOutgoingMessages = 3);
        TestConnection connection = harness.Connect(connect: ServerPackets.ConnectReceiveMaximum1);
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        for (int i = 0; i < 4; i++)
        {
            server.TrySend(ServerHarness.Topic, "hi"u8.ToArray(), MqttQoS.AtLeastOnce).ShouldBe(MqttSendResult.Taken);
            harness.AssertBounds();
        }

        connection.TakeWritten();
        ConnectionCounts waiting = server.GetCounts();

        // Act
        MqttSendResult fifth = server.TrySend(ServerHarness.Topic, "hi"u8.ToArray(), MqttQoS.AtLeastOnce);
        connection.Link.EndFromPeer();
        await connection.RunEnded();

        // Assert
        waiting.WaitingDeliveries.ShouldBe(3);

        // Each waiting delivery is counted with its topic, "a/b", and its payload of two bytes.
        waiting.OutgoingBytes.ShouldBe(3 * (3 + 2));
        waiting.IsWriting.ShouldBeFalse();
        fifth.ShouldBe(MqttSendResult.Evicted);
        connection.TakeWritten().ShouldBe(ServerPackets.Disconnect(MqttReasonCode.QuotaExceeded));
        harness.Handler.Calls.ShouldContain("OnClosed c1 OutgoingLimitPassed");
        server.GetCounts().ShouldBe(ServerHarness.Idle);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that the payload bytes of deliveries that wait for the window count towards
    /// <see cref="MqttWireServerOptions.MaxOutgoingBytes"/>: the delivery that would pass it evicts the connection
    /// with DISCONNECT 0x97.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task TrySendQoS1_WaitingForTheWindowPastMaxOutgoingBytes_EvictsWith0x97()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create(static options => options.MaxOutgoingBytes = 65_536);
        TestConnection connection = harness.Connect(connect: ServerPackets.ConnectReceiveMaximum1);
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        byte[] payload = new byte[20_000];
        for (int i = 0; i < 4; i++)
        {
            server.TrySend(ServerHarness.Topic, payload, MqttQoS.AtLeastOnce).ShouldBe(MqttSendResult.Taken);
            harness.AssertBounds();
        }

        connection.TakeWritten();

        // Act
        MqttSendResult fifth = server.TrySend(ServerHarness.Topic, payload, MqttQoS.AtLeastOnce);
        connection.Link.EndFromPeer();
        await connection.RunEnded();

        // Assert
        fifth.ShouldBe(MqttSendResult.Evicted);
        connection.TakeWritten().ShouldBe(ServerPackets.Disconnect(MqttReasonCode.QuotaExceeded));
        harness.Handler.Calls.ShouldContain("OnClosed c1 OutgoingLimitPassed");
        server.GetCounts().ShouldBe(ServerHarness.Idle);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a QoS 1 delivery whose PUBACK never comes closes the connection with 0x82 and
    /// <see cref="MqttCloseReason.AcknowledgementTimedOut"/> at the sweep 30 seconds after it was written, not before.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task TrySendQoS1_PubAckNeverComes_ClosesWith0x82AfterThirtySeconds()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        server.TrySend(ServerHarness.Topic, "hi"u8.ToArray(), MqttQoS.AtLeastOnce);
        connection.TakeWritten().ShouldBe(ClientPackets.PublishQoS1);

        // Act
        harness.Advance(TimeSpan.FromSeconds(28));
        byte[] writtenAt28 = connection.TakeWritten();
        harness.Advance(TimeSpan.FromSeconds(2));
        byte[] writtenAt30 = connection.TakeWritten();
        connection.Link.EndFromPeer();
        await connection.RunEnded();

        // Assert
        writtenAt28.ShouldBeEmpty();
        writtenAt30.ShouldBe(ServerPackets.Disconnect(MqttReasonCode.ProtocolError));
        harness.Handler.Calls.ShouldContain("OnClosed c1 AcknowledgementTimedOut");
        server.GetCounts().HoldsRecords.ShouldBeFalse();
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that the PUBREC of a QoS 2 delivery starts its acknowledgement deadline again: with the PUBREC at 20
    /// seconds the connection is still open at 48 seconds and is closed with 0x82 at the sweep at 50 seconds.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task TrySendQoS2_PubCompNeverComes_DeadlineRestartsAtThePubRec()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        server.TrySend(ServerHarness.Topic, "hi"u8.ToArray(), MqttQoS.ExactlyOnce);
        connection.TakeWritten().ShouldBe(ClientPackets.PublishQoS2);

        // Act
        harness.Advance(TimeSpan.FromSeconds(20));
        connection.Deliver(ClientPackets.PubRec1);
        byte[] pubRel = connection.TakeWritten();
        harness.Advance(TimeSpan.FromSeconds(28));
        byte[] writtenAt48 = connection.TakeWritten();
        bool openAt48 = !connection.Run.IsCompleted;
        harness.Advance(TimeSpan.FromSeconds(2));
        byte[] writtenAt50 = connection.TakeWritten();
        connection.Link.EndFromPeer();
        await connection.RunEnded();

        // Assert
        pubRel.ShouldBe(ClientPackets.PubRel1);
        writtenAt48.ShouldBeEmpty();
        openAt48.ShouldBeTrue();
        writtenAt50.ShouldBe(ServerPackets.Disconnect(MqttReasonCode.ProtocolError));
        harness.Handler.Calls.ShouldContain("OnClosed c1 AcknowledgementTimedOut");
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a PUBACK for an identifier that has no record is ignored: nothing is written, the record of
    /// another delivery stays, and the connection stays open.
    /// </summary>
    [Fact]
    public void PubAck_UnknownIdentifier_IsIgnored()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        server.TrySend(ServerHarness.Topic, "hi"u8.ToArray(), MqttQoS.AtLeastOnce);
        connection.TakeWritten();

        // Act
        connection.Deliver(ServerPackets.PubAck(7));

        // Assert
        connection.TakeWritten().ShouldBeEmpty();
        server.GetCounts().DeliveryRecords.ShouldBe(1);
        connection.Run.IsCompleted.ShouldBeFalse();
    }

    /// <summary>
    /// Proves that a PUBCOMP for an identifier that has no record is ignored: nothing is written and the connection
    /// stays open.
    /// </summary>
    [Fact]
    public void PubComp_UnknownIdentifier_IsIgnored()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();

        // Act
        connection.Deliver(ClientPackets.PubComp7);

        // Assert
        connection.TakeWritten().ShouldBeEmpty();
        server.GetCounts().ShouldBe(ServerHarness.Idle);
        connection.Run.IsCompleted.ShouldBeFalse();
    }

    /// <summary>
    /// Proves that a PUBREC for an identifier that has no record is answered with PUBREL 0x92 (packet identifier not
    /// found) and changes nothing else.
    /// </summary>
    [Fact]
    public void PubRec_UnknownIdentifier_AnswersPubRel0x92()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();

        // Act
        connection.Deliver(ClientPackets.PubRec7);

        // Assert
        connection.TakeWritten().ShouldBe(ClientPackets.PubRelNotFound7);
        server.GetCounts().ShouldBe(ServerHarness.Idle);
        connection.Run.IsCompleted.ShouldBeFalse();
    }

    /// <summary>
    /// Proves that a PUBREC with a failure code for an identifier that has no record is ignored, as the client does
    /// (design 6.9): a failing PUBREC ends its exchange, so no PUBREL may follow it.
    /// </summary>
    [Fact]
    public void PubRec_UnknownIdentifierWithAFailureCode_IsIgnored()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();

        // Act
        connection.Deliver(Hex.Parse("50 03 00 07 87"));

        // Assert
        connection.TakeWritten().ShouldBeEmpty();
        server.GetCounts().ShouldBe(ServerHarness.Idle);
        connection.Run.IsCompleted.ShouldBeFalse();
    }

    /// <summary>
    /// Proves that a PUBREC repeated for a delivery already waiting for PUBCOMP is answered with PUBREL again and
    /// changes nothing: the record stays, and its deadline still runs from the first PUBREC.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task PubRec_Repeated_AnswersPubRelAgainAndChangesNothing()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        server.TrySend(ServerHarness.Topic, "hi"u8.ToArray(), MqttQoS.ExactlyOnce);
        connection.TakeWritten();
        connection.Deliver(ClientPackets.PubRec1);
        connection.TakeWritten().ShouldBe(ClientPackets.PubRel1);

        // Act
        harness.Advance(TimeSpan.FromSeconds(20));
        connection.Deliver(ClientPackets.PubRec1);
        byte[] secondPubRel = connection.TakeWritten();
        ConnectionCounts counts = server.GetCounts();
        harness.Advance(TimeSpan.FromSeconds(10));
        byte[] writtenAt30 = connection.TakeWritten();
        connection.Link.EndFromPeer();
        await connection.RunEnded();

        // Assert
        secondPubRel.ShouldBe(ClientPackets.PubRel1);
        counts.DeliveryRecords.ShouldBe(1);
        writtenAt30.ShouldBe(ServerPackets.Disconnect(MqttReasonCode.ProtocolError));
        harness.Handler.Calls.ShouldContain("OnClosed c1 AcknowledgementTimedOut");
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a failing PUBREC for a QoS 2 delivery that waits for its PUBCOMP already is taken as the harmless
    /// repetition of a PUBREC (design 6.6): it is answered with PUBREL again, and the record and its place in the
    /// client's window stay, so that with the client's Receive Maximum 1 the next delivery still waits, until the
    /// PUBCOMP lets it out.
    /// </summary>
    [Fact]
    public void PubRec_FailingForAQoS2DeliveryWaitingForPubComp_AnswersPubRelAgainAndKeepsTheRecord()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect(connect: ServerPackets.ConnectReceiveMaximum1);
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        server.TrySend(ServerHarness.Topic, "hi"u8.ToArray(), MqttQoS.ExactlyOnce).ShouldBe(MqttSendResult.Taken);
        harness.AssertBounds();
        connection.Deliver(ClientPackets.PubRec1);
        server.TrySend(ServerHarness.Topic, "B"u8.ToArray(), MqttQoS.ExactlyOnce).ShouldBe(MqttSendResult.Taken);
        harness.AssertBounds();
        connection.TakeWritten().ShouldBe([.. ClientPackets.PublishQoS2, .. ClientPackets.PubRel1]);

        // Act
        connection.Deliver(Hex.Parse("50 03 00 01 87"));
        byte[] answer = connection.TakeWritten();
        ConnectionCounts afterTheRepeat = server.GetCounts();
        connection.Deliver(ClientPackets.PubComp1);
        byte[] afterThePubComp = connection.TakeWritten();

        // Assert
        answer.ShouldBe(ClientPackets.PubRel1);
        afterTheRepeat.DeliveryRecords.ShouldBe(1);
        afterTheRepeat.WaitingDeliveries.ShouldBe(1);
        afterThePubComp.ShouldBe(ServerPackets.Delivery(MqttQoS.ExactlyOnce, packetIdentifier: 2, "B"u8));
        connection.Run.IsCompleted.ShouldBeFalse();
    }

    /// <summary>
    /// Proves that an acknowledgement of the wrong kind for an identifier that has a record closes the connection
    /// with DISCONNECT 0x82 and <see cref="MqttCloseReason.ProtocolError"/>.
    /// </summary>
    /// <param name="qos">
    /// The QoS of the delivery the acknowledgement names.
    /// </param>
    /// <param name="acknowledgement">
    /// The acknowledgement from the client, in hex.
    /// </param>
    /// <param name="before">
    /// An acknowledgement the client sends first, in hex, which moves the exchange on; empty for none.
    /// </param>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Theory]
    [InlineData(MqttQoS.ExactlyOnce, "40 02 00 01", "")] // A PUBACK for a QoS 2 delivery.
    [InlineData(MqttQoS.ExactlyOnce, "70 02 00 01", "")] // A PUBCOMP before the PUBREC.
    [InlineData(MqttQoS.AtLeastOnce, "50 02 00 01", "")] // A PUBREC for a QoS 1 delivery.
    [InlineData(MqttQoS.AtLeastOnce, "70 02 00 01", "")] // A PUBCOMP for a QoS 1 delivery.
    [InlineData(MqttQoS.ExactlyOnce, "40 02 00 01", "50 02 00 01")] // A PUBACK for a QoS 2 delivery after its PUBREC.
    [InlineData(MqttQoS.AtLeastOnce, "50 03 00 01 87", "")] // A failing PUBREC for a QoS 1 delivery.
    public async Task Ack_WrongKindForAKnownIdentifier_ClosesWith0x82(MqttQoS qos, string acknowledgement,
        string before)
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        server.TrySend(ServerHarness.Topic, "hi"u8.ToArray(), qos);
        harness.AssertBounds();
        if (before.Length > 0)
        {
            connection.Deliver(Hex.Parse(before));
        }

        connection.TakeWritten();

        // Act
        connection.Deliver(Hex.Parse(acknowledgement));
        await connection.RunEnded();

        // Assert
        connection.TakeWritten().ShouldBe(ServerPackets.Disconnect(MqttReasonCode.ProtocolError));
        harness.Handler.Calls.ShouldContain("OnClosed c1 ProtocolError");
        server.GetCounts().HoldsRecords.ShouldBeFalse();
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a PUBACK that arrives while its own PUBLISH is still being written finds the delivery's record,
    /// which was in place before the packet was handed to the link, and completes the exchange; the write then ends
    /// with nothing left.
    /// </summary>
    [Fact]
    public void PubAck_ArrivesWhileItsPublishIsStillBeingWritten_CompletesTheExchange()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        connection.Link.HoldWrites();
        server.TrySend(ServerHarness.Topic, "hi"u8.ToArray(), MqttQoS.AtLeastOnce);
        harness.AssertBounds();

        // Act
        connection.Deliver(ClientPackets.PubAck1);
        ConnectionCounts whileWriting = server.GetCounts();
        connection.CompleteHeldWrites();

        // Assert
        whileWriting.DeliveryRecords.ShouldBe(0);
        whileWriting.IsWriting.ShouldBeTrue();
        connection.TakeWritten().ShouldBe(ClientPackets.PublishQoS1);
        server.GetCounts().ShouldBe(ServerHarness.Idle);
        connection.Run.IsCompleted.ShouldBeFalse();
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that the packet identifiers go up to 65,535, wrap to 1, and skip those still in use: with the counter
    /// started at 65,534 and identifier 1 in flight, three deliveries get 65,534, 65,535 and 2.
    /// </summary>
    [Fact]
    public void TrySendQoS1_IdentifiersAcrossTheWrap_SkipThoseInUse()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        server.TrySend(ServerHarness.Topic, "hi"u8.ToArray(), MqttQoS.AtLeastOnce);
        connection.TakeWritten().ShouldBe(ClientPackets.PublishQoS1);
        server.SetLastDeliveryIdentifier(65_533);

        // Act
        server.TrySend(ServerHarness.Topic, "x"u8.ToArray(), MqttQoS.AtLeastOnce);
        server.TrySend(ServerHarness.Topic, "y"u8.ToArray(), MqttQoS.AtLeastOnce);
        server.TrySend(ServerHarness.Topic, "z"u8.ToArray(), MqttQoS.AtLeastOnce);

        // Assert
        connection.TakeWritten().ShouldBe([
            .. ServerPackets.Delivery(MqttQoS.AtLeastOnce, packetIdentifier: 65_534, "x"u8),
            .. ServerPackets.Delivery(MqttQoS.AtLeastOnce, packetIdentifier: 65_535, "y"u8),
            .. ServerPackets.Delivery(MqttQoS.AtLeastOnce, packetIdentifier: 2, "z"u8),
        ]);
        server.GetCounts().DeliveryRecords.ShouldBe(4);
        harness.AssertBounds();
    }

    /// <summary>
    /// Proves that once the last delivery has been acknowledged the connection is idle again: it holds no record, no
    /// records object, no waiting delivery and no buffer.
    /// </summary>
    [Fact]
    public void TrySendQoS1_AfterTheLastAcknowledgement_TheConnectionIsIdleAgain()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect(connect: ServerPackets.ConnectReceiveMaximum1);
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        server.TrySend(ServerHarness.Topic, "a"u8.ToArray(), MqttQoS.AtLeastOnce);
        server.TrySend(ServerHarness.Topic, "b"u8.ToArray(), MqttQoS.ExactlyOnce);
        connection.TakeWritten();

        // Act
        connection.Deliver(ServerPackets.PubAck(1));
        connection.Deliver(ServerPackets.PubRec(2));
        connection.Deliver(ServerPackets.PubComp(2));

        // Assert
        connection.TakeWritten().ShouldBe([
            .. ServerPackets.Delivery(MqttQoS.ExactlyOnce, packetIdentifier: 2, "b"u8),
            .. ServerPackets.PubRel(2),
        ]);
        server.GetCounts().ShouldBe(ServerHarness.Idle);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a delivery the server has taken is delivered although the client unsubscribes meanwhile (design
    /// 6.6, "Deliveries at QoS 1 or 2", item 5): the UNSUBACK goes out at once, and the delivery once the window
    /// has room.
    /// </summary>
    [Fact]
    public void TrySendQoS1_ClientUnsubscribesMeanwhile_IsStillDelivered()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect(connect: ServerPackets.ConnectReceiveMaximum1);
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        connection.Deliver(ServerPackets.Subscribe);
        connection.TakeWritten().ShouldBe(ServerPackets.SubAckGrantedQoS1);
        server.TrySend(ServerHarness.Topic, "a"u8.ToArray(), MqttQoS.AtLeastOnce);
        server.TrySend(ServerHarness.Topic, "b"u8.ToArray(), MqttQoS.AtLeastOnce);
        connection.TakeWritten();

        // Act
        connection.Deliver(ServerPackets.Unsubscribe);
        byte[] unsubAck = connection.TakeWritten();
        connection.Deliver(ClientPackets.PubAck1);

        // Assert
        unsubAck.ShouldBe(ServerPackets.UnsubAck);
        connection.TakeWritten().ShouldBe(ServerPackets.Delivery(MqttQoS.AtLeastOnce, packetIdentifier: 2, "b"u8));
        connection.Run.IsCompleted.ShouldBeFalse();
    }

    /// <summary>
    /// Proves that the PUBCOMP a client sends for a repeated PUBREL is ignored, because the identifier of an exchange
    /// that has just ended is not handed out again at once: with a window of one, B goes out with identifier 2, the
    /// second PUBCOMP for 1 meets no record, the connection stays open, and B's exchange completes normally.
    /// </summary>
    [Fact]
    public void PubComp_SecondForARepeatedPubRel_IsIgnoredBecauseItsIdentifierIsNotReused()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect(connect: ServerPackets.ConnectReceiveMaximum1);
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        server.TrySend(ServerHarness.Topic, "A"u8.ToArray(), MqttQoS.ExactlyOnce);
        server.TrySend(ServerHarness.Topic, "B"u8.ToArray(), MqttQoS.ExactlyOnce);
        harness.AssertBounds();
        connection.TakeWritten().ShouldBe(ServerPackets.Delivery(MqttQoS.ExactlyOnce, packetIdentifier: 1, "A"u8));

        // Act
        connection.Deliver(ServerPackets.PubRec(1));
        byte[] firstPubRel = connection.TakeWritten();
        connection.Deliver(ServerPackets.PubRec(1));
        byte[] secondPubRel = connection.TakeWritten();
        connection.Deliver(ServerPackets.PubComp(1));
        byte[] b = connection.TakeWritten();
        connection.Deliver(ServerPackets.PubComp(1));
        byte[] afterTheSecondPubComp = connection.TakeWritten();
        ConnectionCounts bInFlight = server.GetCounts();
        connection.Deliver(ServerPackets.PubRec(2));
        byte[] bPubRel = connection.TakeWritten();
        connection.Deliver(ServerPackets.PubComp(2));

        // Assert
        firstPubRel.ShouldBe(ServerPackets.PubRel(1));
        secondPubRel.ShouldBe(ServerPackets.PubRel(1));
        b.ShouldBe(ServerPackets.Delivery(MqttQoS.ExactlyOnce, packetIdentifier: 2, "B"u8));
        afterTheSecondPubComp.ShouldBeEmpty();
        bInFlight.DeliveryRecords.ShouldBe(1);
        bPubRel.ShouldBe(ServerPackets.PubRel(2));
        connection.TakeWritten().ShouldBeEmpty();
        server.GetCounts().ShouldBe(ServerHarness.Idle);
        connection.Run.IsCompleted.ShouldBeFalse();
    }

    /// <summary>
    /// Proves that a duplicate PUBACK for an identifier whose exchange has ended does not touch the next QoS 1
    /// delivery: with a window of one, the next delivery has identifier 2, the duplicate PUBACK for 1 is ignored, and
    /// the place stays taken, so a third delivery waits until the PUBACK for 2.
    /// </summary>
    [Fact]
    public void PubAck_DuplicateAfterTheExchangeEnded_DoesNotTouchTheNextDelivery()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect(connect: ServerPackets.ConnectReceiveMaximum1);
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        server.TrySend(ServerHarness.Topic, "a"u8.ToArray(), MqttQoS.AtLeastOnce);
        server.TrySend(ServerHarness.Topic, "b"u8.ToArray(), MqttQoS.AtLeastOnce);
        connection.TakeWritten();
        connection.Deliver(ServerPackets.PubAck(1));
        connection.TakeWritten().ShouldBe(ServerPackets.Delivery(MqttQoS.AtLeastOnce, packetIdentifier: 2, "b"u8));

        // Act
        connection.Deliver(ServerPackets.PubAck(1));
        server.TrySend(ServerHarness.Topic, "c"u8.ToArray(), MqttQoS.AtLeastOnce);
        harness.AssertBounds();
        byte[] afterTheDuplicate = connection.TakeWritten();
        ConnectionCounts counts = server.GetCounts();
        connection.Deliver(ServerPackets.PubAck(2));

        // Assert
        afterTheDuplicate.ShouldBeEmpty();
        counts.DeliveryRecords.ShouldBe(1);
        counts.WaitingDeliveries.ShouldBe(1);
        connection.TakeWritten().ShouldBe(ServerPackets.Delivery(MqttQoS.AtLeastOnce, packetIdentifier: 3, "c"u8));
        connection.Run.IsCompleted.ShouldBeFalse();
    }
}
