using MqttWire.Server;
using MqttWire.Server.Internal;
using MqttWire.Tests.Client;

namespace MqttWire.Tests.Server;

/// <summary>
/// The window deadlock, step by step (design 6.13 item 2, and 6.6, "Scheduling: control packets never wait behind a
/// delivery"): with the client's Receive Maximum 1, delivery A at QoS 2 fills the window and delivery B waits for it;
/// the PUBREL without which the client never sends the PUBCOMP that frees the window, and every other control
/// packet, must not wait behind B. B takes packet identifier 2: the counter is the connection's, so A's identifier is
/// not handed out again at once although A's records went back to the pool. The bounds are asserted after every
/// step.
/// </summary>
public sealed class WindowDeadlockTests
{
    /// <summary>
    /// The topic every test sends to.
    /// </summary>
    private static readonly MqttTopic s_topic = MqttTopic.Create("a/b");

    /// <summary>
    /// What <c>GetCounts</c> reports for a connection that holds nothing and writes nothing.
    /// </summary>
    private static readonly ConnectionCounts s_idle = new(IncomingRecords: 0, DeliveryRecords: 0, IncomingInFlight: 0,
        HoldsRecords: false, WaitingDeliveries: 0, WaitingControlPackets: 0, OutgoingBytes: 0, IsWriting: false,
        HoldsReadBuffer: false);

    /// <summary>
    /// Proves that with the window full the PUBREL for A is written at once although B waits, that A's PUBCOMP lets B
    /// out, and that B's exchange leaves the connection idle.
    /// </summary>
    [Fact]
    public void Window_Full_PubRelIsWrittenAlthoughADeliveryWaits()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect(connect: ServerPackets.ConnectReceiveMaximum1);
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();

        // Act
        // 1: A is written.
        MqttSendResult a = server.TrySend(s_topic, "A"u8.ToArray(), MqttQoS.ExactlyOnce);
        harness.AssertBounds();
        byte[] step1 = connection.TakeWritten();

        // 2: B is taken and waits for the window.
        MqttSendResult b = server.TrySend(s_topic, "B"u8.ToArray(), MqttQoS.ExactlyOnce);
        harness.AssertBounds();
        byte[] step2 = connection.TakeWritten();
        ConnectionCounts afterStep2 = server.GetCounts();

        // 3: the PUBREC for A is answered with PUBREL now; B still waits.
        connection.Deliver(ServerPackets.PubRec(1));
        byte[] step3 = connection.TakeWritten();
        ConnectionCounts afterStep3 = server.GetCounts();

        // 4: the PUBCOMP for A frees the window, and B goes out.
        connection.Deliver(ServerPackets.PubComp(1));
        byte[] step4 = connection.TakeWritten();

        // 5: B's exchange ends, and the connection is idle.
        connection.Deliver(ServerPackets.PubRec(2));
        byte[] step5PubRel = connection.TakeWritten();
        connection.Deliver(ServerPackets.PubComp(2));
        byte[] step5End = connection.TakeWritten();

        // Assert
        a.ShouldBe(MqttSendResult.Taken);
        step1.ShouldBe(ServerPackets.Delivery(MqttQoS.ExactlyOnce, packetIdentifier: 1, "A"u8));
        b.ShouldBe(MqttSendResult.Taken);
        step2.ShouldBeEmpty();
        afterStep2.WaitingDeliveries.ShouldBe(1);
        afterStep2.DeliveryRecords.ShouldBe(1);
        afterStep2.IsWriting.ShouldBeFalse();
        step3.ShouldBe(ServerPackets.PubRel(1));
        afterStep3.WaitingDeliveries.ShouldBe(1);
        step4.ShouldBe(ServerPackets.Delivery(MqttQoS.ExactlyOnce, packetIdentifier: 2, "B"u8));
        step5PubRel.ShouldBe(ServerPackets.PubRel(2));
        step5End.ShouldBeEmpty();
        server.GetCounts().ShouldBe(s_idle);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that with the window full and B waiting, a PINGREQ is answered with PINGRESP at once.
    /// </summary>
    [Fact]
    public void Window_Full_PingReqIsAnsweredAlthoughADeliveryWaits()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect(connect: ServerPackets.ConnectReceiveMaximum1);
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        FillTheWindowAndWait(harness, connection, server);

        // Act
        connection.Deliver(ClientPackets.PingReq);
        byte[] answer = connection.TakeWritten();
        ConnectionCounts afterThePing = server.GetCounts();
        byte[] rest = FinishA(connection);

        // Assert
        answer.ShouldBe(ClientPackets.PingResp);
        afterThePing.WaitingDeliveries.ShouldBe(1);
        rest.ShouldBe([
            .. ServerPackets.PubRel(1),
            .. ServerPackets.Delivery(MqttQoS.ExactlyOnce, packetIdentifier: 2, "B"u8),
        ]);
    }

    /// <summary>
    /// Proves that with the window full and B waiting, a QoS 1 publish from the client reaches the host and is
    /// answered with PUBACK at once.
    /// </summary>
    [Fact]
    public void Window_Full_AClientsQoS1PublishIsAcknowledgedAlthoughADeliveryWaits()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect(connect: ServerPackets.ConnectReceiveMaximum1);
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        FillTheWindowAndWait(harness, connection, server);

        // Act
        connection.Deliver(ClientPackets.PublishQoS1);
        byte[] answer = connection.TakeWritten();
        ConnectionCounts afterThePublish = server.GetCounts();
        byte[] rest = FinishA(connection);

        // Assert
        answer.ShouldBe(ClientPackets.PubAck1);
        harness.Handler.Calls.ShouldContain("OnPublish c1 a/b hi q1 retain=0");
        afterThePublish.WaitingDeliveries.ShouldBe(1);
        rest.ShouldBe([
            .. ServerPackets.PubRel(1),
            .. ServerPackets.Delivery(MqttQoS.ExactlyOnce, packetIdentifier: 2, "B"u8),
        ]);
    }

    /// <summary>
    /// Proves that with A's write held by the link and B waiting for the window, the PINGRESP and the PUBREL produced
    /// meanwhile are written first when the write is released, and that B is not written until A's PUBCOMP.
    /// </summary>
    [Fact]
    public void Window_FullAndAWriteHeld_ControlPacketsStillGoFirstWhenItIsReleased()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect(connect: ServerPackets.ConnectReceiveMaximum1);
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        connection.Link.HoldWrites();
        server.TrySend(s_topic, "A"u8.ToArray(), MqttQoS.ExactlyOnce).ShouldBe(MqttSendResult.Taken);
        harness.AssertBounds();
        server.TrySend(s_topic, "B"u8.ToArray(), MqttQoS.ExactlyOnce).ShouldBe(MqttSendResult.Taken);
        harness.AssertBounds();

        // Act
        connection.Deliver(ClientPackets.PingReq);
        connection.Deliver(ServerPackets.PubRec(1));
        ConnectionCounts whileHeld = server.GetCounts();
        connection.CompleteHeldWrites();
        byte[] released = connection.TakeWritten();
        ConnectionCounts afterTheRelease = server.GetCounts();
        connection.Deliver(ServerPackets.PubComp(1));

        // Assert
        whileHeld.WaitingControlPackets.ShouldBe(2);
        whileHeld.WaitingDeliveries.ShouldBe(1);
        released.ShouldBe([
            .. ServerPackets.Delivery(MqttQoS.ExactlyOnce, packetIdentifier: 1, "A"u8),
            .. ClientPackets.PingResp,
            .. ServerPackets.PubRel(1),
        ]);
        afterTheRelease.WaitingDeliveries.ShouldBe(1);
        afterTheRelease.IsWriting.ShouldBeFalse();
        connection.TakeWritten().ShouldBe(ServerPackets.Delivery(MqttQoS.ExactlyOnce, packetIdentifier: 2, "B"u8));
        harness.AssertBounds();
    }

    /// <summary>
    /// Proves that an acknowledgement that frees a place in the window while a write is held leaves the waiting
    /// delivery to the holder of the send path: with the client's Receive Maximum 1, A at QoS 1 is written and B waits;
    /// the link then holds the PINGRESP of a PINGREQ; A's PUBACK frees the place, but nothing more is written while
    /// the PINGRESP is held; when it completes, the drain takes B at once.
    /// </summary>
    [Fact]
    public void Window_PlaceFreedWhileAWriteIsHeld_TheDrainTakesTheWaitingDelivery()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect(connect: ServerPackets.ConnectReceiveMaximum1);
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        server.TrySend(s_topic, "A"u8.ToArray(), MqttQoS.AtLeastOnce).ShouldBe(MqttSendResult.Taken);
        harness.AssertBounds();
        server.TrySend(s_topic, "B"u8.ToArray(), MqttQoS.AtLeastOnce).ShouldBe(MqttSendResult.Taken);
        harness.AssertBounds();
        byte[] beforeTheHold = connection.TakeWritten();
        connection.Link.HoldWrites();
        connection.Deliver(ClientPackets.PingReq);

        // Act
        connection.Deliver(ServerPackets.PubAck(1));
        byte[] whileHeld = connection.TakeWritten();
        ConnectionCounts afterThePubAck = server.GetCounts();
        connection.CompleteHeldWrites();
        byte[] released = connection.TakeWritten();
        ConnectionCounts afterTheRelease = server.GetCounts();

        // Assert
        beforeTheHold.ShouldBe(ServerPackets.Delivery(MqttQoS.AtLeastOnce, packetIdentifier: 1, "A"u8));
        whileHeld.ShouldBeEmpty();
        afterThePubAck.DeliveryRecords.ShouldBe(0);
        afterThePubAck.WaitingDeliveries.ShouldBe(1);
        afterThePubAck.IsWriting.ShouldBeTrue();
        released.ShouldBe([
            .. ClientPackets.PingResp,
            .. ServerPackets.Delivery(MqttQoS.AtLeastOnce, packetIdentifier: 2, "B"u8),
        ]);
        afterTheRelease.WaitingDeliveries.ShouldBe(0);
        afterTheRelease.DeliveryRecords.ShouldBe(1);
        afterTheRelease.IsWriting.ShouldBeFalse();
    }

    /// <summary>
    /// Steps 1 and 2 of the deadlock: A at QoS 2 is written and fills the window, and B at QoS 2 is taken and waits,
    /// with nothing being written.
    /// </summary>
    /// <param name="harness">
    /// The harness.
    /// </param>
    /// <param name="connection">
    /// The connection, connected with Receive Maximum 1.
    /// </param>
    /// <param name="server">
    /// The server's side of the connection.
    /// </param>
    private static void FillTheWindowAndWait(ServerHarness harness, TestConnection connection,
        MqttWireConnection server)
    {
        server.TrySend(s_topic, "A"u8.ToArray(), MqttQoS.ExactlyOnce).ShouldBe(MqttSendResult.Taken);
        harness.AssertBounds();
        connection.TakeWritten().ShouldBe(ServerPackets.Delivery(MqttQoS.ExactlyOnce, packetIdentifier: 1, "A"u8));
        server.TrySend(s_topic, "B"u8.ToArray(), MqttQoS.ExactlyOnce).ShouldBe(MqttSendResult.Taken);
        harness.AssertBounds();
        connection.TakeWritten().ShouldBeEmpty();
        ConnectionCounts counts = server.GetCounts();
        counts.WaitingDeliveries.ShouldBe(1);
        counts.IsWriting.ShouldBeFalse();
    }

    /// <summary>
    /// Steps 3 and 4 of the deadlock: the client's PUBREC for A, then its PUBCOMP.
    /// </summary>
    /// <param name="connection">
    /// The connection, with A waiting for its PUBREC and B waiting for the window.
    /// </param>
    /// <returns>
    /// What the server wrote meanwhile: the PUBREL for A and, once the window has room, B.
    /// </returns>
    private static byte[] FinishA(TestConnection connection)
    {
        connection.Deliver(ServerPackets.PubRec(1));
        connection.Deliver(ServerPackets.PubComp(1));
        return connection.TakeWritten();
    }
}
