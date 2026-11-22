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
    /// Proves by identity that the records object goes back to the server's pool when the last exchange ends: once a
    /// QoS 2 exchange from the client has ended, the next object the pool hands out is the very one the connection
    /// used, and it holds no record.
    /// </summary>
    [Fact]
    public void PublishQoS2_ExchangeEnded_TheRecordsObjectIsBackInThePool()
    {
        // Arrange
        ServerPools defaults = new();
        CountingObjectPool<ExchangeRecords> records = new(defaults.Records);
        ServerHarness harness = ServerHarness.Create(pools: new ServerPools(defaults.Waiting, records));
        TestConnection connection = harness.Connect();
        connection.Deliver(ClientPackets.PublishQoS2);
        ExchangeRecords used = records.LastHandedOut.ShouldNotBeNull();

        // Act
        connection.Deliver(ClientPackets.PubRel1);
        ExchangeRecords next = harness.Server.Pools.Records.Get();

        // Assert
        connection.TakeWritten().ShouldBe([.. ClientPackets.PubRec1, .. ClientPackets.PubComp1]);
        next.ShouldBeSameAs(used);
        next.IsEmpty.ShouldBeTrue();
        next.IncomingCount.ShouldBe(0);
        next.DeliveryCount.ShouldBe(0);
        records.Distinct.ShouldBe(1);
        harness.Server.Pools.Records.Return(next);
        records.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that an <c>OnPublish</c> that throws at QoS 2 closes the connection with DISCONNECT 0x83 and
    /// <see cref="MqttCloseReason.HandlerThrew"/>, answers no PUBREC, and remembers nothing: the identifier is not
    /// remembered at any moment up to the close, so a PUBREL for it would be answered 0x92.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task PublishQoS2_HostThrows_ClosesWith0x83AndRemembersNothing()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        harness.Handler.Publish = static (_, in _) => throw new InvalidOperationException("The host's bug.");
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();

        // Every buffer that goes back is seen here, the DISCONNECT's among them: its write has ended and the Closing
        // step has not yet dropped what the connection holds, so a record remembered before the close would show.
        List<int> rememberedAtEachReturn = [];
        harness.Pool.Returning = _ => rememberedAtEachReturn.Add(server.GetCounts().IncomingRecords);

        // Act
        connection.Deliver(ClientPackets.PublishQoS2);
        await connection.Run;

        // Assert
        connection.TakeWritten().ShouldBe(Hex.Parse("E0 01 83"));
        rememberedAtEachReturn.ShouldNotBeEmpty();
        rememberedAtEachReturn.ShouldAllBe(static count => count == 0);
        harness.Handler.Calls.ShouldContain("OnClosed c1 HandlerThrew");
        server.GetCounts().HoldsRecords.ShouldBeFalse();
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a host that closes the connection from inside <c>OnPublish</c> at QoS 2 gets no PUBREC written and
    /// no record kept: the DISCONNECT of its close is the last and only packet.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task PublishQoS2_HostClosesInsideOnPublish_WritesNoPubRecAndKeepsNoRecord()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        harness.Handler.Publish = static (publisher, in _) =>
        {
            publisher.Close(MqttReasonCode.AdministrativeAction);
            return MqttPublishResult.Accepted;
        };
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();

        // Act
        connection.Deliver(ClientPackets.PublishQoS2);
        await connection.Run;

        // Assert
        connection.TakeWritten().ShouldBe(ServerPackets.Disconnect(MqttReasonCode.AdministrativeAction));
        server.GetCounts().IncomingRecords.ShouldBe(0);
        server.GetCounts().HoldsRecords.ShouldBeFalse();
        harness.Handler.Calls.ShouldContain("OnClosed c1 ClosedByHost");
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a connection lost while QoS 2 identifiers are remembered gives its records object back to the
    /// server's pool once, with no fault, and that the connection holds nothing once its run has ended.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task PublishQoS2_ConnectionLostWithIdentifiersRemembered_ReturnsTheRecordsOnce()
    {
        // Arrange
        ServerPools defaults = new();
        CountingObjectPool<ExchangeRecords> records = new(defaults.Records);
        ServerHarness harness = ServerHarness.Create(pools: new ServerPools(defaults.Waiting, records));
        TestConnection connection = harness.Connect();
        MqttWireConnection server = connection.Connection.ShouldNotBeNull();
        connection.Deliver(ClientPackets.PublishQoS2Id7);
        connection.Deliver(ClientPackets.PublishQoS2);
        ConnectionCounts remembered = server.GetCounts();

        // Act
        connection.Link.EndFromPeer();
        await connection.Run;

        // Assert
        remembered.IncomingRecords.ShouldBe(2);
        records.Outstanding.ShouldBe(0);
        records.Distinct.ShouldBe(1);
        harness.Log.Entries.ShouldNotContain(static entry => entry.Id.Id == 20);
        server.GetCounts().ShouldBe(new ConnectionCounts(IncomingRecords: 0, DeliveryRecords: 0, IncomingInFlight: 0,
            HoldsRecords: false, WaitingDeliveries: 0, WaitingControlPackets: 0, OutgoingBytes: 0, IsWriting: false,
            HoldsReadBuffer: false));
        harness.Handler.Calls.ShouldContain("OnClosed c1 ConnectionLost");
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a PUBLISH refused as too large counts against the receive maximum like any other: with 256 PUBACKs
    /// waiting behind a held write, a 257th publish at QoS 1 or 2 whose payload is over the limit, read whole or
    /// skipped, closes the connection with <see cref="MqttCloseReason.ReceiveMaximumPassed"/> (0x93) before it is
    /// answered; the write being held, the link is aborted and no DISCONNECT is written.
    /// </summary>
    /// <param name="qos">
    /// The QoS of the 257th publish.
    /// </param>
    /// <param name="remainingLength">
    /// Its Remaining Length: 2,000 is read whole and judged, 16,384 is skipped, against a payload limit of 1,024.
    /// </param>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Theory]
    [InlineData(MqttQoS.AtLeastOnce, 2_000)]
    [InlineData(MqttQoS.ExactlyOnce, 2_000)]
    [InlineData(MqttQoS.AtLeastOnce, 16 * 1_024)]
    [InlineData(MqttQoS.ExactlyOnce, 16 * 1_024)]
    public async Task PublishTooLarge_The257thUnansweredBehindAHeldWrite_ClosesWith0x93(MqttQoS qos,
        int remainingLength)
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create(static options => options.MaxPublishPayloadBytes = 1_024);
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
        connection.Deliver(ServerPackets.BuildPublishOfLength(qos, remainingLength, packetIdentifier: 257));
        bool aborted = connection.Link.IsAborted;
        connection.CompleteHeldWrites();
        await connection.Run;

        // Assert
        full.IncomingInFlight.ShouldBe(256);
        aborted.ShouldBeTrue();
        connection.TakeWritten().ShouldBe(ClientPackets.PingResp);
        harness.Handler.Calls.ShouldContain("OnClosed c1 ReceiveMaximumPassed");
        PublishCalls(harness).Count.ShouldBe(256);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a remembered QoS 2 identifier published again does not start its acknowledgement deadline again:
    /// remembered at 0 s and repeated at 20 s, the connection is still open at the sweep at 28 s and is closed with
    /// DISCONNECT 0x82 at the sweep at 30 s, not 50 s.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task PublishQoS2_RepeatedAtTwentySeconds_DoesNotRestartTheAcknowledgementDeadline()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        connection.Deliver(ClientPackets.PublishQoS2);
        connection.TakeWritten().ShouldBe(ClientPackets.PubRec1);

        // Act
        harness.Advance(TimeSpan.FromSeconds(20));
        connection.Deliver(ClientPackets.PublishQoS2);
        byte[] repeated = connection.TakeWritten();
        harness.Advance(TimeSpan.FromSeconds(8));
        byte[] writtenAt28 = connection.TakeWritten();
        bool openAt28 = !connection.Run.IsCompleted && !connection.Link.IsAborted;
        harness.Advance(TimeSpan.FromSeconds(2));
        byte[] writtenAt30 = connection.TakeWritten();
        connection.Link.EndFromPeer();
        await connection.Run;

        // Assert
        repeated.ShouldBe(ClientPackets.PubRec1);
        writtenAt28.ShouldBeEmpty();
        openAt28.ShouldBeTrue();
        writtenAt30.ShouldBe(ServerPackets.Disconnect(MqttReasonCode.ProtocolError));
        harness.Handler.Calls.ShouldContain("OnClosed c1 AcknowledgementTimedOut");
        PublishCalls(harness).Count.ShouldBe(1);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that the acknowledgement deadline is per record, measured from its own stamp: a QoS 2 publish remembered
    /// at 1 s is still open at the sweep at 30 s, 29 s on, and is closed with DISCONNECT 0x82 at the sweep at 32 s.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task PubRel_NeverComesForARecordEnteredAtOneSecond_ClosesAtTheSweepAt32Seconds()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection connection = harness.Connect();
        harness.Advance(TimeSpan.FromSeconds(1));
        connection.Deliver(ClientPackets.PublishQoS2);
        connection.TakeWritten().ShouldBe(ClientPackets.PubRec1);

        // Act
        harness.Advance(TimeSpan.FromSeconds(29));
        byte[] writtenAt30 = connection.TakeWritten();
        bool openAt30 = !connection.Run.IsCompleted && !connection.Link.IsAborted;
        harness.Advance(TimeSpan.FromSeconds(2));
        byte[] writtenAt32 = connection.TakeWritten();
        connection.Link.EndFromPeer();
        await connection.Run;

        // Assert
        writtenAt30.ShouldBeEmpty();
        openAt30.ShouldBeTrue();
        writtenAt32.ShouldBe(ServerPackets.Disconnect(MqttReasonCode.ProtocolError));
        harness.Handler.Calls.ShouldContain("OnClosed c1 AcknowledgementTimedOut");
        harness.Pool.Outstanding.ShouldBe(0);
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
