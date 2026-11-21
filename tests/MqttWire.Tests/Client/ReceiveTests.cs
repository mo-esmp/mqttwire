using MqttWire.Client;
using MqttWire.Tests.Codec;

namespace MqttWire.Tests.Client;

/// <summary>
/// Tests receiving at QoS 0, 1 and 2: what is handed to <c>OnMessage</c>, the acknowledgements the client sends,
/// and the faults of the server's packets that end the connection.
/// </summary>
public sealed class ReceiveTests
{
    /// <summary>
    /// A message as the handler saw it, copied out of the receive buffer.
    /// </summary>
    /// <param name="Topic">
    /// The topic.
    /// </param>
    /// <param name="Payload">
    /// The payload.
    /// </param>
    /// <param name="QoS">
    /// The QoS.
    /// </param>
    private readonly record struct Received(string Topic, string Payload, MqttQoS QoS);

    /// <summary>
    /// Proves a QoS 0 PUBLISH is handed on with its topic and an empty payload, and nothing is written.
    /// </summary>
    [Fact]
    public async Task Read_PublishQoS0_HandsOnTopicAndPayload()
    {
        // Arrange
        List<Received> received = [];
        ClientHarness harness = await ConnectAsync(received);

        // Act
        harness.Link.Deliver(KnownPackets.PublishQoS0);

        // Assert
        received.ShouldBe([new Received("t", string.Empty, MqttQoS.AtMostOnce)]);
        harness.Link.TakeWritten().ShouldBeEmpty();
    }

    /// <summary>
    /// Proves a QoS 1 PUBLISH is handed on, then answered with PUBACK.
    /// </summary>
    [Fact]
    public async Task Read_PublishQoS1_HandsOnThenSendsPubAck()
    {
        // Arrange
        List<Received> received = [];
        ClientHarness harness = await ConnectAsync(received);

        // Act
        harness.Link.Deliver(KnownPackets.PublishQoS1);

        // Assert
        received.ShouldBe([new Received("a/b", "hi", MqttQoS.AtLeastOnce)]);
        harness.NextPacket().ShouldBe(KnownPackets.PubAck);
    }

    /// <summary>
    /// Proves a QoS 2 PUBLISH is handed on once and answered with PUBREC.
    /// </summary>
    [Fact]
    public async Task Read_PublishQoS2_HandsOnOnceAndSendsPubRec()
    {
        // Arrange
        List<Received> received = [];
        ClientHarness harness = await ConnectAsync(received);

        // Act
        harness.Link.Deliver(ClientPackets.PublishQoS2Id7);

        // Assert
        received.ShouldBe([new Received("a/b", "hi", MqttQoS.ExactlyOnce)]);
        harness.NextPacket().ShouldBe(ClientPackets.PubRec7);
    }

    /// <summary>
    /// Proves a QoS 2 PUBLISH repeated before its PUBREL, with DUP and without, is answered with PUBREC each time
    /// and handed on only once.
    /// </summary>
    [Fact]
    public async Task Read_PublishQoS2Repeated_AnswersPubRecWithoutHandingOnAgain()
    {
        // Arrange
        List<Received> received = [];
        ClientHarness harness = await ConnectAsync(received);

        // Act
        harness.Link.Deliver(ClientPackets.PublishQoS2Id7);
        harness.Link.Deliver(ClientPackets.PublishQoS2Id7Dup);
        harness.Link.Deliver(ClientPackets.PublishQoS2Id7);

        // Assert
        received.Count.ShouldBe(1);
        harness.NextPacket().ShouldBe(ClientPackets.PubRec7);
        harness.NextPacket().ShouldBe(ClientPackets.PubRec7);
        harness.NextPacket().ShouldBe(ClientPackets.PubRec7);
    }

    /// <summary>
    /// Proves a PUBREL for a remembered identifier is answered with PUBCOMP and forgets it, so a PUBLISH that
    /// reuses the identifier is handed on again.
    /// </summary>
    [Fact]
    public async Task Read_PubRelForRemembered_SendsPubCompAndForgets()
    {
        // Arrange
        List<Received> received = [];
        ClientHarness harness = await ConnectAsync(received);
        harness.Link.Deliver(ClientPackets.PublishQoS2Id7);
        harness.NextPacket();

        // Act
        harness.Link.Deliver(ClientPackets.PubRel7);
        byte[] pubComp = harness.NextPacket();
        harness.Link.Deliver(ClientPackets.PublishQoS2Id7);

        // Assert
        pubComp.ShouldBe(ClientPackets.PubComp7);
        received.Count.ShouldBe(2);
        harness.NextPacket().ShouldBe(ClientPackets.PubRec7);
    }

    /// <summary>
    /// Proves a PUBREL for an identifier the client does not remember is answered with PUBCOMP 0x92.
    /// </summary>
    [Fact]
    public async Task Read_PubRelForUnknown_SendsPubCompNotFound()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync([]);

        // Act
        harness.Link.Deliver(ClientPackets.PubRel7);

        // Assert
        harness.NextPacket().ShouldBe(ClientPackets.PubCompNotFound7);
        harness.Client.IsConnected.ShouldBeTrue();
    }

    /// <summary>
    /// Proves a PUBLISH with a Topic Alias, which the client never allowed, ends the connection with DISCONNECT
    /// 0x94 and is not handed on.
    /// </summary>
    [Fact]
    public async Task Read_PublishWithTopicAlias_SendsDisconnect94()
    {
        // Arrange
        List<Received> received = [];
        ClientHarness harness = await ConnectAsync(received);

        // Act
        harness.Link.Deliver(ClientPackets.PublishWithTopicAlias);
        byte[] disconnect = harness.NextPacket();
        harness.Link.EndFromPeer();

        // Assert
        disconnect.ShouldBe(ClientPackets.DisconnectTopicAliasInvalid);
        received.ShouldBeEmpty();
        harness.Closed.ShouldBe([new MqttClientCloseInfo(MqttClientCloseReason.ProtocolError, MqttReasonCode.TopicAliasInvalid, null, null)]);
    }

    /// <summary>
    /// Proves the 1,025th unreleased QoS 2 identifier ends the connection with DISCONNECT 0x93, while the first
    /// 1,024 are each answered with PUBREC.
    /// </summary>
    [Fact]
    public async Task Read_1025UnreleasedQoS2_SendsDisconnect93()
    {
        // Arrange
        List<Received> received = [];
        ClientHarness harness = await ConnectAsync(received);

        // Act
        for (int id = 1; id <= ClientLimits.MaxIncomingQoS2; id++)
        {
            harness.Link.Deliver(PublishQoS2(id));
            harness.NextPacket();
        }

        harness.Link.Deliver(PublishQoS2(ClientLimits.MaxIncomingQoS2 + 1));
        byte[] disconnect = harness.NextPacket();
        harness.Link.EndFromPeer();

        // Assert
        received.Count.ShouldBe(ClientLimits.MaxIncomingQoS2);
        disconnect.ShouldBe(ClientPackets.DisconnectReceiveMaximumExceeded);
        harness.Closed.ShouldBe([new MqttClientCloseInfo(MqttClientCloseReason.ProtocolError, MqttReasonCode.ReceiveMaximumExceeded, null, null)]);
    }

    /// <summary>
    /// Proves a PUBLISH cut at any byte, however the link splits it, is handed on as the same message.
    /// </summary>
    [Fact]
    public async Task Read_PublishSplitAtEveryByte_HandsOnTheSameMessage()
    {
        // Arrange
        byte[] packet = KnownPackets.PublishTwoByteLength;
        string payload = new('x', 128);

        for (int cut = 1; cut < packet.Length; cut++)
        {
            List<Received> received = [];
            ClientHarness harness = await ConnectAsync(received);

            // Act
            harness.Link.Deliver(packet.AsSpan(0, cut));
            bool handedOnEarly = received.Count != 0;
            harness.Link.Deliver(packet.AsSpan(cut));

            // Assert
            handedOnEarly.ShouldBeFalse();
            received.ShouldBe([new Received("a/b", payload, MqttQoS.AtLeastOnce)]);
            harness.NextPacket().ShouldBe(KnownPackets.PubAck);
        }
    }

    /// <summary>
    /// Proves several PUBLISHes arriving in one read are each handed on, in order, with their acknowledgements in
    /// the same order.
    /// </summary>
    [Fact]
    public async Task Read_SeveralPacketsInOneRead_HandsOnEachInOrder()
    {
        // Arrange
        List<Received> received = [];
        ClientHarness harness = await ConnectAsync(received);
        byte[] together = [.. KnownPackets.PublishQoS0, .. KnownPackets.PublishQoS1, .. ClientPackets.PublishQoS2Id7];

        // Act
        harness.Link.Deliver(together);

        // Assert
        received.ShouldBe(
        [
            new Received("t", string.Empty, MqttQoS.AtMostOnce),
            new Received("a/b", "hi", MqttQoS.AtLeastOnce),
            new Received("a/b", "hi", MqttQoS.ExactlyOnce),
        ]);
        harness.NextPacket().ShouldBe(KnownPackets.PubAck);
        harness.NextPacket().ShouldBe(ClientPackets.PubRec7);
    }

    /// <summary>
    /// Proves the payload a handler sees lies inside the pooled receive buffer, so no copy is made.
    /// </summary>
    [Fact]
    public async Task Read_PayloadInOneBuffer_IsAViewNotACopy()
    {
        // Arrange
        CountingPool pool = new();
        bool payloadIsView = false;
        bool topicIsView = false;
        ClientHarness harness = ClientHarness.Create(
            pool: pool,
            onMessage: (in message) =>
            {
                payloadIsView = pool.IsViewIntoRentedBuffer(message.Payload);
                topicIsView = pool.IsViewIntoRentedBuffer(message.Topic);
            });
        await harness.ConnectAsync(ClientPackets.ConnAckAccepted);
        harness.NextPacket();

        // Act
        harness.Link.Deliver(KnownPackets.PublishQoS1);

        // Assert
        payloadIsView.ShouldBeTrue();
        topicIsView.ShouldBeTrue();
    }

    /// <summary>
    /// Proves an <c>OnMessage</c> that throws ends the connection with DISCONNECT 0x83, reports the exception, and
    /// leaves the message without a PUBACK.
    /// </summary>
    [Fact]
    public async Task Read_OnMessageThrows_SendsDisconnect83AndNoPubAck()
    {
        // Arrange
        InvalidOperationException thrown = new("handler failed");
        ClientHarness harness = ClientHarness.Create(onMessage: (in _) => throw thrown);
        await harness.ConnectAsync(ClientPackets.ConnAckAccepted);
        harness.NextPacket();

        // Act
        harness.Link.Deliver(KnownPackets.PublishQoS1);
        byte[] first = harness.NextPacket();
        byte[] rest = harness.Link.TakeWritten();
        harness.Link.EndFromPeer();

        // Assert
        first.ShouldBe(ClientPackets.DisconnectImplementationSpecific);
        rest.ShouldBeEmpty();
        harness.Closed.ShouldBe([new MqttClientCloseInfo(MqttClientCloseReason.MessageHandlerThrew, MqttReasonCode.ImplementationSpecificError, null, thrown)]);
    }

    /// <summary>
    /// Proves an <c>OnMessage</c> that publishes at QoS 1 on the same client and does not await it cannot block
    /// the read loop, and that the publish completes when its PUBACK arrives.
    /// </summary>
    [Fact]
    public async Task Read_OnMessagePublishesQoS1_DoesNotDeadlock()
    {
        // Arrange
        MqttWireClient? client = null;
        List<ValueTask<MqttRequestResult>> publishes = [];
        ClientHarness harness = ClientHarness.Create(
            onMessage: (in _) =>
            {
                if (client is not null)
                {
                    publishes.Add(client.PublishAsync("a/b", "hi"u8.ToArray(), MqttQoS.AtLeastOnce));
                }
            });
        client = harness.Client;
        await harness.ConnectAsync(ClientPackets.ConnAckAccepted);
        harness.NextPacket();

        // Act
        harness.Link.Deliver(KnownPackets.PublishQoS1);
        byte[] publish = harness.NextPacket();
        byte[] pubAck = harness.NextPacket();
        harness.Link.Deliver(ClientPackets.PubAck1);
        MqttRequestResult result = await publishes[0];

        // Assert
        publish.ShouldBe(ClientPackets.PublishQoS1);
        pubAck.ShouldBe(KnownPackets.PubAck);
        result.ShouldBe(new MqttRequestResult(MqttRequestStatus.Completed, MqttReasonCode.Success));
    }

    /// <summary>
    /// Proves a topic that is not valid UTF-8 ends the connection with DISCONNECT 0x81.
    /// </summary>
    [Fact]
    public async Task Read_TopicNotValidUtf8_SendsDisconnect81()
    {
        // Arrange
        List<Received> received = [];
        ClientHarness harness = await ConnectAsync(received);

        // Act
        harness.Link.Deliver(ClientPackets.PublishTopicNotUtf8);
        byte[] disconnect = harness.NextPacket();
        harness.Link.EndFromPeer();

        // Assert
        disconnect.ShouldBe(ClientPackets.DisconnectMalformedPacket);
        received.ShouldBeEmpty();
        harness.Closed.ShouldBe([new MqttClientCloseInfo(MqttClientCloseReason.ProtocolError, MqttReasonCode.MalformedPacket, null, null)]);
    }

    /// <summary>
    /// Proves the table of unreleased identifiers refuses a duplicate and a new one when full, and forgets what it
    /// is told to.
    /// </summary>
    [Fact]
    public void TryAdd_DuplicateOrFull_ReturnsFalseAndKeepsTheCount()
    {
        // Arrange
        IncomingQoS2 table = new();

        // Act
        bool first = table.TryAdd(7);
        bool duplicate = table.TryAdd(7);
        bool removed = table.Remove(7);
        bool removedAgain = table.Remove(7);
        for (ushort id = 1; id <= ClientLimits.MaxIncomingQoS2; id++)
        {
            table.TryAdd(id);
        }

        bool overflow = table.TryAdd(ClientLimits.MaxIncomingQoS2 + 1);

        // Assert
        first.ShouldBeTrue();
        duplicate.ShouldBeFalse();
        removed.ShouldBeTrue();
        removedAgain.ShouldBeFalse();
        overflow.ShouldBeFalse();
        table.Count.ShouldBe(ClientLimits.MaxIncomingQoS2);
    }

    /// <summary>
    /// Builds a PUBLISH at QoS 2 with topic "a/b", payload "hi" and the given identifier.
    /// </summary>
    /// <param name="id">
    /// The packet identifier.
    /// </param>
    /// <returns>
    /// The packet's bytes.
    /// </returns>
    private static byte[] PublishQoS2(int id) => [0x34, 0x0A, 0x00, 0x03, 0x61, 0x2F, 0x62, (byte)(id >> 8), (byte)id, 0x00, 0x68, 0x69];

    /// <summary>
    /// Creates a client whose handler copies each message into a list, and connects it, taking the CONNECT it
    /// wrote.
    /// </summary>
    /// <param name="received">
    /// Where the handler records what it saw.
    /// </param>
    /// <returns>
    /// The harness around the connected client.
    /// </returns>
    private static async Task<ClientHarness> ConnectAsync(List<Received> received)
    {
        ClientHarness harness = ClientHarness.Create(
            onMessage: (in message) => received.Add(new Received(message.GetTopic(), System.Text.Encoding.UTF8.GetString(message.Payload), message.QoS)));
        await harness.ConnectAsync(ClientPackets.ConnAckAccepted);
        harness.NextPacket();
        return harness;
    }
}
