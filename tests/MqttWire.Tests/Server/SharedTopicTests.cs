using System.Buffers.Binary;
using Microsoft.Extensions.Options;
using MqttWire.Server;
using MqttWire.TestHost;
using MqttWire.Tests.Codec;

namespace MqttWire.Tests.Server;

/// <summary>
/// Tests of shared topics through the host (design 6.10 and 6.13 item 8): several connections on one topic through
/// the small host of <see cref="ExactTopicHandler"/>, over in-memory links. One message reaches every member, and a
/// member that stops reading is evicted without delaying the others.
/// </summary>
public sealed class SharedTopicTests
{
    /// <summary>
    /// The topic every member of these tests subscribes to.
    /// </summary>
    private const string GroupTopic = "g/1";

    /// <summary>
    /// Proves that one message published to a topic ten connections subscribed to reaches each of the ten, the
    /// publisher included, and that every connection is idle again once the deliveries are acknowledged.
    /// </summary>
    [Fact]
    public void Publish_TenConnectionsOnOneTopic_ReachesAll()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        UseExactTopics(harness);
        List<TestConnection> members = [];
        for (int i = 1; i <= 10; i++)
        {
            members.Add(Join(harness, $"c{i}", MqttQoS.AtLeastOnce));
        }

        byte[] delivery = ServerPackets.BuildPublish(MqttQoS.AtLeastOnce, "g/1"u8, packetIdentifier: 1, properties: [], "hi"u8);

        // Act
        members[0].Deliver(ServerPackets.BuildPublish(MqttQoS.AtLeastOnce, "g/1"u8, packetIdentifier: 5, properties: [], "hi"u8));

        // Assert: the publisher has its own copy, sent from inside OnPublish, and then its PUBACK.
        members[0].TakeWritten().ShouldBe([.. delivery, .. ServerPackets.PubAck(5)]);
        for (int i = 1; i < members.Count; i++)
        {
            members[i].TakeWritten().ShouldBe(delivery, $"member {i + 1}");
        }

        foreach (TestConnection member in members)
        {
            member.Deliver(ServerPackets.PubAck(1));
            member.Connection.ShouldNotBeNull().GetCounts().ShouldBe(ServerHarness.Idle);
        }

        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a member that stops reading delays nobody: with its link holding every write, each of 2,000
    /// messages has reached the other nine members when its publish returns, with no step of the clock; the member
    /// is evicted by the message that would make 1,001 wait for it, and the nine go on receiving after that.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Publish_OneMemberStopsReading_TheOthersKeepReceivingAndItIsDisconnected()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        ExactTopicHandler handler = UseExactTopics(harness);
        List<TestConnection> members = [];
        for (int i = 1; i <= 10; i++)
        {
            members.Add(Join(harness, $"c{i}", MqttQoS.AtMostOnce));
        }

        TestConnection stalled = members[9];
        stalled.Link.HoldWrites();
        byte[] payload = new byte[1_024];
        int evictedBy = 0;

        // Act
        for (int i = 1; i <= 2_000; i++)
        {
            // Every message is its own, so one that came twice or out of order would not match.
            BinaryPrimitives.WriteInt32BigEndian(payload, i);
            byte[] publish = ServerPackets.BuildPublish(MqttQoS.AtMostOnce, "g/1"u8, packetIdentifier: 0, properties: [], payload);
            members[0].Deliver(publish);

            // Assert, after every publish: at QoS 0 and with no properties, a delivery is the bytes of the publish.
            for (int m = 0; m < 9; m++)
            {
                members[m].TakeWritten().AsSpan().SequenceEqual(publish).ShouldBeTrue($"publish {i}, member {m + 1}");
            }

            if (evictedBy == 0 && stalled.Link.IsAborted)
            {
                evictedBy = i;
            }
        }

        stalled.CompleteHeldWrites(new IOException("The socket was aborted."));
        await stalled.RunEnded();

        // Assert: the first message is the write the link holds, 1,000 more wait, and the next one evicts.
        evictedBy.ShouldBe(1_002);
        stalled.TakeWritten().ShouldBeEmpty();
        harness.ClosedCalls().ShouldBe(["OnClosed c10 OutgoingLimitPassed"]);
        handler.CountSubscribers(GroupTopic).ShouldBe(9);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a message goes to a member at the lower of the publish's QoS and the QoS the member was granted:
    /// a member granted QoS 0 gets a QoS 2 publish at QoS 0.
    /// </summary>
    [Fact]
    public void Publish_LowerOfTheTwoQoS_IsUsed()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        UseExactTopics(harness);
        TestConnection member = Join(harness, "c1", MqttQoS.AtMostOnce);
        TestConnection publisher = harness.Connect("c2");

        // Act
        publisher.Deliver(ServerPackets.BuildPublish(MqttQoS.ExactlyOnce, "g/1"u8, packetIdentifier: 7, properties: [], "hi"u8));

        // Assert
        member.TakeWritten().ShouldBe(ServerPackets.BuildPublish(MqttQoS.AtMostOnce, "g/1"u8, packetIdentifier: 0, properties: [], "hi"u8));
        publisher.TakeWritten().ShouldBe(ServerPackets.PubRec(7));
        member.Connection.ShouldNotBeNull().GetCounts().ShouldBe(ServerHarness.Idle);
    }

    /// <summary>
    /// Proves that a member whose connection ended is taken out of the topic's table, and that a later publish goes
    /// to the members that are left and throws nothing.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Closed_Member_IsRemovedFromTheTable()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        ExactTopicHandler handler = UseExactTopics(harness);
        TestConnection first = Join(harness, "c1", MqttQoS.AtLeastOnce);
        TestConnection second = Join(harness, "c2", MqttQoS.AtLeastOnce);
        int before = handler.CountSubscribers(GroupTopic);
        byte[] publish = ServerPackets.BuildPublish(MqttQoS.AtMostOnce, "g/1"u8, packetIdentifier: 0, properties: [], "hi"u8);

        // Act: a DISCONNECT with the reason code "normal disconnection".
        second.Deliver(Hex.Parse("E0 00"));
        await second.RunEnded();
        int after = handler.CountSubscribers(GroupTopic);
        first.Deliver(publish);

        // Assert
        before.ShouldBe(2);
        after.ShouldBe(1);
        first.TakeWritten().ShouldBe(publish);
        second.TakeWritten().ShouldBeEmpty();
        harness.ClosedCalls().ShouldBe(["OnClosed c2 ClientDisconnected"]);
    }

    /// <summary>
    /// Makes the harness's recording handler pass every call on to a new <see cref="ExactTopicHandler"/>, so that the
    /// server is driven by the host of the interop tests while the harness still records the calls.
    /// </summary>
    /// <param name="harness">
    /// The harness, before any connection is opened.
    /// </param>
    /// <returns>
    /// The handler the calls are passed on to.
    /// </returns>
    private static ExactTopicHandler UseExactTopics(ServerHarness harness)
    {
        ExactTopicHandler handler = new(Options.Create(harness.Server.Options));
        harness.Handler.Connect = handler.OnConnectAsync;
        harness.Handler.Admit = handler.OnAdmit;
        harness.Handler.Subscribe = handler.OnSubscribe;
        harness.Handler.Unsubscribe = handler.OnUnsubscribe;
        harness.Handler.Publish = handler.OnPublish;
        harness.Handler.Closed = handler.OnClosed;
        return handler;
    }

    /// <summary>
    /// Connects a client and subscribes it to <see cref="GroupTopic"/>, asserting that the server granted the QoS
    /// asked for.
    /// </summary>
    /// <param name="harness">
    /// The harness.
    /// </param>
    /// <param name="clientId">
    /// The client id.
    /// </param>
    /// <param name="qos">
    /// The QoS to subscribe at.
    /// </param>
    /// <returns>
    /// The connection, subscribed, with nothing written left on its link.
    /// </returns>
    private static TestConnection Join(ServerHarness harness, string clientId, MqttQoS qos)
    {
        TestConnection connection = harness.Connect(clientId);
        connection.Deliver(ServerPackets.BuildSubscribe(packetIdentifier: 1, properties: [], [(GroupTopic, (byte)qos)]));
        connection.TakeWritten().ShouldBe([0x90, 0x04, 0x00, 0x01, 0x00, (byte)qos]);
        return connection;
    }
}
