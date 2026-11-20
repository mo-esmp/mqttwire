using System.Buffers;
using System.Text;
using MqttWire.Codec;

namespace MqttWire.Tests.Codec;

/// <summary>
/// Proves decoding and encoding allocate nothing on the managed heap. Each measurement is a difference of two
/// readings of the allocated bytes of the current thread, taken after one warm-up pass; everything the measured
/// work needs is built before the first reading, and no assertion runs between the readings.
/// </summary>
public sealed class AllocationTests
{
    /// <summary>
    /// Added to a checksum for every packet that did not decode as success, so a checksum below this is all success.
    /// A decoder's error reason code is added with it, so a test can tell which errors were seen.
    /// </summary>
    private const long FailureWeight = 1L << 40;

    /// <summary>
    /// Every property id, so every accessor is tried on every packet.
    /// </summary>
    private static readonly PropertyId[] s_propertyIds = Enum.GetValues<PropertyId>();

    /// <summary>
    /// A CONNACK with a reason string and a user property.
    /// </summary>
    private static readonly byte[] s_connAckWithUserProperty = BuildConnAckWithUserProperty();

    /// <summary>
    /// A PUBLISH with a payload format indicator and a user property.
    /// </summary>
    private static readonly byte[] s_publishWithProperties = Hex.Parse("30 0F 00 01 74 09 26 00 01 61 00 01 62 01 01 68 69");

    /// <summary>
    /// A SUBSCRIBE with a user property.
    /// </summary>
    private static readonly byte[] s_subscribeWithUserProperty = Hex.Parse("82 10 00 01 07 26 00 01 61 00 01 62 00 03 61 2F 2B 01");

    /// <summary>
    /// An UNSUBSCRIBE with a user property.
    /// </summary>
    private static readonly byte[] s_unsubscribeWithUserProperty = Hex.Parse("A2 0F 00 02 07 26 00 01 61 00 01 62 00 03 61 2F 2B");

    /// <summary>
    /// The packets a server reads: every known packet a client sends, and some with properties.
    /// </summary>
    private static readonly byte[][] s_serverReads =
    [
        KnownPackets.Connect,
        KnownPackets.ConnectMinimal,
        KnownPackets.ConnectWithWill,
        KnownPackets.PublishQoS0,
        KnownPackets.PublishQoS1,
        KnownPackets.PublishQoS2,
        s_publishWithProperties,
        KnownPackets.PubAck,
        KnownPackets.PubRecRefused,
        KnownPackets.PubRel,
        KnownPackets.PubCompNotFound,
        KnownPackets.PubAckWithReason,
        KnownPackets.Subscribe,
        KnownPackets.SubscribeTwo,
        s_subscribeWithUserProperty,
        KnownPackets.Unsubscribe,
        s_unsubscribeWithUserProperty,
        KnownPackets.PingReq,
        KnownPackets.Disconnect,
        KnownPackets.DisconnectTakenOver,
        KnownPackets.DisconnectWithReason,
    ];

    /// <summary>
    /// The packets a client reads: every known packet a server sends, and some with properties.
    /// </summary>
    private static readonly byte[][] s_clientReads =
    [
        KnownPackets.ConnAck,
        s_connAckWithUserProperty,
        KnownPackets.PublishQoS0,
        KnownPackets.PublishQoS1,
        KnownPackets.PublishQoS2,
        s_publishWithProperties,
        KnownPackets.PubAck,
        KnownPackets.PubRecRefused,
        KnownPackets.PubRel,
        KnownPackets.PubCompNotFound,
        KnownPackets.PubAckWithReason,
        KnownPackets.SubAck,
        KnownPackets.UnsubAck,
        KnownPackets.PingResp,
        KnownPackets.Disconnect,
        KnownPackets.DisconnectTakenOver,
        KnownPackets.DisconnectWithReason,
    ];

    /// <summary>
    /// Framed packets a server reads that its decoders refuse: bad UTF-8, a property the packet does not allow, an
    /// identifier of 0, a reserved bit and left-over bytes.
    /// </summary>
    private static readonly byte[][] s_serverMalformed =
    [
        Hex.Parse("30 04 00 01 C3 00"),
        Hex.Parse("30 09 00 01 74 05 11 00 00 00 01"),
        Hex.Parse("82 07 00 00 00 00 01 78 01"),
        Hex.Parse("82 07 00 01 00 00 01 78 40"),
        Hex.Parse("E0 03 8E 00 00"),
        Hex.Parse("C0 01 00"),
    ];

    /// <summary>
    /// The reason code each packet of <see cref="s_serverMalformed"/> decodes to, in the same order.
    /// </summary>
    private static readonly MqttReasonCode[] s_serverMalformedResults =
    [
        MqttReasonCode.MalformedPacket,
        MqttReasonCode.MalformedPacket,
        MqttReasonCode.ProtocolError,
        MqttReasonCode.MalformedPacket,
        MqttReasonCode.MalformedPacket,
        MqttReasonCode.MalformedPacket,
    ];

    /// <summary>
    /// Framed packets a client reads that its decoders refuse: bad UTF-8, a property the packet does not allow, an
    /// identifier of 0, a reserved bit and left-over bytes.
    /// </summary>
    private static readonly byte[][] s_clientMalformed =
    [
        Hex.Parse("30 04 00 01 C3 00"),
        Hex.Parse("20 08 00 00 05 18 00 00 00 01"),
        Hex.Parse("90 04 00 00 00 01"),
        Hex.Parse("20 03 02 00 00"),
        Hex.Parse("20 04 00 00 00 00"),
        Hex.Parse("D0 01 00"),
    ];

    /// <summary>
    /// The reason code each packet of <see cref="s_clientMalformed"/> decodes to, in the same order.
    /// </summary>
    private static readonly MqttReasonCode[] s_clientMalformedResults =
    [
        MqttReasonCode.MalformedPacket,
        MqttReasonCode.MalformedPacket,
        MqttReasonCode.ProtocolError,
        MqttReasonCode.MalformedPacket,
        MqttReasonCode.MalformedPacket,
        MqttReasonCode.MalformedPacket,
    ];

    /// <summary>
    /// The client id of the encoded CONNECT.
    /// </summary>
    private static readonly string s_clientId = "client-1";

    /// <summary>
    /// The user name of the encoded CONNECT.
    /// </summary>
    private static readonly string s_userName = "user";

    /// <summary>
    /// The password of the encoded CONNECT.
    /// </summary>
    private static readonly byte[] s_password = [1, 2, 3, 4];

    /// <summary>
    /// A reason string, written into the packets that can carry one.
    /// </summary>
    private static readonly string s_reasonString = "because";

    /// <summary>
    /// The user properties of the encoded CONNACK.
    /// </summary>
    private static readonly MqttWireUserProperty[] s_userProperties = [new("a", "b"), new("name", "value")];

    /// <summary>
    /// The topic of the encoded PUBLISH.
    /// </summary>
    private static readonly byte[] s_topic = Encoding.UTF8.GetBytes("a/b/c");

    /// <summary>
    /// The payload of the encoded PUBLISH.
    /// </summary>
    private static readonly byte[] s_payload = new byte[100];

    /// <summary>
    /// The topic filter of the encoded SUBSCRIBE and UNSUBSCRIBE.
    /// </summary>
    private static readonly byte[] s_filter = Encoding.UTF8.GetBytes("a/+/c");

    /// <summary>
    /// The reason codes of the encoded SUBACK and UNSUBACK.
    /// </summary>
    private static readonly MqttReasonCode[] s_reasonCodes = [MqttReasonCode.Success, MqttReasonCode.GrantedQoS1, MqttReasonCode.NotAuthorized];

    /// <summary>
    /// Proves framing and decoding every known packet, reading every property, user property, subscription and
    /// topic filter, allocates nothing.
    /// </summary>
    [Fact]
    public void TryDecode_EveryKnownPacket_AllocatesNothing()
    {
        // Arrange
        long expected = DecodeEvery(s_serverReads, Reader.Server) + DecodeEvery(s_clientReads, Reader.Client);

        // Act
        long before = GC.GetAllocatedBytesForCurrentThread();
        long checksum = DecodeEvery(s_serverReads, Reader.Server) + DecodeEvery(s_clientReads, Reader.Client);
        long after = GC.GetAllocatedBytesForCurrentThread();

        // Assert
        (after - before).ShouldBe(0);
        checksum.ShouldBe(expected);
        checksum.ShouldBeLessThan(FailureWeight);
    }

    /// <summary>
    /// Proves framing and decoding framed but malformed packets, for each reader, allocates nothing, and that the
    /// decoders answered each with its expected error.
    /// </summary>
    [Fact]
    public void TryDecode_MalformedPackets_AllocatesNothing()
    {
        // Arrange
        long expected = DecodeEvery(s_serverMalformed, Reader.Server) + DecodeEvery(s_clientMalformed, Reader.Client);

        // Act
        long before = GC.GetAllocatedBytesForCurrentThread();
        long checksum = DecodeEvery(s_serverMalformed, Reader.Server) + DecodeEvery(s_clientMalformed, Reader.Client);
        long after = GC.GetAllocatedBytesForCurrentThread();

        // Assert
        (after - before).ShouldBe(0);
        checksum.ShouldBe(expected);
        for (int i = 0; i < s_serverMalformed.Length; i++)
        {
            DecodeEvery([s_serverMalformed[i]], Reader.Server).ShouldBe(Failure(s_serverMalformedResults[i]));
        }

        for (int i = 0; i < s_clientMalformed.Length; i++)
        {
            DecodeEvery([s_clientMalformed[i]], Reader.Client).ShouldBe(Failure(s_clientMalformedResults[i]));
        }
    }

    /// <summary>
    /// Proves sizing and writing every packet the encoders write, into one preallocated buffer, allocates nothing.
    /// </summary>
    [Fact]
    public void Write_EveryPacketTheEncodersWrite_AllocatesNothing()
    {
        // Arrange
        byte[] destination = new byte[4096];
        long expected = EncodeEvery(destination);

        // Act
        long before = GC.GetAllocatedBytesForCurrentThread();
        long total = EncodeEvery(destination);
        long after = GC.GetAllocatedBytesForCurrentThread();

        // Assert
        (after - before).ShouldBe(0);
        total.ShouldBe(expected);
        total.ShouldBeGreaterThan(0);
    }

    /// <summary>
    /// Builds a CONNACK that carries a reason string and user properties, before any measurement.
    /// </summary>
    /// <returns>
    /// The bytes of the packet.
    /// </returns>
    private static byte[] BuildConnAckWithUserProperty()
    {
        ConnAckFields fields = new()
        {
            ReasonCode = MqttReasonCode.Success,
            ReasonString = "because",
            UserProperties = new MqttWireUserProperty[] { new("a", "b"), new("name", "value") },
        };
        byte[] packet = new byte[ConnAckCodec.GetSize(fields)];
        ConnAckCodec.Write(packet, fields);
        return packet;
    }

    /// <summary>
    /// Frames and decodes every packet, reading everything the decoded packet offers.
    /// </summary>
    /// <param name="packets">
    /// One framed packet per element.
    /// </param>
    /// <param name="reader">
    /// Who reads them.
    /// </param>
    /// <returns>
    /// A checksum of what was read, plus <see cref="FailureWeight"/> for each packet that did not decode as success;
    /// it keeps the reads from being optimised away.
    /// </returns>
    private static long DecodeEvery(byte[][] packets, Reader reader)
    {
        long sum = 0;
        for (int i = 0; i < packets.Length; i++)
        {
            byte[] bytes = packets[i];
            if (PacketHeader.TryReadPacket(bytes, out PacketHeader header) != OperationStatus.Done)
            {
                sum += FailureWeight;
                continue;
            }

            sum += DecodeAndRead(bytes.AsSpan(0, header.TotalLength), header.Type, reader);
        }

        return sum;
    }

    /// <summary>
    /// Decodes one framed packet as the reader would and reads every property of the result.
    /// </summary>
    /// <param name="packet">
    /// Exactly one framed packet.
    /// </param>
    /// <param name="type">
    /// Its type.
    /// </param>
    /// <param name="reader">
    /// Who reads it.
    /// </param>
    /// <returns>
    /// A checksum of what was read; <see cref="Failure"/> of the reason code when the decoder did not answer
    /// success; <see cref="FailureWeight"/> alone when the reader never reads the type.
    /// </returns>
    private static long DecodeAndRead(ReadOnlySpan<byte> packet, PacketType type, Reader reader)
    {
        bool server = reader == Reader.Server;
        long sum = 0;
        switch (type)
        {
            case PacketType.Connect when server:
                MqttReasonCode connectResult = ConnectCodec.TryDecode(packet, out ConnectPacket connect);
                if (connectResult != MqttReasonCode.Success)
                {
                    return Failure(connectResult);
                }

                sum += connect.ClientId.Length + connect.UserName.Length + connect.Password.Length + connect.KeepAliveSeconds;
                sum += (connect.HasUserName ? 1 : 0) + (connect.HasPassword ? 1 : 0) + (connect.CleanStart ? 1 : 0) + (connect.HasWill ? 1 : 0);
                return sum + ReadProperties(connect.Properties);
            case PacketType.ConnAck when !server:
                MqttReasonCode connAckResult = ConnAckCodec.TryDecode(packet, out ConnAckPacket connAck);
                if (connAckResult != MqttReasonCode.Success)
                {
                    return Failure(connAckResult);
                }

                sum += (connAck.SessionPresent ? 1 : 0) + (int)connAck.ReasonCode;
                return sum + ReadProperties(connAck.Properties);
            case PacketType.Publish:
                MqttReasonCode publishResult = PublishCodec.TryDecode(packet, server ? PacketSource.Client : PacketSource.Server, out PublishPacket publish);
                if (publishResult != MqttReasonCode.Success)
                {
                    return Failure(publishResult);
                }

                sum += (int)publish.QoS + (publish.Dup ? 1 : 0) + (publish.Retain ? 1 : 0) + publish.Topic.Length + publish.PacketIdentifier + publish.Payload.Length + (publish.HasTopicAlias ? 1 : 0);
                return sum + ReadProperties(publish.Properties);
            case PacketType.PubAck or PacketType.PubRec or PacketType.PubRel or PacketType.PubComp:
                MqttReasonCode ackResult = AckCodec.TryDecode(packet, out AckPacket ack);
                if (ackResult != MqttReasonCode.Success)
                {
                    return Failure(ackResult);
                }

                sum += (int)ack.Type + ack.PacketIdentifier + (int)ack.ReasonCode;
                return sum + ReadProperties(ack.Properties);
            case PacketType.Subscribe when server:
                MqttReasonCode subscribeResult = SubscribeCodec.TryDecode(packet, out SubscribePacket subscribe);
                if (subscribeResult != MqttReasonCode.Success)
                {
                    return Failure(subscribeResult);
                }

                sum += subscribe.PacketIdentifier + subscribe.Count + ReadProperties(subscribe.Properties);
                foreach (Subscription subscription in subscribe.GetSubscriptions())
                {
                    sum += subscription.TopicFilter.Length + (int)subscription.MaximumQoS + (subscription.NoLocal ? 1 : 0)
                        + (subscription.RetainAsPublished ? 1 : 0) + subscription.RetainHandling;
                }

                return sum;
            case PacketType.SubAck when !server:
                MqttReasonCode subAckResult = SubAckCodec.TryDecode(packet, out SubAckPacket subAck);
                if (subAckResult != MqttReasonCode.Success)
                {
                    return Failure(subAckResult);
                }

                sum += subAck.PacketIdentifier + ReadProperties(subAck.Properties);
                for (int i = 0; i < subAck.ReasonCodes.Length; i++)
                {
                    sum += subAck.ReasonCodes[i];
                }

                return sum;
            case PacketType.Unsubscribe when server:
                MqttReasonCode unsubscribeResult = UnsubscribeCodec.TryDecode(packet, out UnsubscribePacket unsubscribe);
                if (unsubscribeResult != MqttReasonCode.Success)
                {
                    return Failure(unsubscribeResult);
                }

                sum += unsubscribe.PacketIdentifier + unsubscribe.Count + ReadProperties(unsubscribe.Properties);
                foreach (ReadOnlySpan<byte> filter in unsubscribe.GetTopicFilters())
                {
                    sum += filter.Length;
                }

                return sum;
            case PacketType.UnsubAck when !server:
                MqttReasonCode unsubAckResult = UnsubAckCodec.TryDecode(packet, out UnsubAckPacket unsubAck);
                if (unsubAckResult != MqttReasonCode.Success)
                {
                    return Failure(unsubAckResult);
                }

                sum += unsubAck.PacketIdentifier + ReadProperties(unsubAck.Properties);
                for (int i = 0; i < unsubAck.ReasonCodes.Length; i++)
                {
                    sum += unsubAck.ReasonCodes[i];
                }

                return sum;
            case PacketType.PingReq when server:
            case PacketType.PingResp when !server:
                MqttReasonCode pingResult = PingCodec.TryDecode(packet);
                return pingResult == MqttReasonCode.Success ? 1 : Failure(pingResult);
            case PacketType.Disconnect:
                MqttReasonCode disconnectResult = DisconnectCodec.TryDecode(packet, out DisconnectPacket disconnect);
                if (disconnectResult != MqttReasonCode.Success)
                {
                    return Failure(disconnectResult);
                }

                return (int)disconnect.ReasonCode + ReadProperties(disconnect.Properties);
            default:
                return FailureWeight;
        }
    }

    /// <summary>
    /// Gets what a packet the decoder refused adds to a checksum: <see cref="FailureWeight"/> plus the reason code.
    /// </summary>
    /// <param name="reasonCode">
    /// The error the decoder answered.
    /// </param>
    /// <returns>
    /// The weight of one failure carrying its reason code.
    /// </returns>
    private static long Failure(MqttReasonCode reasonCode) => FailureWeight + (long)reasonCode;

    /// <summary>
    /// Tries every property accessor on a block and enumerates its user properties.
    /// </summary>
    /// <param name="block">
    /// The block to read.
    /// </param>
    /// <returns>
    /// A checksum of everything read.
    /// </returns>
    private static long ReadProperties(PropertyBlock block)
    {
        long sum = 0;
        for (int i = 0; i < s_propertyIds.Length; i++)
        {
            PropertyId id = s_propertyIds[i];
            sum += block.Contains(id) ? 1 : 0;
            if (block.TryGetInteger(id, out uint number))
            {
                sum += number;
            }

            if (block.TryGetBytes(id, out ReadOnlySpan<byte> bytes))
            {
                sum += bytes.Length;
            }
        }

        foreach (Utf8Pair pair in block.UserProperties)
        {
            sum += pair.Name.Length + pair.Value.Length;
        }

        return sum;
    }

    /// <summary>
    /// Sizes and writes every packet the encoders write.
    /// </summary>
    /// <param name="destination">
    /// The buffer every packet is written into, from its start.
    /// </param>
    /// <returns>
    /// The sum of every size and every number of bytes written.
    /// </returns>
    private static long EncodeEvery(byte[] destination)
    {
        long total = 0;

        ConnectFields connect = new()
        {
            ClientId = s_clientId,
            UserName = s_userName,
            Password = s_password,
            KeepAliveSeconds = 15,
            ReceiveMaximum = 1024,
        };
        total += ConnectCodec.GetSize(connect) + ConnectCodec.Write(destination, connect);

        ConnAckFields connAck = new()
        {
            ReasonCode = MqttReasonCode.Success,
            SessionExpiryInterval = 60,
            ReceiveMaximum = 100,
            MaximumQoS = 1,
            SubscriptionIdentifiersAvailable = true,
            ServerKeepAlive = 30,
            ReasonString = s_reasonString,
            UserProperties = s_userProperties,
        };
        total += ConnAckCodec.GetSize(connAck) + ConnAckCodec.Write(destination, connAck);

        for (int qos = 0; qos <= 2; qos++)
        {
            PublishFields publish = new()
            {
                Topic = s_topic,
                QoS = (MqttQoS)qos,
                Retain = qos == 1,
                Dup = qos == 2,
                PacketIdentifier = (ushort)(qos == 0 ? 0 : 7),
            };
            total += PublishCodec.GetHeaderSize(publish, s_payload.Length)
                + PublishCodec.WriteHeader(destination, publish, s_payload.Length)
                + PublishCodec.GetSize(publish, s_payload.Length)
                + PublishCodec.Write(destination, publish, s_payload);
        }

        total += WriteAck(destination, PacketType.PubAck, MqttReasonCode.QuotaExceeded);
        total += WriteAck(destination, PacketType.PubRec, MqttReasonCode.Success);
        total += WriteAck(destination, PacketType.PubRel, MqttReasonCode.PacketIdentifierNotFound);
        total += WriteAck(destination, PacketType.PubComp, MqttReasonCode.Success);

        total += SubscribeCodec.GetSize(s_filter) + SubscribeCodec.Write(destination, 1, s_filter, MqttQoS.AtLeastOnce);
        total += SubAckCodec.GetSize(s_reasonCodes.Length, s_reasonString) + SubAckCodec.Write(destination, 1, s_reasonCodes, s_reasonString);
        total += UnsubscribeCodec.GetSize(s_filter) + UnsubscribeCodec.Write(destination, 2, s_filter);
        total += UnsubAckCodec.GetSize(s_reasonCodes.Length, s_reasonString) + UnsubAckCodec.Write(destination, 2, s_reasonCodes, s_reasonString);

        total += DisconnectCodec.GetSize(MqttReasonCode.SessionTakenOver, s_reasonString) + DisconnectCodec.Write(destination, MqttReasonCode.SessionTakenOver, s_reasonString);

        PingCodec.PingReq.CopyTo(destination);
        total += PingCodec.PingReq.Length;
        PingCodec.PingResp.CopyTo(destination);
        total += PingCodec.PingResp.Length;

        return total;
    }

    /// <summary>
    /// Sizes and writes one acknowledgement with a reason string.
    /// </summary>
    /// <param name="destination">
    /// Where to write.
    /// </param>
    /// <param name="type">
    /// PUBACK, PUBREC, PUBREL or PUBCOMP.
    /// </param>
    /// <param name="reasonCode">
    /// The reason code.
    /// </param>
    /// <returns>
    /// The size plus the number of bytes written.
    /// </returns>
    private static long WriteAck(byte[] destination, PacketType type, MqttReasonCode reasonCode)
        => AckCodec.GetSize(reasonCode, s_reasonString) + AckCodec.Write(destination, type, 10, reasonCode, s_reasonString);
}
