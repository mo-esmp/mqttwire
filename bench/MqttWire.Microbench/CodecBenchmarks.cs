using System.Buffers;
using System.Text;
using BenchmarkDotNet.Attributes;
using MqttWire.Codec;

namespace MqttWire.Microbench;

/// <summary>
/// Measures the speed of the codec's hot paths: decoding and encoding PUBLISH, decoding CONNECT and SUBSCRIBE, and
/// framing packets out of a stream; and of encoding a CONNACK that carries user properties. The inputs are built in
/// <see cref="Setup"/>, so only the codec call is timed.
/// The Allocated column is reported for information; that the codec allocates nothing is asserted by the test
/// project, not here. The class is not sealed because BenchmarkDotNet generates a class that derives from it.
/// </summary>
[MemoryDiagnoser]
public class CodecBenchmarks
{
    /// <summary>
    /// The number of packets in <see cref="_stream"/>.
    /// </summary>
    private const int StreamPacketCount = 100;

    /// <summary>
    /// The size of the PUBLISH payload, in bytes.
    /// </summary>
    private const int PayloadSize = 1024;

    /// <summary>
    /// The packet identifier of the benchmark PUBLISH.
    /// </summary>
    private const ushort PublishPacketIdentifier = 7;

    /// <summary>
    /// The size of the long topic, in bytes.
    /// </summary>
    private const int LongTopicSize = 256;

    /// <summary>
    /// The number of bytes in each level of the long topic, the separator included.
    /// </summary>
    private const int LongTopicLevelSize = 8;

    /// <summary>
    /// The number of user properties in the benchmark CONNACK.
    /// </summary>
    private const int UserPropertyCount = 8;

    /// <summary>
    /// How often the value of a user property repeats its 9 characters, which are 15 bytes in UTF-8.
    /// </summary>
    private const int UserPropertyValueRepeats = 20;

    /// <summary>
    /// A 26-byte CONNECT with a Receive Maximum property of 1,024 and a client id, user name and password.
    /// </summary>
    private static readonly byte[] s_connect =
    [
        0x10, 0x18, 0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, 0x05, 0xC2, 0x00, 0x0F, 0x03, 0x21, 0x04, 0x00,
        0x00, 0x02, 0x63, 0x31, 0x00, 0x01, 0x75, 0x00, 0x01, 0x70,
    ];

    /// <summary>
    /// A SUBSCRIBE with two topic filters.
    /// </summary>
    private static readonly byte[] s_subscribe =
    [
        0x82, 0x0D, 0x00, 0x01, 0x00, 0x00, 0x03, 0x61, 0x2F, 0x2B, 0x01, 0x00, 0x01, 0x78, 0x2D,
    ];

    /// <summary>
    /// The topic of the benchmark PUBLISH, in UTF-8.
    /// </summary>
    private byte[] _topic = [];

    /// <summary>
    /// The payload of the benchmark PUBLISH.
    /// </summary>
    private byte[] _payload = [];

    /// <summary>
    /// The encoded QoS 1 PUBLISH that <see cref="DecodePublishQoS1"/> reads.
    /// </summary>
    private byte[] _publish = [];

    /// <summary>
    /// The buffer that <see cref="EncodePublishQoS1"/> writes into.
    /// </summary>
    private byte[] _destination = [];

    /// <summary>
    /// <see cref="StreamPacketCount"/> small packets, one after the other.
    /// </summary>
    private byte[] _stream = [];

    /// <summary>
    /// A topic of <see cref="LongTopicSize"/> bytes, in UTF-8.
    /// </summary>
    private byte[] _longTopic = [];

    /// <summary>
    /// The buffer the benchmarks with the long topic write into.
    /// </summary>
    private byte[] _longDestination = [];

    /// <summary>
    /// The user properties of the benchmark CONNACK: long names and values with characters of two and three bytes.
    /// </summary>
    private MqttWireUserProperty[] _userProperties = [];

    /// <summary>
    /// The buffer that <see cref="EncodeConnAckUserProperties"/> writes into.
    /// </summary>
    private byte[] _connAckDestination = [];

    /// <summary>
    /// Builds every input once, before any benchmark runs.
    /// </summary>
    [GlobalSetup]
    public void Setup()
    {
        _topic = "sensors/room1/temperature"u8.ToArray();
        _payload = new byte[PayloadSize];
        new Random(1).NextBytes(_payload);

        PublishFields fields = CreateFields(_topic);
        _publish = new byte[PublishCodec.GetSize(fields, _payload.Length)];
        PublishCodec.Write(_publish, fields, _payload);
        _destination = new byte[_publish.Length];

        _longTopic = new byte[LongTopicSize];
        for (int index = 0; index < _longTopic.Length; index++)
        {
            _longTopic[index] = index % LongTopicLevelSize == LongTopicLevelSize - 1 ? (byte)'/' : (byte)'a';
        }

        _longDestination = new byte[PublishCodec.GetSize(CreateFields(_longTopic), _payload.Length)];

        string value = new StringBuilder().Insert(0, "größe-温度-", UserPropertyValueRepeats).ToString();
        _userProperties = new MqttWireUserProperty[UserPropertyCount];
        for (int index = 0; index < _userProperties.Length; index++)
        {
            _userProperties[index] = new MqttWireUserProperty($"schlüssel-{index}-温度", value);
        }

        _connAckDestination = new byte[ConnAckCodec.GetSize(CreateConnAckFields())];

        // PINGREQ (2 bytes) and PUBACK (4 bytes) alternate.
        List<byte> stream = [];

        for (int index = 0; index < StreamPacketCount; index++)
        {
            if (index % 2 == 0)
            {
                stream.AddRange([0xC0, 0x00]);
            }
            else
            {
                stream.AddRange([0x40, 0x02, 0x00, 0x01]);
            }
        }

        _stream = stream.ToArray();
    }

    /// <summary>
    /// Decodes a QoS 1 PUBLISH with a 1 KiB payload.
    /// </summary>
    /// <returns>
    /// The reason code and the topic length, so the work cannot be discarded.
    /// </returns>
    [Benchmark]
    public int DecodePublishQoS1()
    {
        MqttReasonCode reason = PublishCodec.TryDecode(_publish, PacketSource.Client, out PublishPacket publish);

        return (int)reason + publish.Topic.Length + publish.PacketIdentifier;
    }

    /// <summary>
    /// Encodes the same QoS 1 PUBLISH into a preallocated buffer.
    /// </summary>
    /// <returns>
    /// The number of bytes written.
    /// </returns>
    [Benchmark]
    public int EncodePublishQoS1()
    {
        PublishFields fields = CreateFields(_topic);

        return PublishCodec.Write(_destination, fields, _payload);
    }

    /// <summary>
    /// Sizes the same QoS 1 PUBLISH and then writes it into that much of a preallocated buffer, as a caller that
    /// sizes its buffer for each packet does. The fields are checked and the topic scanned twice, once by each
    /// call; the difference from <see cref="EncodePublishQoS1"/> is the cost of the second pass.
    /// </summary>
    /// <returns>
    /// The number of bytes written.
    /// </returns>
    [Benchmark]
    public int SizeAndEncodePublishQoS1()
    {
        PublishFields fields = CreateFields(_topic);
        int size = PublishCodec.GetSize(fields, _payload.Length);

        return PublishCodec.Write(_destination.AsSpan(0, size), fields, _payload);
    }

    /// <summary>
    /// Encodes a QoS 1 PUBLISH with a topic of <see cref="LongTopicSize"/> bytes into a preallocated buffer.
    /// </summary>
    /// <returns>
    /// The number of bytes written.
    /// </returns>
    [Benchmark]
    public int EncodePublishLongTopic()
    {
        PublishFields fields = CreateFields(_longTopic);

        return PublishCodec.Write(_longDestination, fields, _payload);
    }

    /// <summary>
    /// Sizes the PUBLISH with the long topic and then writes it into that much of a preallocated buffer; the
    /// difference from <see cref="EncodePublishLongTopic"/> is the cost of checking and scanning a long topic twice.
    /// </summary>
    /// <returns>
    /// The number of bytes written.
    /// </returns>
    [Benchmark]
    public int SizeAndEncodePublishLongTopic()
    {
        PublishFields fields = CreateFields(_longTopic);
        int size = PublishCodec.GetSize(fields, _payload.Length);

        return PublishCodec.Write(_longDestination.AsSpan(0, size), fields, _payload);
    }

    /// <summary>
    /// Encodes a CONNACK with <see cref="UserPropertyCount"/> user properties, whose names and values are long and
    /// hold characters of two and three bytes, into a preallocated buffer.
    /// </summary>
    /// <returns>
    /// The number of bytes written.
    /// </returns>
    [Benchmark]
    public int EncodeConnAckUserProperties() => ConnAckCodec.Write(_connAckDestination, CreateConnAckFields());

    /// <summary>
    /// Decodes the 26-byte CONNECT and reads its Receive Maximum.
    /// </summary>
    /// <returns>
    /// The reason code and the Receive Maximum, so the work cannot be discarded.
    /// </returns>
    [Benchmark]
    public int DecodeConnect()
    {
        MqttReasonCode reason = ConnectCodec.TryDecode(s_connect, out ConnectPacket connect);
        connect.Properties.TryGetInteger(PropertyId.ReceiveMaximum, out uint receiveMaximum);

        return (int)reason + (int)receiveMaximum;
    }

    /// <summary>
    /// Frames every packet of the stream buffer, one after the other.
    /// </summary>
    /// <returns>
    /// The number of packets found.
    /// </returns>
    [Benchmark]
    public int FrameStream()
    {
        ReadOnlySpan<byte> buffer = _stream;
        int count = 0;

        while (!buffer.IsEmpty && PacketHeader.TryReadPacket(buffer, out PacketHeader header) == OperationStatus.Done)
        {
            buffer = buffer[header.TotalLength..];
            count++;
        }

        return count;
    }

    /// <summary>
    /// Decodes the two-filter SUBSCRIBE and enumerates its subscriptions.
    /// </summary>
    /// <returns>
    /// The reason code plus the summed topic filter lengths, so the work cannot be discarded.
    /// </returns>
    [Benchmark]
    public int DecodeSubscribe()
    {
        MqttReasonCode reason = SubscribeCodec.TryDecode(s_subscribe, out SubscribePacket subscribe);
        int total = (int)reason;

        foreach (Subscription subscription in subscribe.GetSubscriptions())
        {
            total += subscription.TopicFilter.Length;
        }

        return total;
    }

    /// <summary>
    /// Builds the fields of a benchmark PUBLISH for a topic prepared in setup.
    /// </summary>
    /// <param name="topic">
    /// The topic, in UTF-8.
    /// </param>
    /// <returns>
    /// The QoS 1 fields; a ref struct, so it is built inside each benchmark.
    /// </returns>
    private static PublishFields CreateFields(byte[] topic) => new()
    {
        Topic = topic,
        QoS = MqttQoS.AtLeastOnce,
        PacketIdentifier = PublishPacketIdentifier,
    };

    /// <summary>
    /// Builds the fields of the benchmark CONNACK from the user properties prepared in setup.
    /// </summary>
    /// <returns>
    /// The fields of a successful CONNACK; a ref struct, so it is built inside each benchmark.
    /// </returns>
    private ConnAckFields CreateConnAckFields() => new()
    {
        ReasonCode = MqttReasonCode.Success,
        ReceiveMaximum = 256,
        UserProperties = _userProperties,
    };
}
