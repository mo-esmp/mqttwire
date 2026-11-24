using System.Buffers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using MQTTnet.Adapter;
using MQTTnet.Formatter;
using MQTTnet.Packets;
using MQTTnet.Protocol;
using MqttWire.Codec;

namespace MqttWire.Microbench;

/// <summary>
/// Compares MqttWire's codec with MQTTnet's MQTT 5 formatter, packet by packet: each category times the same bytes
/// through both, with MqttWire as the baseline, so the Ratio column reads how many times MqttWire's time MQTTnet
/// takes. Both sides get their cheapest steady-state use: MqttWire encodes into a preallocated buffer, and MQTTnet
/// reuses one formatter and one buffer writer, as its channel adapter does for a connection. The two do not hand
/// back the same thing, and the numbers should be read with that in mind: MqttWire decodes into a view of the
/// bytes and allocates nothing, while MQTTnet builds a packet object with strings and a copy of the payload; and
/// MqttWire encodes the payload into its buffer, while MQTTnet encodes the header and hands the payload back
/// beside it for the transport to write. The class is not sealed because BenchmarkDotNet generates a class that
/// derives from it.
/// </summary>
[MemoryDiagnoser]
[CategoriesColumn]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
public class CodecComparisonBenchmarks
{
    /// <summary>
    /// The category of decoding a QoS 1 PUBLISH.
    /// </summary>
    private const string DecodePublishCategory = "DecodePublish";

    /// <summary>
    /// The category of encoding a QoS 1 PUBLISH.
    /// </summary>
    private const string EncodePublishCategory = "EncodePublish";

    /// <summary>
    /// The category of decoding a CONNECT.
    /// </summary>
    private const string DecodeConnectCategory = "DecodeConnect";

    /// <summary>
    /// The category of decoding a SUBSCRIBE.
    /// </summary>
    private const string DecodeSubscribeCategory = "DecodeSubscribe";

    /// <summary>
    /// The topic of the benchmark PUBLISH.
    /// </summary>
    private const string Topic = "sensors/room1/temperature";

    /// <summary>
    /// The size of the PUBLISH payload, in bytes.
    /// </summary>
    private const int PayloadSize = 1024;

    /// <summary>
    /// The packet identifier of the benchmark PUBLISH.
    /// </summary>
    private const ushort PublishPacketIdentifier = 7;

    /// <summary>
    /// The initial size of MQTTnet's buffer writer, its own default.
    /// </summary>
    private const int MqttNetBufferSize = 8192;

    /// <summary>
    /// The size above which MQTTnet's buffer writer gives its buffer back after a packet, its own default.
    /// </summary>
    private const int MqttNetMaxBufferSize = 65536;

    /// <summary>
    /// The topic of the benchmark PUBLISH, in UTF-8.
    /// </summary>
    private byte[] _topic = [];

    /// <summary>
    /// The payload of the benchmark PUBLISH.
    /// </summary>
    private byte[] _payload = [];

    /// <summary>
    /// The encoded QoS 1 PUBLISH both decoders read.
    /// </summary>
    private byte[] _publish = [];

    /// <summary>
    /// The buffer MqttWire encodes the PUBLISH into.
    /// </summary>
    private byte[] _destination = [];

    /// <summary>
    /// The PUBLISH as MQTTnet's decoder takes it: the first byte and the body after the fixed header.
    /// </summary>
    private ReceivedMqttPacket _receivedPublish;

    /// <summary>
    /// The CONNECT as MQTTnet's decoder takes it.
    /// </summary>
    private ReceivedMqttPacket _receivedConnect;

    /// <summary>
    /// The SUBSCRIBE as MQTTnet's decoder takes it.
    /// </summary>
    private ReceivedMqttPacket _receivedSubscribe;

    /// <summary>
    /// MQTTnet's MQTT 5 formatter, reused for every call as a connection reuses its own.
    /// </summary>
    private MqttPacketFormatterAdapter _formatter = null!;

    /// <summary>
    /// The PUBLISH that MQTTnet encodes, built once: the same packet as <see cref="_publish"/>.
    /// </summary>
    private MqttPublishPacket _netPublish = null!;

    /// <summary>
    /// Builds every input once, before any benchmark runs, and checks that MQTTnet encodes the PUBLISH to the same
    /// size as MqttWire does, so that both encode the same packet.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// The two libraries encode the benchmark PUBLISH to different sizes.
    /// </exception>
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

        _receivedPublish = CreateReceived(_publish);
        _receivedConnect = CreateReceived(BenchPackets.Connect.ToArray());
        _receivedSubscribe = CreateReceived(BenchPackets.Subscribe.ToArray());

        _formatter = new MqttPacketFormatterAdapter(MqttProtocolVersion.V500, new MqttBufferWriter(MqttNetBufferSize, MqttNetMaxBufferSize));
        _netPublish = new MqttPublishPacket
        {
            Topic = Topic,
            QualityOfServiceLevel = MqttQualityOfServiceLevel.AtLeastOnce,
            PacketIdentifier = PublishPacketIdentifier,
            Payload = new ReadOnlySequence<byte>(_payload),
        };

        int netSize = EncodePublishMqttNet();
        if (netSize != _publish.Length)
        {
            throw new InvalidOperationException($"MQTTnet encodes the PUBLISH to {netSize} bytes and MqttWire to {_publish.Length}.");
        }
    }

    /// <summary>
    /// Decodes a QoS 1 PUBLISH with a 1 KiB payload with MqttWire, into a view of the bytes.
    /// </summary>
    /// <returns>
    /// The reason code, the topic length and the packet identifier, so the work cannot be discarded.
    /// </returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory(DecodePublishCategory)]
    public int DecodePublishMqttWire()
    {
        MqttReasonCode reason = PublishCodec.TryDecode(_publish, PacketSource.Client, out PublishPacket publish);

        return (int)reason + publish.Topic.Length + publish.PacketIdentifier;
    }

    /// <summary>
    /// Decodes the same PUBLISH with MQTTnet, into a packet object with the topic as a string.
    /// </summary>
    /// <returns>
    /// The decoded packet.
    /// </returns>
    [Benchmark]
    [BenchmarkCategory(DecodePublishCategory)]
    public MqttPacket DecodePublishMqttNet() => _formatter.Decode(_receivedPublish);

    /// <summary>
    /// Encodes the QoS 1 PUBLISH with MqttWire, the payload included, into a preallocated buffer.
    /// </summary>
    /// <returns>
    /// The number of bytes written.
    /// </returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory(EncodePublishCategory)]
    public int EncodePublishMqttWire() => PublishCodec.Write(_destination, CreateFields(_topic), _payload);

    /// <summary>
    /// Encodes the same PUBLISH with MQTTnet: the header into its buffer writer, with the payload handed back
    /// beside it; then lets the writer give back a buffer that grew, as MQTTnet does after every packet it sends.
    /// </summary>
    /// <returns>
    /// The size of the whole packet, payload included.
    /// </returns>
    [Benchmark]
    [BenchmarkCategory(EncodePublishCategory)]
    public int EncodePublishMqttNet()
    {
        int length = _formatter.Encode(_netPublish).Length;
        _formatter.Cleanup();

        return length;
    }

    /// <summary>
    /// Decodes the 26-byte CONNECT with MqttWire and reads its Receive Maximum.
    /// </summary>
    /// <returns>
    /// The reason code and the Receive Maximum, so the work cannot be discarded.
    /// </returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory(DecodeConnectCategory)]
    public int DecodeConnectMqttWire()
    {
        MqttReasonCode reason = ConnectCodec.TryDecode(BenchPackets.Connect, out ConnectPacket connect);
        connect.Properties.TryGetInteger(PropertyId.ReceiveMaximum, out uint receiveMaximum);

        return (int)reason + (int)receiveMaximum;
    }

    /// <summary>
    /// Decodes the same CONNECT with MQTTnet, which reads every property into the packet object.
    /// </summary>
    /// <returns>
    /// The decoded packet.
    /// </returns>
    [Benchmark]
    [BenchmarkCategory(DecodeConnectCategory)]
    public MqttPacket DecodeConnectMqttNet() => _formatter.Decode(_receivedConnect);

    /// <summary>
    /// Decodes the two-filter SUBSCRIBE with MqttWire and enumerates its subscriptions.
    /// </summary>
    /// <returns>
    /// The reason code plus the summed topic filter lengths, so the work cannot be discarded.
    /// </returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory(DecodeSubscribeCategory)]
    public int DecodeSubscribeMqttWire()
    {
        MqttReasonCode reason = SubscribeCodec.TryDecode(BenchPackets.Subscribe, out SubscribePacket subscribe);
        int total = (int)reason;

        foreach (Subscription subscription in subscribe.GetSubscriptions())
        {
            total += subscription.TopicFilter.Length;
        }

        return total;
    }

    /// <summary>
    /// Decodes the same SUBSCRIBE with MQTTnet, which builds a list of topic filter objects.
    /// </summary>
    /// <returns>
    /// The decoded packet.
    /// </returns>
    [Benchmark]
    [BenchmarkCategory(DecodeSubscribeCategory)]
    public MqttPacket DecodeSubscribeMqttNet() => _formatter.Decode(_receivedSubscribe);

    /// <summary>
    /// Builds the fields of the benchmark PUBLISH.
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
    /// Splits a framed packet as MQTTnet's channel adapter does once it has read the fixed header: the first byte,
    /// and the body after the fixed header. Done once, in setup, because MqttWire's decoders read the fixed header
    /// themselves and MQTTnet's do not; so MqttWire's times include that work and MQTTnet's leave it out.
    /// </summary>
    /// <param name="packet">
    /// The whole packet.
    /// </param>
    /// <returns>
    /// The packet as MQTTnet's decoder takes it.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// The bytes are not one whole packet.
    /// </exception>
    private static ReceivedMqttPacket CreateReceived(byte[] packet)
    {
        if (PacketHeader.TryReadPacket(packet, out PacketHeader header) != OperationStatus.Done || header.TotalLength != packet.Length)
        {
            throw new InvalidOperationException("The benchmark bytes are not one whole packet.");
        }

        ArraySegment<byte> body = new(packet, header.HeaderLength, packet.Length - header.HeaderLength);
        return new ReceivedMqttPacket(packet[0], body, packet.Length);
    }
}
