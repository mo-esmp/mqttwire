using BenchmarkDotNet.Attributes;
using MqttWire.Codec;

namespace MqttWire.Microbench;

/// <summary>
/// Measures what the server's send path asks of the codec for one delivery to one recipient: the size of the
/// PUBLISH, to decide whether the client accepts it; its size again, to rent the buffer it is encoded into; and the
/// encoding itself, whole for a packet of at most 16 KiB and the header alone for a larger one, whose payload is
/// written from the host's memory. The topic is one a host validated once, when it created it; a host that sends
/// one message to a thousand recipients pays this a thousand times. The class is not sealed because BenchmarkDotNet
/// generates a class that derives from it.
/// </summary>
[MemoryDiagnoser]
public class DeliveryEncodingBenchmarks
{
    /// <summary>
    /// The size of the payload, in bytes: small, so that the cost of the topic shows.
    /// </summary>
    private const int PayloadSize = 64;

    /// <summary>
    /// The size of a whole PUBLISH up to which the server copies the payload behind the header; the server's own
    /// constant is not visible from here.
    /// </summary>
    private const int CopyThresholdBytes = 16_384;

    /// <summary>
    /// The packet identifier of the delivery.
    /// </summary>
    private const ushort PacketIdentifier = 7;

    /// <summary>
    /// The number of bytes in each level of the topic, the separator included.
    /// </summary>
    private const int LevelSize = 8;

    /// <summary>
    /// The topic, in UTF-8.
    /// </summary>
    private byte[] _topic = [];

    /// <summary>
    /// The payload.
    /// </summary>
    private byte[] _payload = [];

    /// <summary>
    /// The buffer the PUBLISH is encoded into, as large as an encoding buffer of the server.
    /// </summary>
    private byte[] _buffer = [];

    /// <summary>
    /// Gets or sets the length of the topic, in bytes: a short one and one near the longest a host may send to.
    /// </summary>
    [Params(20, 60_000)]
    public int TopicLength { get; set; }

    /// <summary>
    /// Builds the topic, the payload and the buffer.
    /// </summary>
    [GlobalSetup]
    public void Setup()
    {
        _topic = new byte[TopicLength];
        for (int i = 0; i < _topic.Length; i++)
        {
            _topic[i] = i % LevelSize == LevelSize - 1 ? (byte)'/' : (byte)('a' + (i % LevelSize));
        }

        _topic[^1] = (byte)'z';
        _payload = new byte[PayloadSize];
        _buffer = new byte[65_536];
    }

    /// <summary>
    /// What the server did for one recipient before the codec could be told that a topic is validated, and what any
    /// caller that does not vouch for its topic still pays: the size, the size again, and the encoding, each of
    /// which scans the topic for a wildcard and U+0000.
    /// </summary>
    /// <returns>
    /// The sizes and the bytes written, added up so that nothing is optimised away.
    /// </returns>
    [Benchmark(Baseline = true)]
    public int SizeSizeWrite_Checked()
    {
        // Any identifier stands for the one the delivery will get: it has the same size.
        PublishFields sizing = new() { Topic = _topic, QoS = MqttQoS.AtLeastOnce, PacketIdentifier = 1 };
        int offered = PublishCodec.GetSize(sizing, _payload.Length);
        PublishFields fields = new() { Topic = _topic, QoS = MqttQoS.AtLeastOnce, PacketIdentifier = PacketIdentifier };
        int size = PublishCodec.GetSize(fields, _payload.Length);
        int written = size <= CopyThresholdBytes
            ? PublishCodec.Write(_buffer, fields, _payload)
            : PublishCodec.WriteHeader(_buffer, fields, _payload.Length);
        return offered + size + written;
    }

    /// <summary>
    /// What the server does for one recipient now: the size of the packet from the lengths alone, to decide whether
    /// the client accepts it; then the size and the encoding of a topic it vouches for, which the codec does not
    /// scan. What is left that grows with the topic is the copy of the topic into the packet.
    /// </summary>
    /// <returns>
    /// The sizes and the bytes written, added up so that nothing is optimised away.
    /// </returns>
    [Benchmark]
    public int SizeSizeWrite_Vouched()
    {
        long remainingLength = PublishCodec.MeasureRemainingLength(_topic.Length, MqttQoS.AtLeastOnce, _payload.Length);
        int offered = remainingLength > VariableByteInteger.MaxValue
            ? 0
            : PacketHeader.GetLength((int)remainingLength) + (int)remainingLength;
        PublishFields fields = new()
        {
            Topic = _topic,
            QoS = MqttQoS.AtLeastOnce,
            PacketIdentifier = PacketIdentifier,
            IsTopicValidated = true,
        };
        int size = PublishCodec.GetSize(fields, _payload.Length);
        int written = size <= CopyThresholdBytes
            ? PublishCodec.Write(_buffer, fields, _payload)
            : PublishCodec.WriteHeader(_buffer, fields, _payload.Length);
        return offered + size + written;
    }
}
