using System.Runtime.InteropServices;

namespace MqttWire.Codec;

/// <summary>
/// What is written into a PUBLISH. MqttWire never writes PUBLISH properties, so the property length is always 0.
/// </summary>
[StructLayout(LayoutKind.Auto)]
internal readonly ref struct PublishFields
{
    /// <summary>
    /// The topic name as UTF-8, encoded once by the caller. The encoder checks that it is not empty, holds no
    /// wildcard and no U+0000 and fits 65,535 bytes. It does not check that the bytes are well-formed UTF-8: that is
    /// the duty of whoever makes the topic bytes, met once per topic rather than once per packet written.
    /// </summary>
    public ReadOnlySpan<byte> Topic { get; init; }

    /// <summary>
    /// Whether the caller vouches that <see cref="Topic"/> holds no wildcard and no U+0000: it checked those very
    /// bytes when it made them, owns them, and has not changed them since. The encoder then leaves out its scan of
    /// the topic, the one check whose cost grows with the topic's length, which every sizing and writing call
    /// otherwise repeats; a debug build still asserts it. Every check that reads no topic byte stays: an empty
    /// topic, one over 65,535 bytes, the QoS, the identifier, DUP and the lengths still throw. The default is
    /// <see langword="false"/>: the encoder scans the topic on every call. Only the server's deliveries set it,
    /// for a topic its <c>MqttTopic</c> validated once.
    /// </summary>
    public bool IsTopicValidated { get; init; }

    /// <summary>
    /// The quality of service; the default is QoS 0.
    /// </summary>
    public MqttQoS QoS { get; init; }

    /// <summary>
    /// The retain flag.
    /// </summary>
    public bool Retain { get; init; }

    /// <summary>
    /// The DUP flag, written as given at QoS 1 and 2. It must be clear at QoS 0 (MQTT-3.3.1-2): the encoder
    /// refuses it there, though the decoder accepts it on input.
    /// </summary>
    public bool Dup { get; init; }

    /// <summary>
    /// The packet identifier: 0 at QoS 0, from 1 to 65,535 at QoS 1 and 2; the encoder refuses anything else.
    /// </summary>
    public ushort PacketIdentifier { get; init; }
}
