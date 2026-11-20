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
    /// The quality of service; the default is QoS 0.
    /// </summary>
    public MqttQoS QoS { get; init; }

    /// <summary>
    /// The retain flag.
    /// </summary>
    public bool Retain { get; init; }

    /// <summary>
    /// The DUP flag, written as given.
    /// </summary>
    public bool Dup { get; init; }

    /// <summary>
    /// The packet identifier: 0 at QoS 0, from 1 to 65,535 at QoS 1 and 2; the encoder refuses anything else.
    /// </summary>
    public ushort PacketIdentifier { get; init; }
}
