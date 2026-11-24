namespace MqttWire.Microbench;

/// <summary>
/// The packets more than one benchmark class reads, spelled once. Each property returns a view of data compiled
/// into the assembly, so reading one allocates nothing; a benchmark that needs an array copies it once, in setup.
/// </summary>
internal static class BenchPackets
{
    /// <summary>
    /// Gets a 26-byte CONNECT with a Receive Maximum property of 1,024 and a client id, user name and password.
    /// </summary>
    public static ReadOnlySpan<byte> Connect =>
    [
        0x10, 0x18, 0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, 0x05, 0xC2, 0x00, 0x0F, 0x03, 0x21, 0x04, 0x00,
        0x00, 0x02, 0x63, 0x31, 0x00, 0x01, 0x75, 0x00, 0x01, 0x70,
    ];

    /// <summary>
    /// Gets a SUBSCRIBE with two topic filters, <c>a/+</c> at QoS 1 and <c>x</c> with the options byte 0x2D.
    /// </summary>
    public static ReadOnlySpan<byte> Subscribe =>
    [
        0x82, 0x0D, 0x00, 0x01, 0x00, 0x00, 0x03, 0x61, 0x2F, 0x2B, 0x01, 0x00, 0x01, 0x78, 0x2D,
    ];
}
