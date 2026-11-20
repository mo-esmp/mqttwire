using System.Runtime.InteropServices;

namespace MqttWire.Codec;

/// <summary>
/// The name and value of a user property, as UTF-8 bytes inside the block.
/// </summary>
[StructLayout(LayoutKind.Auto)]
internal readonly ref struct Utf8Pair
{
    /// <summary>
    /// Creates a pair.
    /// </summary>
    /// <param name="name">
    /// The UTF-8 name.
    /// </param>
    /// <param name="value">
    /// The UTF-8 value.
    /// </param>
    public Utf8Pair(ReadOnlySpan<byte> name, ReadOnlySpan<byte> value)
    {
        Name = name;
        Value = value;
    }

    /// <summary>
    /// The UTF-8 name, without its length prefix.
    /// </summary>
    public ReadOnlySpan<byte> Name { get; }

    /// <summary>
    /// The UTF-8 value, without its length prefix.
    /// </summary>
    public ReadOnlySpan<byte> Value { get; }
}
