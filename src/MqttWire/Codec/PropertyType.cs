namespace MqttWire.Codec;

/// <summary>
/// The wire type of a property value.
/// </summary>
internal enum PropertyType : byte
{
    /// <summary>
    /// One byte.
    /// </summary>
    Byte,

    /// <summary>
    /// Two-byte big-endian integer.
    /// </summary>
    UInt16,

    /// <summary>
    /// Four-byte big-endian integer.
    /// </summary>
    UInt32,

    /// <summary>
    /// Variable byte integer.
    /// </summary>
    VariableByteInteger,

    /// <summary>
    /// Length-prefixed UTF-8 string.
    /// </summary>
    String,

    /// <summary>
    /// Length-prefixed binary data.
    /// </summary>
    Binary,

    /// <summary>
    /// Two length-prefixed UTF-8 strings: a name and a value.
    /// </summary>
    StringPair,
}
