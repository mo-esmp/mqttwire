using System.Runtime.InteropServices;

namespace MqttWire.Codec;

/// <summary>
/// Walks the user properties of a validated block in wire order, skipping the other properties.
/// </summary>
[StructLayout(LayoutKind.Auto)]
internal ref struct UserPropertyEnumerator
{
    /// <summary>
    /// The reader over the validated block; it only moves forward.
    /// </summary>
    private MqttReader _reader;

    /// <summary>
    /// Creates an enumerator over a validated block.
    /// </summary>
    /// <param name="properties">
    /// The validated properties, without the length prefix.
    /// </param>
    public UserPropertyEnumerator(ReadOnlySpan<byte> properties) => _reader = new MqttReader(properties);

    /// <summary>
    /// The pair found by the last successful <see cref="MoveNext"/>.
    /// </summary>
    public Utf8Pair Current { get; private set; }

    /// <summary>
    /// Returns this enumerator, so the struct works in <c>foreach</c>.
    /// </summary>
    /// <returns>
    /// A copy of this enumerator.
    /// </returns>
    public readonly UserPropertyEnumerator GetEnumerator() => this;

    /// <summary>
    /// Moves to the next user property.
    /// </summary>
    /// <returns>
    /// <see langword="false"/> when there are no more user properties.
    /// </returns>
    public bool MoveNext()
    {
        while (_reader.Remaining > 0)
        {
            _reader.TryReadByte(out byte id);
            if (id == (byte)PropertyId.UserProperty)
            {
                _reader.TryReadBinary(out ReadOnlySpan<byte> name);
                _reader.TryReadBinary(out ReadOnlySpan<byte> value);
                Current = new Utf8Pair(name, value);
                return true;
            }

            PropertyBlock.ReadValidated(ref _reader, PropertyBlock.TypeOf(id), out _, out _);
        }

        return false;
    }
}
