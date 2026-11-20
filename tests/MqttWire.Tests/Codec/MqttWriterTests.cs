using MqttWire.Codec;

namespace MqttWire.Tests.Codec;

/// <summary>
/// Tests the writer, the field size checks and that a failed write writes nothing.
/// </summary>
public sealed class MqttWriterTests
{
    /// <summary>
    /// Proves a string is written as its length then its UTF-8 bytes, matching its measured size.
    /// </summary>
    [Fact]
    public void WriteUtf8String_String_WritesItsLengthThenItsBytes()
    {
        // Arrange
        byte[] destination = new byte[4];

        // Act
        MqttWriter writer = new(destination);
        writer.WriteUtf8String("ü");
        int size = FieldSize.Utf8String("ü");

        // Assert
        writer.Written.ShouldBe(4);
        destination.ShouldBe(Hex.Parse("00 02 C3 BC"));
        size.ShouldBe(4);
    }

    /// <summary>
    /// Proves measuring a string too long for the length prefix throws.
    /// </summary>
    [Fact]
    public void Utf8String_StringOver65535Bytes_ThrowsArgumentException()
    {
        // Arrange
        string value = new('a', 65_536);

        // Act & Assert
        Should.Throw<ArgumentException>(() => FieldSize.Utf8String(value));
    }

    /// <summary>
    /// Proves measuring a string of fewer than 65,536 chars but more than 65,535 UTF-8 bytes throws, naming the
    /// string rather than an internal length.
    /// </summary>
    [Fact]
    public void Utf8String_MultiByteStringOver65535Bytes_ThrowsNamingTheString()
    {
        // Arrange
        string value = new('ü', 32_768);

        // Act
        ArgumentException exception = Should.Throw<ArgumentException>(() => FieldSize.Utf8String(value));

        // Assert
        exception.ParamName.ShouldBe("value");
    }

    /// <summary>
    /// Proves measuring UTF-8 bytes or binary data too long for the length prefix throws.
    /// </summary>
    [Fact]
    public void Utf8StringAndBinary_Over65535Bytes_ThrowsArgumentException()
    {
        // Arrange
        byte[] utf8 = new byte[65_536];

        // Act & Assert
        Should.Throw<ArgumentException>(() => FieldSize.Utf8String(utf8));
        Should.Throw<ArgumentException>(() => FieldSize.Binary(65_536));
    }

    /// <summary>
    /// Proves measuring a string with U+0000 throws.
    /// </summary>
    [Fact]
    public void Utf8String_StringWithNullCharacter_ThrowsArgumentException()
    {
        // Arrange
        string value = "a\0";

        // Act & Assert
        Should.Throw<ArgumentException>(() => FieldSize.Utf8String(value));
    }

    /// <summary>
    /// Proves measuring a string with an unpaired surrogate throws.
    /// </summary>
    [Fact]
    public void Utf8String_StringWithUnpairedSurrogate_ThrowsArgumentException()
    {
        // Arrange
        string value = "a\uD800";

        // Act & Assert
        Should.Throw<ArgumentException>(() => FieldSize.Utf8String(value));
    }

    /// <summary>
    /// Proves writing a string with an unpaired surrogate throws before anything is written.
    /// </summary>
    [Fact]
    public void WriteUtf8String_StringWithUnpairedSurrogate_ThrowsAndWritesNothing()
    {
        // Arrange
        byte[] destination = new byte[16];

        // Act & Assert
        Should.Throw<ArgumentException>(() =>
        {
            MqttWriter writer = new(destination);
            writer.WriteUtf8String("a\uD800");
        });
        destination.ShouldAllBe(b => b == 0);
    }

    /// <summary>
    /// Proves a string of the longest allowed length is written and the reader reads it back.
    /// </summary>
    [Fact]
    public void WriteUtf8String_StringOf65535Bytes_WritesItAndReaderReadsIt()
    {
        // Arrange
        byte[] destination = new byte[2 + 65_535];

        // Act
        MqttWriter writer = new(destination);
        writer.WriteUtf8String(new string('a', 65_535));
        MqttReader reader = new(destination);
        bool result = reader.TryReadUtf8String(out ReadOnlySpan<byte> value);

        // Assert
        writer.Written.ShouldBe(destination.Length);
        result.ShouldBeTrue();
        value.Length.ShouldBe(65_535);
    }

    /// <summary>
    /// Proves writing a string too long for the length prefix throws before anything is written.
    /// </summary>
    [Fact]
    public void WriteUtf8String_StringOver65535Bytes_ThrowsAndWritesNothing()
    {
        // Arrange
        byte[] destination = new byte[70_000];

        // Act & Assert
        Should.Throw<ArgumentException>(() =>
        {
            MqttWriter writer = new(destination);
            writer.WriteUtf8String(new string('a', 65_536));
        });
        destination.ShouldAllBe(b => b == 0);
    }

    /// <summary>
    /// Proves a string that does not fit throws before anything is written.
    /// </summary>
    [Fact]
    public void WriteUtf8String_DestinationTooSmall_ThrowsAndWritesNothing()
    {
        // Arrange
        byte[] destination = new byte[3];

        // Act & Assert
        Should.Throw<ArgumentException>(() =>
        {
            MqttWriter writer = new(destination);
            writer.WriteUtf8String("abc");
        });
        destination.ShouldAllBe(b => b == 0);
    }

    /// <summary>
    /// Proves a variable byte integer that does not fit throws, naming the destination, and writes nothing.
    /// </summary>
    [Fact]
    public void WriteVariableByteInteger_DestinationTooSmall_ThrowsAndWritesNothing()
    {
        // Arrange
        byte[] destination = new byte[1];

        // Act
        ArgumentException exception = Should.Throw<ArgumentException>(() =>
        {
            MqttWriter writer = new(destination);
            writer.WriteVariableByteInteger(128);
        });

        // Assert
        exception.ParamName.ShouldBe("destination");
        destination.ShouldAllBe(b => b == 0);
    }

    /// <summary>
    /// Proves writing to a full destination throws.
    /// </summary>
    [Fact]
    public void WriteByte_DestinationFull_ThrowsArgumentException()
    {
        // Arrange
        byte[] destination = [];

        // Act & Assert
        Should.Throw<ArgumentException>(() =>
        {
            MqttWriter writer = new(destination);
            writer.WriteByte(1);
        });
    }

    /// <summary>
    /// Proves every kind of field the writer writes is read back by the reader.
    /// </summary>
    [Fact]
    public void WriteAndTryRead_EveryFieldKind_RoundTrips()
    {
        // Arrange
        byte[] destination = new byte[64];

        // Act
        MqttWriter writer = new(destination);
        writer.WriteByte(7);
        writer.WriteUInt16(0x0102);
        writer.WriteUInt32(0x01020304);
        writer.WriteVariableByteInteger(16_384);
        writer.WriteUtf8String("ü");
        writer.WriteBinary(Hex.Parse("C3 28"));
        writer.WriteBytes(Hex.Parse("AA BB"));
        int written = writer.Written;

        MqttReader reader = new(destination.AsSpan(0, written));
        bool readByte = reader.TryReadByte(out byte byteValue);
        bool readShort = reader.TryReadUInt16(out ushort shortValue);
        bool readInt = reader.TryReadUInt32(out uint intValue);
        bool readVbi = reader.TryReadVariableByteInteger(out int vbiValue);
        bool readString = reader.TryReadUtf8String(out ReadOnlySpan<byte> text);
        byte[] textBytes = text.ToArray();
        bool readBinary = reader.TryReadBinary(out ReadOnlySpan<byte> binary);
        byte[] binaryBytes = binary.ToArray();
        byte[] rest = reader.ReadToEnd().ToArray();

        // Assert
        (readByte && readShort && readInt && readVbi && readString && readBinary).ShouldBeTrue();
        byteValue.ShouldBe((byte)7);
        shortValue.ShouldBe((ushort)0x0102);
        intValue.ShouldBe(0x01020304u);
        vbiValue.ShouldBe(16_384);
        textBytes.ShouldBe(Hex.Parse("C3 BC"));
        binaryBytes.ShouldBe(Hex.Parse("C3 28"));
        rest.ShouldBe(Hex.Parse("AA BB"));
        written.ShouldBe(1 + 2 + 4 + 3 + 4 + 4 + 2);
    }
}
