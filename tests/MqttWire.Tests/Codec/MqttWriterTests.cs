using MqttWire.Codec;

namespace MqttWire.Tests.Codec;

public class MqttWriterTests
{
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

    [Fact]
    public void Utf8String_StringOver65535Bytes_ThrowsArgumentException()
    {
        // Arrange
        string value = new('a', 65_536);

        // Act & Assert
        Should.Throw<ArgumentException>(() => FieldSize.Utf8String(value));
    }

    [Fact]
    public void Utf8StringAndBinary_Over65535Bytes_ThrowsArgumentException()
    {
        // Arrange
        byte[] utf8 = new byte[65_536];

        // Act & Assert
        Should.Throw<ArgumentException>(() => FieldSize.Utf8String(utf8));
        Should.Throw<ArgumentException>(() => FieldSize.Binary(65_536));
    }

    [Fact]
    public void Utf8String_StringWithNullCharacter_ThrowsArgumentException()
    {
        // Arrange
        string value = "a\0";

        // Act & Assert
        Should.Throw<ArgumentException>(() => FieldSize.Utf8String(value));
    }

    [Fact]
    public void Utf8String_StringWithUnpairedSurrogate_ThrowsArgumentException()
    {
        // Arrange
        string value = "a\uD800";

        // Act & Assert
        Should.Throw<ArgumentException>(() => FieldSize.Utf8String(value));
    }

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
