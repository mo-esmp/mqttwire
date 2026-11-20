using MqttWire.Codec;

namespace MqttWire.Tests.Codec;

/// <summary>
/// Tests the reader's fields, its failure results and that a failed read does not move the position.
/// </summary>
public class MqttReaderTests
{
    /// <summary>
    /// Proves two- and four-byte integers are read big-endian.
    /// </summary>
    [Fact]
    public void TryReadUInt16AndUInt32_TwoAndFourBytes_ReadsBigEndian()
    {
        // Arrange
        MqttReader reader = new(Hex.Parse("01 02 01 02 03 04"));

        // Act
        bool readShort = reader.TryReadUInt16(out ushort shortValue);
        bool readInt = reader.TryReadUInt32(out uint intValue);

        // Assert
        readShort.ShouldBeTrue();
        shortValue.ShouldBe((ushort)0x0102);
        readInt.ShouldBeTrue();
        intValue.ShouldBe(0x01020304u);
        reader.Remaining.ShouldBe(0);
    }

    /// <summary>
    /// Proves a valid string, including an empty one, comes back as its UTF-8 bytes without the length.
    /// </summary>
    [Theory]
    [InlineData("00 01 61", "61")]
    [InlineData("00 02 C3 BC", "C3 BC")]
    [InlineData("00 04 F0 9F 98 80", "F0 9F 98 80")]
    [InlineData("00 00", "")]
    public void TryReadUtf8String_ValidString_ReturnsItsUtf8Bytes(string hex, string expected)
    {
        // Arrange
        MqttReader reader = new(Hex.Parse(hex));

        // Act
        bool result = reader.TryReadUtf8String(out ReadOnlySpan<byte> value);

        // Assert
        result.ShouldBeTrue();
        value.ToArray().ShouldBe(Hex.Parse(expected));
        reader.Remaining.ShouldBe(0);
    }

    /// <summary>
    /// Proves a string of the longest allowed length is read whole.
    /// </summary>
    [Fact]
    public void TryReadUtf8String_StringOf65535Bytes_ReturnsAllBytes()
    {
        // Arrange
        byte[] source = new byte[2 + 65_535];
        source[0] = 0xFF;
        source[1] = 0xFF;
        source.AsSpan(2).Fill(0x61);
        MqttReader reader = new(source);

        // Act
        bool result = reader.TryReadUtf8String(out ReadOnlySpan<byte> value);

        // Assert
        result.ShouldBeTrue();
        value.Length.ShouldBe(65_535);
        reader.Remaining.ShouldBe(0);
    }

    /// <summary>
    /// Proves a malformed, cut-off or U+0000 string returns false and leaves the position alone.
    /// </summary>
    [Theory]
    [InlineData("00 02 61")]
    [InlineData("00 02 C3 28")]
    [InlineData("00 02 C0 AF")]
    [InlineData("00 03 ED A0 80")]
    [InlineData("00 01 C3")]
    [InlineData("00 01 00")]
    [InlineData("00")]
    public void TryReadUtf8String_NotValid_ReturnsFalseAndMovesNothing(string hex)
    {
        // Arrange
        byte[] source = Hex.Parse(hex);
        MqttReader reader = new(source);

        // Act
        bool result = reader.TryReadUtf8String(out ReadOnlySpan<byte> value);

        // Assert
        result.ShouldBeFalse();
        value.Length.ShouldBe(0);
        reader.Remaining.ShouldBe(source.Length);
    }

    /// <summary>
    /// Proves binary data is returned as it is, whatever its content.
    /// </summary>
    [Fact]
    public void TryReadBinary_InvalidUtf8_ReturnsTheBytes()
    {
        // Arrange
        MqttReader reader = new(Hex.Parse("00 02 C3 28"));

        // Act
        bool result = reader.TryReadBinary(out ReadOnlySpan<byte> value);

        // Assert
        result.ShouldBeTrue();
        value.ToArray().ShouldBe(Hex.Parse("C3 28"));
        reader.Remaining.ShouldBe(0);
    }

    /// <summary>
    /// Proves a length past the end returns false and leaves the position alone.
    /// </summary>
    [Fact]
    public void TryReadBinary_LengthPastTheEnd_ReturnsFalseAndMovesNothing()
    {
        // Arrange
        MqttReader reader = new(Hex.Parse("00 03 01 02"));

        // Act
        bool result = reader.TryReadBinary(out _);

        // Assert
        result.ShouldBeFalse();
        reader.Remaining.ShouldBe(4);
    }

    /// <summary>
    /// Proves a valid integer is read and a cut or overlong one returns false and leaves the position alone.
    /// </summary>
    [Fact]
    public void TryReadVariableByteInteger_ValidAndMalformed_ReadsOrReturnsFalse()
    {
        // Arrange
        MqttReader valid = new(Hex.Parse("80 80 01 AA"));
        MqttReader cut = new(Hex.Parse("80"));
        MqttReader overlong = new(Hex.Parse("80 00"));

        // Act
        bool validResult = valid.TryReadVariableByteInteger(out int value);
        bool cutResult = cut.TryReadVariableByteInteger(out _);
        bool overlongResult = overlong.TryReadVariableByteInteger(out _);

        // Assert
        validResult.ShouldBeTrue();
        value.ShouldBe(16_384);
        valid.Remaining.ShouldBe(1);
        cutResult.ShouldBeFalse();
        cut.Remaining.ShouldBe(1);
        overlongResult.ShouldBeFalse();
        overlong.Remaining.ShouldBe(2);
    }

    /// <summary>
    /// Proves a byte is read and the rest is then returned and consumed.
    /// </summary>
    [Fact]
    public void TryReadByteAndReadToEnd_SomeBytes_ReadsThenReturnsTheRest()
    {
        // Arrange
        MqttReader reader = new(Hex.Parse("07 01 02"));

        // Act
        bool result = reader.TryReadByte(out byte value);
        ReadOnlySpan<byte> rest = reader.ReadToEnd();

        // Assert
        result.ShouldBeTrue();
        value.ShouldBe((byte)7);
        rest.ToArray().ShouldBe(Hex.Parse("01 02"));
        reader.Remaining.ShouldBe(0);
    }

    /// <summary>
    /// Proves reads past the end of the buffer return false and leave the position alone.
    /// </summary>
    [Fact]
    public void TryRead_PastTheEnd_ReturnsFalseAndMovesNothing()
    {
        // Arrange
        MqttReader shortReader = new(Hex.Parse("01"));
        MqttReader bytesReader = new(Hex.Parse("01 02"));
        MqttReader emptyReader = new([]);

        // Act
        bool shortResult = shortReader.TryReadUInt16(out _);
        bool bytesResult = bytesReader.TryReadBytes(3, out _);
        bool emptyResult = emptyReader.TryReadByte(out _);

        // Assert
        shortResult.ShouldBeFalse();
        shortReader.Remaining.ShouldBe(1);
        bytesResult.ShouldBeFalse();
        bytesReader.Remaining.ShouldBe(2);
        emptyResult.ShouldBeFalse();
    }
}
