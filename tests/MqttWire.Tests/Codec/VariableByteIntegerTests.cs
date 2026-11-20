using System.Buffers;
using MqttWire.Codec;

namespace MqttWire.Tests.Codec;

/// <summary>
/// Tests reading and writing the variable byte integer.
/// </summary>
public class VariableByteIntegerTests
{
    /// <summary>
    /// Proves each value the MQTT standard lists is written as, and read back from, the standard's bytes.
    /// </summary>
    [Theory]
    [InlineData(0, "00")]
    [InlineData(127, "7F")]
    [InlineData(128, "80 01")]
    [InlineData(16_383, "FF 7F")]
    [InlineData(16_384, "80 80 01")]
    [InlineData(2_097_151, "FF FF 7F")]
    [InlineData(2_097_152, "80 80 80 01")]
    [InlineData(268_435_455, "FF FF FF 7F")]
    public void WriteAndTryRead_ValueFromTheStandard_MatchesTheStandardBytes(int value, string hex)
    {
        // Arrange
        byte[] expected = Hex.Parse(hex);
        byte[] destination = new byte[VariableByteInteger.MaxLength];
        byte[] source = [.. expected, 0xAA];

        // Act
        int written = VariableByteInteger.Write(destination, value);
        int length = VariableByteInteger.GetLength(value);
        OperationStatus status = VariableByteInteger.TryRead(source, out int readValue, out int readLength);

        // Assert
        destination.AsSpan(0, written).ToArray().ShouldBe(expected);
        written.ShouldBe(expected.Length);
        length.ShouldBe(expected.Length);
        status.ShouldBe(OperationStatus.Done);
        readValue.ShouldBe(value);
        readLength.ShouldBe(expected.Length);
    }

    /// <summary>
    /// Proves a value cut off before its last byte returns need more data.
    /// </summary>
    [Theory]
    [InlineData("")]
    [InlineData("80")]
    [InlineData("FF FF FF")]
    public void TryRead_ValueCutShort_ReturnsNeedMoreData(string hex)
    {
        // Arrange
        byte[] source = Hex.Parse(hex);

        // Act
        OperationStatus status = VariableByteInteger.TryRead(source, out _, out _);

        // Assert
        status.ShouldBe(OperationStatus.NeedMoreData);
    }

    /// <summary>
    /// Proves four bytes without an end, or a longer form than needed, return invalid data.
    /// </summary>
    [Theory]
    [InlineData("FF FF FF FF")]
    [InlineData("80 00")]
    [InlineData("FF 80 00")]
    public void TryRead_FiveBytesOrLongerFormThanNeeded_ReturnsInvalidData(string hex)
    {
        // Arrange
        byte[] source = Hex.Parse(hex);

        // Act
        OperationStatus status = VariableByteInteger.TryRead(source, out _, out _);

        // Assert
        status.ShouldBe(OperationStatus.InvalidData);
    }

    /// <summary>
    /// Proves a negative value or one above the maximum throws.
    /// </summary>
    [Theory]
    [InlineData(-1)]
    [InlineData(268_435_456)]
    public void Write_ValueOutsideTheRange_ThrowsArgumentOutOfRangeException(int value)
    {
        // Arrange
        byte[] destination = new byte[VariableByteInteger.MaxLength];

        // Act & Assert
        Should.Throw<ArgumentOutOfRangeException>(() => VariableByteInteger.Write(destination, value));
    }
}
