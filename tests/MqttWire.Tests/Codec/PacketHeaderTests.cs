using System.Buffers;
using MqttWire.Codec;

namespace MqttWire.Tests.Codec;

/// <summary>
/// Tests reading and writing the fixed header.
/// </summary>
public sealed class PacketHeaderTests
{
    /// <summary>
    /// Proves a valid header gives its type, flags and lengths.
    /// </summary>
    [Theory]
    [InlineData("10 0E", (byte)PacketType.Connect, 0x0, 14, 2)]
    [InlineData("3D 07", (byte)PacketType.Publish, 0xD, 7, 2)]
    [InlineData("62 02", (byte)PacketType.PubRel, 0x2, 2, 2)]
    [InlineData("82 80 01", (byte)PacketType.Subscribe, 0x2, 128, 3)]
    [InlineData("30 FF FF FF 7F", (byte)PacketType.Publish, 0x0, 268_435_455, 5)]
    [InlineData("F0 00", (byte)PacketType.Auth, 0x0, 0, 2)]
    public void TryRead_ValidHeader_GivesTypeFlagsAndLengths(string hex, byte type, byte flags, int remaining, int headerLength)
    {
        // Arrange
        byte[] source = Hex.Parse(hex);

        // Act
        OperationStatus status = PacketHeader.TryRead(source, out PacketHeader header);

        // Assert
        status.ShouldBe(OperationStatus.Done);
        ((byte)header.Type).ShouldBe(type);
        header.Flags.ShouldBe(flags);
        header.RemainingLength.ShouldBe(remaining);
        header.HeaderLength.ShouldBe(headerLength);
        header.TotalLength.ShouldBe(headerLength + remaining);
    }

    /// <summary>
    /// Proves an unknown type, flags not allowed for the type or an invalid length return invalid data.
    /// </summary>
    [Theory]
    [InlineData("00 00")]
    [InlineData("60 02")]
    [InlineData("80 02")]
    [InlineData("A0 02")]
    [InlineData("36 02")]
    [InlineData("41 02")]
    [InlineData("C1")]
    [InlineData("30 FF FF FF FF")]
    [InlineData("30 80 00")]
    public void TryRead_WrongFlagsOrLength_ReturnsInvalidData(string hex)
    {
        // Arrange
        byte[] source = Hex.Parse(hex);

        // Act
        OperationStatus status = PacketHeader.TryRead(source, out PacketHeader _);

        // Assert
        status.ShouldBe(OperationStatus.InvalidData);
    }

    /// <summary>
    /// Proves a header cut off before its length ends returns need more data.
    /// </summary>
    [Theory]
    [InlineData("")]
    [InlineData("30")]
    [InlineData("30 80")]
    [InlineData("30 FF FF FF")]
    public void TryRead_HeaderCutShort_ReturnsNeedMoreData(string hex)
    {
        // Arrange
        byte[] source = Hex.Parse(hex);

        // Act
        OperationStatus status = PacketHeader.TryRead(source, out PacketHeader _);

        // Assert
        status.ShouldBe(OperationStatus.NeedMoreData);
    }

    /// <summary>
    /// Proves a packet returns need more data until its last byte is in the buffer and done after, ignoring trailing bytes.
    /// </summary>
    [Fact]
    public void TryReadPacket_PacketNotComplete_ReturnsNeedMoreDataUntilItsLastByte()
    {
        // Arrange
        byte[] packet = Hex.Parse("32 0A 00 03 61 2F 62 00 0A 00 68 69");
        byte[] withTrailingByte = [.. packet, 0xAA];

        // Act & Assert
        for (int length = 0; length < packet.Length; length++)
        {
            PacketHeader.TryReadPacket(packet.AsSpan(0, length), out PacketHeader _).ShouldBe(OperationStatus.NeedMoreData);
        }

        PacketHeader.TryReadPacket(packet, out PacketHeader exact).ShouldBe(OperationStatus.Done);
        exact.TotalLength.ShouldBe(12);
        PacketHeader.TryReadPacket(withTrailingByte, out PacketHeader trailing).ShouldBe(OperationStatus.Done);
        trailing.TotalLength.ShouldBe(12);
    }

    /// <summary>
    /// Proves a complete header whose body is missing returns need more data with the header filled, so a read loop
    /// can check the packet's total length before it buffers the body.
    /// </summary>
    /// <param name="hex">
    /// A complete fixed header and less than its body.
    /// </param>
    /// <param name="headerLength">
    /// The size of the fixed header.
    /// </param>
    /// <param name="totalLength">
    /// The size of the whole packet the header announces.
    /// </param>
    [Theory]
    [InlineData("30 FF FF FF 7F 00", 5, 268_435_460)]
    [InlineData("30 05", 2, 7)]
    public void TryReadPacket_HeaderCompleteBodyMissing_ReturnsNeedMoreDataWithTheHeader(string hex, int headerLength, int totalLength)
    {
        // Arrange
        byte[] source = Hex.Parse(hex);

        // Act
        OperationStatus status = PacketHeader.TryReadPacket(source, out PacketHeader header);

        // Assert
        status.ShouldBe(OperationStatus.NeedMoreData);
        header.Type.ShouldBe(PacketType.Publish);
        header.HeaderLength.ShouldBe(headerLength);
        header.TotalLength.ShouldBe(totalLength);
    }

    /// <summary>
    /// Proves an invalid header returns invalid data even from the whole-packet read.
    /// </summary>
    [Fact]
    public void TryReadPacket_InvalidHeader_ReturnsInvalidData()
    {
        // Arrange
        byte[] source = Hex.Parse("60 02 00 00");

        // Act
        OperationStatus status = PacketHeader.TryReadPacket(source, out PacketHeader _);

        // Assert
        status.ShouldBe(OperationStatus.InvalidData);
    }

    /// <summary>
    /// Proves a written header has the expected bytes and reads back unchanged.
    /// </summary>
    [Fact]
    public void WriteAndTryRead_PublishHeader_ReadsBack()
    {
        // Arrange
        byte[] destination = new byte[5];

        // Act
        int written = PacketHeader.Write(destination, PacketType.Publish, 0x2, 128);
        int length = PacketHeader.GetLength(128);
        OperationStatus status = PacketHeader.TryRead(destination.AsSpan(0, written), out PacketHeader header);

        // Assert
        destination.AsSpan(0, written).ToArray().ShouldBe(Hex.Parse("32 80 01"));
        length.ShouldBe(3);
        status.ShouldBe(OperationStatus.Done);
        header.Type.ShouldBe(PacketType.Publish);
        header.Flags.ShouldBe((byte)0x2);
        header.RemainingLength.ShouldBe(128);
    }

    /// <summary>
    /// Proves flags that do not fit in four bits throw.
    /// </summary>
    [Fact]
    public void Write_FlagsAboveTheLowNibble_Throws()
    {
        // Arrange
        byte[] destination = new byte[5];

        // Act & Assert
        Should.Throw<ArgumentOutOfRangeException>(() => PacketHeader.Write(destination, PacketType.Publish, 0x10, 0));
    }

    /// <summary>
    /// Proves a remaining length above the maximum throws.
    /// </summary>
    [Fact]
    public void Write_RemainingLengthOutOfRange_Throws()
    {
        // Arrange
        byte[] destination = new byte[5];

        // Act & Assert
        Should.Throw<ArgumentOutOfRangeException>(() => PacketHeader.Write(destination, PacketType.Publish, 0, VariableByteInteger.MaxValue + 1));
    }

    /// <summary>
    /// Proves exactly one packet of the expected type gives its header.
    /// </summary>
    [Fact]
    public void ReadExact_OnePacketOfTheExpectedType_ReturnsItsHeader()
    {
        // Arrange
        byte[] packet = Hex.Parse("20 03 00 00 00");

        // Act
        PacketHeader header = PacketHeader.ReadExact(packet, PacketType.ConnAck);

        // Assert
        header.Type.ShouldBe(PacketType.ConnAck);
        header.HeaderLength.ShouldBe(2);
        header.TotalLength.ShouldBe(5);
    }

    /// <summary>
    /// Proves a packet of another type, a cut-off one, one with trailing bytes, an invalid header and an empty buffer throw.
    /// </summary>
    /// <param name="hex">
    /// The bytes that are not exactly one CONNACK.
    /// </param>
    [Theory]
    [InlineData("10 03 00 00 00")] // another type
    [InlineData("20 03 00 00")] // body cut short
    [InlineData("20 03 00 00 00 00")] // trailing byte
    [InlineData("20 80")] // remaining length cut short
    [InlineData("21 03 00 00 00")] // invalid flags
    [InlineData("")] // nothing
    public void ReadExact_NotExactlyOnePacketOfTheType_Throws(string hex)
    {
        // Arrange
        byte[] packet = Hex.Parse(hex);

        // Act & Assert
        Should.Throw<ArgumentException>(() => PacketHeader.ReadExact(packet, PacketType.ConnAck));
    }

    /// <summary>
    /// Proves a packet whose type is anywhere in the range gives its header, and one above it, one below it or one
    /// with a trailing byte throws; the packets outside the range carry valid flags for their type.
    /// </summary>
    [Fact]
    public void ReadExact_TypeRange_AcceptsTheRangeAndRefusesTheRest()
    {
        // Arrange
        byte[] first = Hex.Parse("40 02 00 0A");
        byte[] last = Hex.Parse("70 02 00 0A");
        byte[] above = Hex.Parse("82 02 00 0A");
        byte[] below = Hex.Parse("30 02 00 00");
        byte[] trailing = Hex.Parse("40 02 00 0A 00");

        // Act
        PacketHeader firstHeader = PacketHeader.ReadExact(first, PacketType.PubAck, PacketType.PubComp);
        PacketHeader lastHeader = PacketHeader.ReadExact(last, PacketType.PubAck, PacketType.PubComp);

        // Assert
        firstHeader.Type.ShouldBe(PacketType.PubAck);
        lastHeader.Type.ShouldBe(PacketType.PubComp);
        Should.Throw<ArgumentException>(() => PacketHeader.ReadExact(above, PacketType.PubAck, PacketType.PubComp));
        Should.Throw<ArgumentException>(() => PacketHeader.ReadExact(below, PacketType.PubAck, PacketType.PubComp));
        Should.Throw<ArgumentException>(() => PacketHeader.ReadExact(trailing, PacketType.PubAck, PacketType.PubComp));
    }
}

