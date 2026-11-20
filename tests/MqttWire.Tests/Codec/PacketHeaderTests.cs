using System.Buffers;
using MqttWire.Codec;

namespace MqttWire.Tests.Codec;

public class PacketHeaderTests
{
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

    [Fact]
    public void Write_FlagsAboveTheLowNibble_Throws()
    {
        // Arrange
        byte[] destination = new byte[5];

        // Act & Assert
        Should.Throw<ArgumentOutOfRangeException>(() => PacketHeader.Write(destination, PacketType.Publish, 0x10, 0));
    }

    [Fact]
    public void Write_RemainingLengthOutOfRange_Throws()
    {
        // Arrange
        byte[] destination = new byte[5];

        // Act & Assert
        Should.Throw<ArgumentOutOfRangeException>(() => PacketHeader.Write(destination, PacketType.Publish, 0, VariableByteInteger.MaxValue + 1));
    }
}
