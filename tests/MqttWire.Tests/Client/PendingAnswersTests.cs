using MqttWire.Client;
using MqttWire.Codec;
using MqttWire.Tests.Codec;

namespace MqttWire.Tests.Client;

/// <summary>
/// Tests the pooled buffer in which the read loop keeps its answers until it writes them in one go.
/// </summary>
public sealed class PendingAnswersTests
{
    /// <summary>
    /// Proves the first answer rents one buffer of the full answer size and a second answer rents no other.
    /// </summary>
    [Fact]
    public void Add_FirstAnswer_RentsOneBufferOfAnswerBufferBytes()
    {
        // Arrange
        CountingPool pool = new();
        PendingAnswers answers = new(pool);

        // Act
        answers.Add(PacketType.PubAck, 1, MqttReasonCode.Success);
        int afterFirst = pool.Outstanding;
        answers.Add(PacketType.PubAck, 2, MqttReasonCode.Success);

        // Assert
        afterFirst.ShouldBe(1);
        pool.Outstanding.ShouldBe(1);
        pool.LargestRent.ShouldBe(80);
        answers.Release();
    }

    /// <summary>
    /// Proves the answers are handed over encoded one after the other, in the order they were added, and that the
    /// buffer then holds none.
    /// </summary>
    [Fact]
    public void Take_ThreeAnswers_ReturnsThemEncodedInOrder()
    {
        // Arrange
        CountingPool pool = new();
        PendingAnswers answers = new(pool);
        answers.Add(PacketType.PubAck, 1, MqttReasonCode.Success);
        answers.Add(PacketType.PubRec, 7, MqttReasonCode.Success);
        answers.Add(PacketType.PubComp, 7, MqttReasonCode.PacketIdentifierNotFound);

        // Act
        byte[] buffer = answers.Take(out int length);

        // Assert
        buffer.AsSpan(0, length).SequenceEqual(Hex.Parse("40 02 00 01 50 02 00 07 70 03 00 07 92")).ShouldBeTrue();
        answers.Count.ShouldBe(0);
        pool.Return(buffer);
    }

    /// <summary>
    /// Proves that once the buffer is taken, the next answer rents a fresh one.
    /// </summary>
    [Fact]
    public void Take_AfterwardsAdd_RentsAFreshBuffer()
    {
        // Arrange
        CountingPool pool = new();
        PendingAnswers answers = new(pool);
        answers.Add(PacketType.PubAck, 1, MqttReasonCode.Success);
        byte[] taken = answers.Take(out _);
        pool.Return(taken);

        // Act
        answers.Add(PacketType.PubAck, 2, MqttReasonCode.Success);

        // Assert
        pool.Outstanding.ShouldBe(1);
        answers.Release();
    }

    /// <summary>
    /// Proves a write is due once fifteen packets have been counted after the oldest answer, so the sixteenth
    /// packet is not handled before it.
    /// </summary>
    [Fact]
    public void MustWriteBeforeNextPacket_FifteenPacketsAfterTheOldest_IsTrue()
    {
        // Arrange
        CountingPool pool = new();
        PendingAnswers answers = new(pool);
        answers.Add(PacketType.PubAck, 1, MqttReasonCode.Success);

        // Act
        for (int i = 0; i < ClientLimits.AnswerPacketBound - 2; i++)
        {
            answers.CountPacket();
        }

        bool beforeLast = answers.MustWriteBeforeNextPacket;
        answers.CountPacket();

        // Assert
        beforeLast.ShouldBeFalse();
        answers.MustWriteBeforeNextPacket.ShouldBeTrue();
        answers.Release();
    }

    /// <summary>
    /// Proves packets handled while no answer waits are not counted against the next answer.
    /// </summary>
    [Fact]
    public void CountPacket_NothingWaiting_DoesNotCount()
    {
        // Arrange
        CountingPool pool = new();
        PendingAnswers answers = new(pool);
        for (int i = 0; i < 20; i++)
        {
            answers.CountPacket();
        }

        // Act
        answers.Add(PacketType.PubAck, 1, MqttReasonCode.Success);
        for (int i = 0; i < ClientLimits.AnswerPacketBound - 2; i++)
        {
            answers.CountPacket();
        }

        // Assert
        answers.MustWriteBeforeNextPacket.ShouldBeFalse();
        answers.Release();
    }

    /// <summary>
    /// Proves a second release returns nothing more, and a release with nothing held does nothing.
    /// </summary>
    [Fact]
    public void Release_Twice_ReturnsTheBufferOnce()
    {
        // Arrange
        CountingPool pool = new();
        PendingAnswers answers = new(pool);
        answers.Release();
        answers.Add(PacketType.PubAck, 1, MqttReasonCode.Success);

        // Act
        answers.Release();
        answers.Release();

        // Assert
        pool.Outstanding.ShouldBe(0);
        pool.FaultyReturns.ShouldBe(0);
        answers.Count.ShouldBe(0);
    }
}
