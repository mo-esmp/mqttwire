using MqttWire.Server.Internal;

namespace MqttWire.Tests.Server;

/// <summary>
/// Tests of <see cref="ExchangeRecords"/>, the records of one connection's QoS 1 and 2 exchanges (plan decision 13):
/// two arrays of 16-byte records, searched linearly, at most 256 each.
/// </summary>
public sealed class ExchangeRecordsTests
{
    /// <summary>
    /// Proves that an incoming identifier that was added is found, and is no longer found once removed, and that a
    /// second removal says it was not there.
    /// </summary>
    [Fact]
    public void RemoveIncoming_AddedIdentifier_IsForgotten()
    {
        // Arrange
        ExchangeRecords records = new();
        records.AddIncoming(packetIdentifier: 7, now: 100);
        records.AddIncoming(packetIdentifier: 9, now: 200);

        // Act
        bool containedBefore = records.ContainsIncoming(7);
        bool removed = records.RemoveIncoming(7);
        bool removedAgain = records.RemoveIncoming(7);

        // Assert
        containedBefore.ShouldBeTrue();
        removed.ShouldBeTrue();
        removedAgain.ShouldBeFalse();
        records.ContainsIncoming(7).ShouldBeFalse();
        records.ContainsIncoming(9).ShouldBeTrue();
        records.IncomingCount.ShouldBe(1);
        records.IsEmpty.ShouldBeFalse();
    }

    /// <summary>
    /// Proves that the incoming records grow from their initial capacity to 256, keeping every identifier, and that a
    /// 257th is refused as a caller bug.
    /// </summary>
    [Fact]
    public void AddIncoming_UpTo256_GrowsAndKeepsEveryIdentifier()
    {
        // Arrange
        ExchangeRecords records = new();

        // Act
        for (int identifier = 1; identifier <= 256; identifier++)
        {
            records.AddIncoming((ushort)identifier, now: identifier);
        }

        // Assert
        records.IncomingCount.ShouldBe(256);
        for (int identifier = 1; identifier <= 256; identifier++)
        {
            records.ContainsIncoming((ushort)identifier).ShouldBeTrue($"identifier {identifier}");
        }

        records.ContainsIncoming(257).ShouldBeFalse();
        Should.Throw<InvalidOperationException>(() => records.AddIncoming(257, now: 1));
    }

    /// <summary>
    /// Proves that <see cref="ExchangeRecords.AddDelivery"/> hands out 1, 2, 3 in order from a counter that starts at
    /// 0, with the phase and the time given, each found by its identifier, and leaves the counter at the last one.
    /// </summary>
    [Fact]
    public void AddDelivery_FromEmpty_HandsOutOneTwoThree()
    {
        // Arrange
        ExchangeRecords records = new();
        ushort last = 0;

        // Act
        ushort first = records.AddDelivery(ExchangePhase.DeliveryAwaitingPubAck, now: 10, ref last);
        ushort second = records.AddDelivery(ExchangePhase.DeliveryAwaitingPubRec, now: 20, ref last);
        ushort third = records.AddDelivery(ExchangePhase.DeliveryAwaitingPubAck, now: 30, ref last);

        // Assert
        first.ShouldBe((ushort)1);
        second.ShouldBe((ushort)2);
        third.ShouldBe((ushort)3);
        last.ShouldBe((ushort)3);
        records.DeliveryCount.ShouldBe(3);
        records.TryFindDelivery(2, out int index).ShouldBeTrue();
        ExchangeRecord record = records.DeliveryAt(index);
        record.PacketIdentifier.ShouldBe((ushort)2);
        record.Phase.ShouldBe(ExchangePhase.DeliveryAwaitingPubRec);
        record.EnteredAt.ShouldBe(20);
        records.TryFindDelivery(4, out _).ShouldBeFalse();
    }

    /// <summary>
    /// Proves that the identifier counter wraps from 65,535 to 1, and skips identifiers that still have a record.
    /// </summary>
    [Fact]
    public void AddDelivery_AfterTheLastIdentifier_WrapsToOneSkippingThoseInUse()
    {
        // Arrange
        ExchangeRecords records = new();
        ushort last = 0;
        ushort kept1 = records.AddDelivery(ExchangePhase.DeliveryAwaitingPubAck, now: 1, ref last);
        ushort kept2 = records.AddDelivery(ExchangePhase.DeliveryAwaitingPubAck, now: 1, ref last);

        // Hand out and remove every identifier up to 65,535, keeping 1 and 2.
        for (int i = 3; i <= ushort.MaxValue; i++)
        {
            ushort identifier = records.AddDelivery(ExchangePhase.DeliveryAwaitingPubAck, now: 1, ref last);
            identifier.ShouldBe((ushort)i);
            records.TryFindDelivery(identifier, out int index).ShouldBeTrue();
            records.RemoveDeliveryAt(index);
        }

        // Act
        ushort wrapped = records.AddDelivery(ExchangePhase.DeliveryAwaitingPubAck, now: 1, ref last);

        // Assert
        kept1.ShouldBe((ushort)1);
        kept2.ShouldBe((ushort)2);
        wrapped.ShouldBe((ushort)3);
        last.ShouldBe((ushort)3);
        records.DeliveryCount.ShouldBe(3);
    }

    /// <summary>
    /// Proves that a record changed through <see cref="ExchangeRecords.DeliveryAt"/> is changed in place, and that a
    /// removed delivery is no longer found while the others are.
    /// </summary>
    [Fact]
    public void DeliveryAt_ChangedInPlace_KeepsTheChange()
    {
        // Arrange
        ExchangeRecords records = new();
        ushort last = 0;
        records.AddDelivery(ExchangePhase.DeliveryAwaitingPubRec, now: 10, ref last);
        records.AddDelivery(ExchangePhase.DeliveryAwaitingPubAck, now: 20, ref last);
        records.TryFindDelivery(1, out int index).ShouldBeTrue();

        // Act
        ref ExchangeRecord record = ref records.DeliveryAt(index);
        record.Phase = ExchangePhase.DeliveryAwaitingPubComp;
        record.EnteredAt = 50;
        records.TryFindDelivery(2, out int second).ShouldBeTrue();
        records.RemoveDeliveryAt(second);

        // Assert
        records.TryFindDelivery(1, out int found).ShouldBeTrue();
        records.DeliveryAt(found).Phase.ShouldBe(ExchangePhase.DeliveryAwaitingPubComp);
        records.DeliveryAt(found).EnteredAt.ShouldBe(50);
        records.TryFindDelivery(2, out _).ShouldBeFalse();
        records.DeliveryCount.ShouldBe(1);
    }

    /// <summary>
    /// Proves that <see cref="ExchangeRecords.OldestEnteredAt"/> is the earliest stamp over both halves, and 0 when
    /// no record is held.
    /// </summary>
    [Fact]
    public void OldestEnteredAt_OverBothHalves_IsTheEarliestAndZeroWhenEmpty()
    {
        // Arrange
        ExchangeRecords records = new();
        long whenEmpty = records.OldestEnteredAt;
        ushort last = 0;
        records.AddIncoming(packetIdentifier: 1, now: 300);
        records.AddDelivery(ExchangePhase.DeliveryAwaitingPubAck, now: 200, ref last);
        records.AddIncoming(packetIdentifier: 2, now: 400);

        // Act
        long oldest = records.OldestEnteredAt;
        records.TryFindDelivery(1, out int index).ShouldBeTrue();
        records.RemoveDeliveryAt(index);
        long oldestIncoming = records.OldestEnteredAt;
        records.RemoveIncoming(1);
        records.RemoveIncoming(2);

        // Assert
        whenEmpty.ShouldBe(0);
        oldest.ShouldBe(200);
        oldestIncoming.ShouldBe(300);
        records.OldestEnteredAt.ShouldBe(0);
        records.IsEmpty.ShouldBeTrue();
    }

    /// <summary>
    /// Proves that <see cref="ExchangeRecords.Clear"/> leaves the records empty, and that the arrays take records
    /// again afterwards.
    /// </summary>
    [Fact]
    public void Clear_WithRecords_LeavesThemEmptyAndReusable()
    {
        // Arrange
        ExchangeRecords records = new();
        ushort last = 0;
        for (int identifier = 1; identifier <= 40; identifier++)
        {
            records.AddIncoming((ushort)identifier, now: identifier);
            records.AddDelivery(ExchangePhase.DeliveryAwaitingPubAck, now: identifier, ref last);
        }

        // Act
        records.Clear();
        bool emptyAfterClear = records.IsEmpty;
        records.AddIncoming(packetIdentifier: 5, now: 9);
        ushort delivery = records.AddDelivery(ExchangePhase.DeliveryAwaitingPubRec, now: 8, ref last);

        // Assert
        emptyAfterClear.ShouldBeTrue();
        records.IncomingCount.ShouldBe(1);
        records.DeliveryCount.ShouldBe(1);
        records.ContainsIncoming(5).ShouldBeTrue();
        records.ContainsIncoming(6).ShouldBeFalse();
        delivery.ShouldBe((ushort)41);
        records.OldestEnteredAt.ShouldBe(8);
    }

    /// <summary>
    /// Proves that the identifier counter is the owner's, not the pooled object's: one object used by two owners in
    /// turn, cleared between them as the pool does, hands each owner the identifiers that follow its own last one.
    /// </summary>
    [Fact]
    public void AddDelivery_OneObjectForTwoOwners_DoesNotShareTheCounter()
    {
        // Arrange
        ExchangeRecords records = new();
        ushort firstOwner = 0;
        ushort secondOwner = 0;
        records.AddDelivery(ExchangePhase.DeliveryAwaitingPubAck, now: 1, ref firstOwner);
        records.AddDelivery(ExchangePhase.DeliveryAwaitingPubAck, now: 1, ref firstOwner);
        records.Clear();

        // Act
        ushort secondOwnersFirst = records.AddDelivery(ExchangePhase.DeliveryAwaitingPubAck, now: 2, ref secondOwner);
        records.Clear();
        ushort firstOwnersThird = records.AddDelivery(ExchangePhase.DeliveryAwaitingPubAck, now: 3, ref firstOwner);

        // Assert
        secondOwnersFirst.ShouldBe((ushort)1);
        secondOwner.ShouldBe((ushort)1);
        firstOwnersThird.ShouldBe((ushort)3);
        firstOwner.ShouldBe((ushort)3);
    }
}
