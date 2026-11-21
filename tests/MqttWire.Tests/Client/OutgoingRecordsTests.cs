using MqttWire.Client;

namespace MqttWire.Tests.Client;

/// <summary>
/// Tests the packet identifiers, the publish limit and the pooled completions of the outgoing records.
/// </summary>
public sealed class OutgoingRecordsTests
{
    /// <summary>
    /// A result that says the exchange completed with success.
    /// </summary>
    private static readonly MqttRequestResult s_success = new(MqttRequestStatus.Completed, MqttReasonCode.Success);

    /// <summary>
    /// Proves the first identifier given is 1.
    /// </summary>
    [Fact]
    public void TryAdd_First_GivesIdentifierOne()
    {
        // Arrange
        OutgoingRecords records = new(10);

        // Act
        bool added = records.TryAdd(RecordKind.Subscribe, 100, records.RentCompletion(), out OutgoingRecord? record, out _);

        // Assert
        added.ShouldBeTrue();
        record.ShouldNotBeNull();
        record.PacketIdentifier.ShouldBe((ushort)1);
        record.Kind.ShouldBe(RecordKind.Subscribe);
        record.PhaseStartedAt.ShouldBe(100);
    }

    /// <summary>
    /// Proves a freed identifier is not reused at once: the next one after the last given is.
    /// </summary>
    [Fact]
    public void TryAdd_AfterRemove_GivesTheNextNotTheFreedOne()
    {
        // Arrange
        OutgoingRecords records = new(10);
        records.TryAdd(RecordKind.Subscribe, 0, records.RentCompletion(), out OutgoingRecord? first, out _);
        records.TryAdd(RecordKind.Subscribe, 0, records.RentCompletion(), out _, out _);
        records.Remove(first!);

        // Act
        records.TryAdd(RecordKind.Subscribe, 0, records.RentCompletion(), out OutgoingRecord? third, out _);

        // Assert
        third.ShouldNotBeNull();
        third.PacketIdentifier.ShouldBe((ushort)3);
    }

    /// <summary>
    /// Proves that after 65,535 the identifiers wrap to 1 and skip those in use.
    /// </summary>
    [Fact]
    public void TryAdd_At65535_WrapsToOneSkippingThoseInUse()
    {
        // Arrange
        OutgoingRecords records = new(10);
        FillWithSubscribes(records);
        records.TryGet(3, out OutgoingRecord? three).ShouldBeTrue();
        records.Remove(three);

        // Act
        bool added = records.TryAdd(RecordKind.Subscribe, 0, records.RentCompletion(), out OutgoingRecord? record, out _);

        // Assert
        added.ShouldBeTrue();
        record.ShouldNotBeNull();
        record.PacketIdentifier.ShouldBe((ushort)3);
    }

    /// <summary>
    /// Proves a publish past the limit fails while a subscribe still succeeds.
    /// </summary>
    [Fact]
    public void TryAdd_PublishLimitReached_FailsWithReceiveMaximumReached()
    {
        // Arrange
        OutgoingRecords records = new(2);
        records.TryAdd(RecordKind.PublishQoS1, 0, records.RentCompletion(), out _, out _);
        records.TryAdd(RecordKind.PublishQoS1, 0, records.RentCompletion(), out _, out _);

        // Act
        bool publishAdded = records.TryAdd(RecordKind.PublishQoS1, 0, records.RentCompletion(), out _, out MqttRequestStatus failure);
        bool subscribeAdded = records.TryAdd(RecordKind.Subscribe, 0, records.RentCompletion(), out _, out _);

        // Assert
        publishAdded.ShouldBeFalse();
        failure.ShouldBe(MqttRequestStatus.ReceiveMaximumReached);
        subscribeAdded.ShouldBeTrue();
        records.PublishesInFlight.ShouldBe(2);
    }

    /// <summary>
    /// Proves a refused publish does not consume a packet identifier.
    /// </summary>
    [Fact]
    public void TryAdd_PublishLimitReached_DoesNotUseAnIdentifier()
    {
        // Arrange
        OutgoingRecords records = new(1);
        records.TryAdd(RecordKind.PublishQoS1, 0, records.RentCompletion(), out _, out _);
        records.TryAdd(RecordKind.PublishQoS1, 0, records.RentCompletion(), out _, out _);

        // Act
        records.TryAdd(RecordKind.Subscribe, 0, records.RentCompletion(), out OutgoingRecord? record, out _);

        // Assert
        record.ShouldNotBeNull();
        record.PacketIdentifier.ShouldBe((ushort)2);
    }

    /// <summary>
    /// Proves that when all 65,535 identifiers are in use the next request fails.
    /// </summary>
    [Fact]
    public void TryAdd_AllIdentifiersInUse_FailsWithNoFreePacketIdentifier()
    {
        // Arrange
        OutgoingRecords records = new(10);
        FillWithSubscribes(records);

        // Act
        bool added = records.TryAdd(RecordKind.Subscribe, 0, records.RentCompletion(), out _, out MqttRequestStatus failure);

        // Assert
        added.ShouldBeFalse();
        failure.ShouldBe(MqttRequestStatus.NoFreePacketIdentifier);
    }

    /// <summary>
    /// Proves a QoS 2 record keeps counting against the limit after PUBREC, until it is removed.
    /// </summary>
    [Fact]
    public void Advance_PubRecReceived_KeepsCountingAgainstTheLimit()
    {
        // Arrange
        OutgoingRecords records = new(1);
        records.TryAdd(RecordKind.PublishQoS2AwaitingPubRec, 0, records.RentCompletion(), out OutgoingRecord? record, out _);

        // Act
        records.Advance(record!, RecordKind.PublishQoS2AwaitingPubComp, 5);
        bool added = records.TryAdd(RecordKind.PublishQoS1, 6, records.RentCompletion(), out _, out MqttRequestStatus failure);

        // Assert
        record!.Kind.ShouldBe(RecordKind.PublishQoS2AwaitingPubComp);
        records.PublishesInFlight.ShouldBe(1);
        added.ShouldBeFalse();
        failure.ShouldBe(MqttRequestStatus.ReceiveMaximumReached);
    }

    /// <summary>
    /// Proves the oldest phase start follows an advance, and is null when the table is empty.
    /// </summary>
    [Fact]
    public void OldestPhaseStartedAt_AfterAdvance_IsTheAdvanceTime()
    {
        // Arrange
        OutgoingRecords records = new(10);
        long? emptyOldest = records.OldestPhaseStartedAt;
        records.TryAdd(RecordKind.PublishQoS2AwaitingPubRec, 10, records.RentCompletion(), out OutgoingRecord? record, out _);

        // Act
        records.Advance(record!, RecordKind.PublishQoS2AwaitingPubComp, 50);

        // Assert
        emptyOldest.ShouldBeNull();
        records.OldestPhaseStartedAt.ShouldBe(50);
    }

    /// <summary>
    /// Proves removing a record frees its identifier lookup and its publish slot.
    /// </summary>
    [Fact]
    public void Remove_Record_FreesTheIdentifierAndTheSlot()
    {
        // Arrange
        OutgoingRecords records = new(1);
        records.TryAdd(RecordKind.PublishQoS1, 0, records.RentCompletion(), out OutgoingRecord? record, out _);
        ushort identifier = record!.PacketIdentifier;

        // Act
        records.Remove(record);
        bool added = records.TryAdd(RecordKind.PublishQoS1, 0, records.RentCompletion(), out _, out _);

        // Assert
        records.TryGet(identifier, out _).ShouldBeFalse();
        added.ShouldBeTrue();
        records.PublishesInFlight.ShouldBe(1);
    }

    /// <summary>
    /// Proves removing a record that was already removed does nothing, even after its identifier was given to a
    /// new record: the new record stays in the table.
    /// </summary>
    [Fact]
    public void Remove_StaleRecordAfterIdentifierReuse_LeavesTheNewRecord()
    {
        // Arrange
        OutgoingRecords records = new(10);
        FillWithSubscribes(records);
        records.TryGet(3, out OutgoingRecord? stale).ShouldBeTrue();
        records.TryGet(5, out OutgoingRecord? five).ShouldBeTrue();
        records.Remove(stale);
        records.Remove(five);
        records.TryAdd(RecordKind.Subscribe, 0, records.RentCompletion(), out OutgoingRecord? reused, out _);

        // Act
        records.Remove(stale);

        // Assert
        reused.ShouldNotBeNull();
        reused.ShouldNotBeSameAs(stale);
        reused.PacketIdentifier.ShouldBe((ushort)3);
        records.TryGet(3, out OutgoingRecord? found).ShouldBeTrue();
        found.ShouldBeSameAs(reused);
    }

    /// <summary>
    /// Proves DrainAll hands every record on and leaves the table empty.
    /// </summary>
    [Fact]
    public void DrainAll_RecordsInUse_VisitsEachAndEmptiesTheTable()
    {
        // Arrange
        OutgoingRecords records = new(10);
        records.TryAdd(RecordKind.PublishQoS1, 0, records.RentCompletion(), out _, out _);
        records.TryAdd(RecordKind.Subscribe, 0, records.RentCompletion(), out _, out _);
        int visited = 0;

        // Act
        records.DrainAll(_ => visited++);

        // Assert
        visited.ShouldBe(2);
        records.PublishesInFlight.ShouldBe(0);
        records.OldestPhaseStartedAt.ShouldBeNull();
        records.TryGet(1, out _).ShouldBeFalse();
    }

    /// <summary>
    /// Proves a completion that is completed but never awaited is not given out again.
    /// </summary>
    [Fact]
    public void Complete_NeverAwaited_DoesNotReturnTheSourceToThePool()
    {
        // Arrange
        OutgoingRecords records = new(10);
        RequestCompletion first = records.RentCompletion();
        first.Complete(s_success);

        // Act
        RequestCompletion second = records.RentCompletion();

        // Assert
        second.ShouldNotBeSameAs(first);
    }

    /// <summary>
    /// Proves an awaited completion goes back to the pool it came from once its result is read.
    /// </summary>
    /// <returns>
    /// A task that ends when the test has run.
    /// </returns>
    [Fact]
    public async Task Complete_Awaited_ReturnsTheSourceToThePoolAfterGetResult()
    {
        // Arrange
        OutgoingRecords records = new(10);
        RequestCompletion first = records.RentCompletion();
        ValueTask<MqttRequestResult> task = first.Task;
        first.Complete(s_success);

        // Act
        MqttRequestResult result = await task;
        RequestCompletion second = records.RentCompletion();

        // Assert
        result.ShouldBe(s_success);
        second.ShouldBeSameAs(first);
    }

    /// <summary>
    /// Adds a Subscribe record for every packet identifier, 1 to 65,535.
    /// </summary>
    /// <param name="records">
    /// The table to fill.
    /// </param>
    private static void FillWithSubscribes(OutgoingRecords records)
    {
        for (int i = 0; i < ushort.MaxValue; i++)
        {
            records.TryAdd(RecordKind.Subscribe, 0, records.RentCompletion(), out _, out _).ShouldBeTrue();
        }
    }
}
