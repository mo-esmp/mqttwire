using Microsoft.AspNetCore.Http;
using MqttWire.Server;
using MqttWire.Server.Internal;

namespace MqttWire.Tests.Server;

/// <summary>
/// Tests of <see cref="ConnectionRegistry"/>: the slots the sweep walks and the client-id table admission uses.
/// </summary>
public sealed class ConnectionRegistryTests
{
    /// <summary>
    /// Proves that a slot freed by a removal is the one the next connection takes.
    /// </summary>
    [Fact]
    public void Add_AfterRemove_ReusesTheFreedSlot()
    {
        // Arrange
        MqttWireServer server = ServerHarness.Create().Server;
        ConnectionRegistry registry = new();
        MqttWireConnection first = NewConnection(server, "a");
        MqttWireConnection second = NewConnection(server, "b");
        MqttWireConnection third = NewConnection(server, "c");
        registry.Add(first);
        registry.Add(second);
        int freed = Array.IndexOf(registry.Slots, first);
        registry.Remove(first);

        // Act
        registry.Add(third);

        // Assert
        registry.Slots[freed].ShouldBeSameAs(third);
        registry.Slots.Count(static slot => slot is not null).ShouldBe(2);
    }

    /// <summary>
    /// Proves that the slots grow past their first size and keep every connection added.
    /// </summary>
    [Fact]
    public void Add_MoreThanTheArrayHolds_GrowsAndKeepsEveryConnection()
    {
        // Arrange
        MqttWireServer server = ServerHarness.Create().Server;
        ConnectionRegistry registry = new();
        int initialLength = registry.Slots.Length;
        List<MqttWireConnection> connections = [];
        for (int i = 0; i < 100; i++)
        {
            connections.Add(NewConnection(server, $"c{i}"));
        }

        // Act
        foreach (MqttWireConnection connection in connections)
        {
            registry.Add(connection);
        }

        // Assert
        registry.Slots.Length.ShouldBeGreaterThan(initialLength);
        foreach (MqttWireConnection connection in connections)
        {
            registry.Slots.ShouldContain(connection);
        }
    }

    /// <summary>
    /// Proves that removing a connection whose client id was taken over leaves the one that replaced it in the table.
    /// </summary>
    [Fact]
    public void Remove_ConnectionThatWasReplaced_LeavesItsReplacementInTheTable()
    {
        // Arrange
        MqttWireServer server = ServerHarness.Create().Server;
        ConnectionRegistry registry = new();
        MqttWireConnection old = NewConnection(server, "c1");
        MqttWireConnection replacement = NewConnection(server, "c1");
        registry.Add(old);
        registry.Add(replacement);
        lock (registry.Gate)
        {
            registry.SetCurrent(old);
            registry.SetCurrent(replacement);
        }

        // Act
        registry.Remove(old);

        // Assert
        registry.TryGet("c1", out MqttWireConnection? found).ShouldBeTrue();
        found.ShouldBeSameAs(replacement);
        registry.ActiveCount.ShouldBe(1);
    }

    /// <summary>
    /// Proves that the task of <see cref="ConnectionRegistry.WhenEmptyAsync"/> completes once the last connection
    /// is removed, and not before.
    /// </summary>
    [Fact]
    public void WhenEmptyAsync_LastConnectionRemoved_Completes()
    {
        // Arrange
        MqttWireServer server = ServerHarness.Create().Server;
        ConnectionRegistry registry = new();
        MqttWireConnection first = NewConnection(server, "a");
        MqttWireConnection second = NewConnection(server, "b");
        registry.Add(first);
        registry.Add(second);
        Task empty = registry.WhenEmptyAsync();
        registry.Remove(first);
        bool completedEarly = empty.IsCompleted;

        // Act
        registry.Remove(second);

        // Assert
        completedEarly.ShouldBeFalse();
        empty.IsCompletedSuccessfully.ShouldBeTrue();
    }

    /// <summary>
    /// Creates a connection that is not running, with a client id.
    /// </summary>
    /// <param name="server">
    /// The server it belongs to.
    /// </param>
    /// <param name="clientId">
    /// Its client id.
    /// </param>
    /// <returns>
    /// The connection.
    /// </returns>
    private static MqttWireConnection NewConnection(MqttWireServer server, string clientId) =>
        new(server, new DefaultHttpContext()) { ClientId = clientId };
}
