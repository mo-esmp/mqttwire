namespace MqttWire.Tests;

/// <summary>
/// Pins what the zero value of <see cref="MqttClientCloseReason"/> means.
/// </summary>
public sealed class MqttClientCloseReasonTests
{
    /// <summary>
    /// Proves a default <see cref="MqttClientCloseInfo"/> reads as no reason recorded, not as a real reason such as
    /// a dispose.
    /// </summary>
    [Fact]
    public void Default_CloseInfo_HasNoReasonRecorded()
    {
        // Arrange
        MqttClientCloseInfo info = default;

        // Act
        string reason = info.Reason.ToString();

        // Assert
        reason.ShouldBe("None");
    }
}
