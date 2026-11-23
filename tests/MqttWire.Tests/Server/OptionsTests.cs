using Microsoft.Extensions.Options;
using MqttWire.Server;
using MqttWire.Server.Internal;

namespace MqttWire.Tests.Server;

/// <summary>
/// Tests of <see cref="MqttWireServerOptionsValidator"/>, of the default options and of the fixed limits of
/// <see cref="ServerLimits"/>.
/// </summary>
public sealed class OptionsTests
{
    /// <summary>
    /// Proves that the default options are valid.
    /// </summary>
    [Fact]
    public void Validate_Defaults_Succeeds()
    {
        // Arrange
        MqttWireServerOptions options = new();

        // Act
        ValidateOptionsResult result = Validate(options);

        // Assert
        result.Succeeded.ShouldBeTrue();
        options.MaxQoS.ShouldBe(MqttQoS.ExactlyOnce);
        options.MaxPublishPayloadBytes.ShouldBe(65_536);
        options.MaxOutgoingMessages.ShouldBe(1_000);
        options.MaxOutgoingBytes.ShouldBe(1_048_576);
    }

    /// <summary>
    /// Proves that every fixed limit and deadline of <see cref="ServerLimits"/> has the value design 6.6 gives it.
    /// </summary>
    [Fact]
    public void Limits_EachConstant_HasTheValueOfTheDesign()
    {
        // Arrange
        // Act
        TimeSpan[] deadlines =
        [
            ServerLimits.SweepInterval, ServerLimits.ConnectDeadline, ServerLimits.AuthenticationDeadline,
            ServerLimits.PartialPacketDeadline, ServerLimits.WriteDeadline, ServerLimits.AcknowledgementDeadline,
            ServerLimits.CloseDeadline,
        ];
        int[] limits =
        [
            ServerLimits.MaxKeepAliveSeconds, ServerLimits.ReceiveMaximum, ServerLimits.MaxWaitingControlPackets,
            ServerLimits.MaxControlPacketBytes, ServerLimits.MaxEncodingBufferBytes, ServerLimits.CopyThresholdBytes,
            ServerLimits.FirstReadBufferBytes, ServerLimits.PublishOverheadBytes, ServerLimits.SkipFactor,
            ServerLimits.MinClientMaximumPacketSize, ServerLimits.MaxReasonStringBytes,
            ServerLimits.MaxConnAckUserPropertyBytes, ServerLimits.MaxClientIdBytes, ServerLimits.MaxTopicBytes,
            ServerLimits.MaxPooledObjects, ServerLimits.MaxPooledQueueCapacity,
        ];

        // Assert
        deadlines.ShouldBe(
        [
            TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(10),
            TimeSpan.FromSeconds(15), TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(2),
        ]);
        limits.ShouldBe([60, 256, 1_024, 16_384, 65_536, 16_384, 4_096, 5_120, 16, 4_096, 256, 1_024, 256, 65_526, 1_024, 64]);
        (ServerLimits.KeepAliveFactorNumerator, ServerLimits.KeepAliveFactorDenominator).ShouldBe((3, 2));
    }

    /// <summary>
    /// Proves that a maximum quality of service of at most once is refused, and the message names the option.
    /// </summary>
    [Fact]
    public void Validate_MaxQoSAtMostOnce_Fails()
    {
        // Arrange
        MqttWireServerOptions options = new() { MaxQoS = MqttQoS.AtMostOnce };

        // Act
        ValidateOptionsResult result = Validate(options);

        // Assert
        result.Failed.ShouldBeTrue();
        result.Failures.ShouldHaveSingleItem().ShouldContain("MaxQoS");
    }

    /// <summary>
    /// Proves that a maximum quality of service outside the enumeration is refused.
    /// </summary>
    [Fact]
    public void Validate_MaxQoSOutOfRange_Fails()
    {
        // Arrange
        MqttWireServerOptions options = new() { MaxQoS = (MqttQoS)3 };

        // Act
        ValidateOptionsResult result = Validate(options);

        // Assert
        result.Failed.ShouldBeTrue();
        result.Failures.ShouldHaveSingleItem().ShouldContain("MaxQoS");
    }

    /// <summary>
    /// Proves that a payload limit below 1 or above 16 MiB is refused.
    /// </summary>
    /// <param name="value">The payload limit to check.</param>
    [Theory]
    [InlineData(0)]
    [InlineData(16_777_217)]
    public void Validate_MaxPublishPayloadBytesOutOfRange_Fails(int value)
    {
        // Arrange
        MqttWireServerOptions options = new() { MaxPublishPayloadBytes = value };

        // Act
        ValidateOptionsResult result = Validate(options);

        // Assert
        result.Failed.ShouldBeTrue();
        result.Failures.ShouldHaveSingleItem().ShouldContain("MaxPublishPayloadBytes");
    }

    /// <summary>
    /// Proves that a queue limit of zero messages is refused.
    /// </summary>
    [Fact]
    public void Validate_MaxOutgoingMessagesZero_Fails()
    {
        // Arrange
        MqttWireServerOptions options = new() { MaxOutgoingMessages = 0 };

        // Act
        ValidateOptionsResult result = Validate(options);

        // Assert
        result.Failed.ShouldBeTrue();
        result.Failures.ShouldHaveSingleItem().ShouldContain("MaxOutgoingMessages");
    }

    /// <summary>
    /// Proves that a queue byte limit below 64 KiB is refused, and 64 KiB itself is accepted.
    /// </summary>
    [Fact]
    public void Validate_MaxOutgoingBytesBelow64KiB_Fails()
    {
        // Arrange
        MqttWireServerOptions tooSmall = new() { MaxOutgoingBytes = 65_535 };
        MqttWireServerOptions smallest = new() { MaxOutgoingBytes = 65_536 };

        // Act
        ValidateOptionsResult failed = Validate(tooSmall);
        ValidateOptionsResult succeeded = Validate(smallest);

        // Assert
        failed.Failed.ShouldBeTrue();
        failed.Failures.ShouldHaveSingleItem().ShouldContain("MaxOutgoingBytes");
        succeeded.Succeeded.ShouldBeTrue();
    }

    /// <summary>
    /// Proves that two invalid values are both reported in one result.
    /// </summary>
    [Fact]
    public void Validate_TwoInvalidValues_ReportsBoth()
    {
        // Arrange
        MqttWireServerOptions options = new() { MaxOutgoingMessages = 0, MaxPublishPayloadBytes = 0 };

        // Act
        ValidateOptionsResult result = Validate(options);

        // Assert
        result.Failed.ShouldBeTrue();
        result.Failures.Count().ShouldBe(2);
    }

    /// <summary>
    /// Runs the validator on the options.
    /// </summary>
    /// <param name="options">The options to check.</param>
    /// <returns>The result of the validation.</returns>
    private static ValidateOptionsResult Validate(MqttWireServerOptions options) =>
        new MqttWireServerOptionsValidator().Validate(null, options);
}
