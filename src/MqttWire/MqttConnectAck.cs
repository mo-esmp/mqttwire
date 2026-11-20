namespace MqttWire;

/// <summary>
/// What <c>ConnectAsync</c> reports: connected, or why not.
/// </summary>
public sealed class MqttConnectAck
{
    /// <summary>
    /// Creates a report.
    /// </summary>
    /// <param name="failure">
    /// Why the connection failed, or <see langword="null"/> when it is connected.
    /// </param>
    /// <param name="reasonCode">
    /// The CONNACK's reason code, or <see cref="MqttReasonCode.UnspecifiedError"/> when none arrived.
    /// </param>
    /// <param name="reasonString">
    /// The CONNACK's reason string, or <see langword="null"/>.
    /// </param>
    /// <param name="userProperties">
    /// The CONNACK's user properties, or <see langword="null"/> for none.
    /// </param>
    /// <param name="exception">
    /// The network error behind <see cref="MqttClientCloseReason.Unreachable"/>, or <see langword="null"/>.
    /// </param>
    internal MqttConnectAck(
        MqttClientCloseReason? failure,
        MqttReasonCode reasonCode,
        string? reasonString,
        IReadOnlyList<MqttWireUserProperty>? userProperties,
        Exception? exception)
    {
        Failure = failure;
        ReasonCode = reasonCode;
        ReasonString = reasonString;
        UserProperties = userProperties ?? [];
        Exception = exception;
    }

    /// <summary>
    /// Gets a value indicating whether the client is connected.
    /// </summary>
    public bool IsConnected => Failure is null;

    /// <summary>
    /// Gets why the connection failed: <see cref="MqttClientCloseReason.ConnectRefused"/>,
    /// <see cref="MqttClientCloseReason.Unreachable"/>, <see cref="MqttClientCloseReason.ConnectTimedOut"/> or
    /// <see cref="MqttClientCloseReason.ProtocolError"/>; <see langword="null"/> when connected.
    /// </summary>
    public MqttClientCloseReason? Failure { get; }

    /// <summary>
    /// Gets the CONNACK's reason code, or <see cref="MqttReasonCode.UnspecifiedError"/> when no valid CONNACK arrived.
    /// </summary>
    public MqttReasonCode ReasonCode { get; }

    /// <summary>
    /// Gets the CONNACK's reason string, or <see langword="null"/>.
    /// </summary>
    public string? ReasonString { get; }

    /// <summary>
    /// Gets the CONNACK's user properties; empty when there are none.
    /// </summary>
    public IReadOnlyList<MqttWireUserProperty> UserProperties { get; }

    /// <summary>
    /// Gets the network error behind <see cref="MqttClientCloseReason.Unreachable"/>, if any.
    /// </summary>
    public Exception? Exception { get; }
}
