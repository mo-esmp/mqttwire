using System.Runtime.InteropServices;

namespace MqttWire;

/// <summary>
/// The outcome of a publish, subscribe or unsubscribe request.
/// </summary>
/// <param name="Status">
/// How the request ended.
/// </param>
/// <param name="ReasonCode">
/// The server's reason code when <paramref name="Status"/> is <see cref="MqttRequestStatus.Completed"/>;
/// otherwise meaningless.
/// </param>
[StructLayout(LayoutKind.Auto)]
public readonly record struct MqttRequestResult(MqttRequestStatus Status, MqttReasonCode ReasonCode)
{
    /// <summary>
    /// Gets a value indicating whether the exchange completed and the server's reason code is not a refusal
    /// (below 0x80).
    /// </summary>
    public bool IsSuccess => Status == MqttRequestStatus.Completed && (byte)ReasonCode < 0x80;

    /// <summary>
    /// Gets the QoS a subscription was granted, or <see langword="null"/> when the exchange did not complete
    /// or the reason code is not a granted QoS (0x00, 0x01 or 0x02). Meaningful for a subscribe's result only:
    /// the result does not know its request's kind, so a successful publish or unsubscribe, whose reason code is
    /// 0x00, reads as <see cref="MqttQoS.AtMostOnce"/> here.
    /// </summary>
    public MqttQoS? GrantedQoS => Status == MqttRequestStatus.Completed && (byte)ReasonCode <= (byte)MqttQoS.ExactlyOnce
        ? (MqttQoS)(byte)ReasonCode
        : null;
}
