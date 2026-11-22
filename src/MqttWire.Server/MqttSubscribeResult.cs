using System.Runtime.InteropServices;
using MqttWire.Server.Internal;

namespace MqttWire.Server;

/// <summary>
/// The answer to <see cref="IMqttWireHandler.OnSubscribe"/>: grant at a QoS (the server lowers it to the highest
/// QoS it supports if needed), or refuse with a reason code and a reason string. The default value grants
/// <see cref="MqttQoS.AtMostOnce"/>.
/// </summary>
[StructLayout(LayoutKind.Auto)]
public readonly struct MqttSubscribeResult
{
    /// <summary>
    /// Creates a result.
    /// </summary>
    /// <param name="reasonCode">
    /// The reason code: 0x00 to 0x02 grants that QoS, 0x80 or above refuses.
    /// </param>
    /// <param name="reasonString">
    /// The reason string of a refusal, already cut.
    /// </param>
    private MqttSubscribeResult(MqttReasonCode reasonCode, string? reasonString)
    {
        ReasonCode = reasonCode;
        ReasonString = reasonString;
    }

    /// <summary>
    /// Gets the refusal "not authorized" (0x87) with no reason string.
    /// </summary>
    public static MqttSubscribeResult NotAuthorized => new(reasonCode: MqttReasonCode.NotAuthorized, reasonString: null);

    /// <summary>
    /// Gets a value indicating whether the subscription is granted.
    /// </summary>
    internal bool IsAccepted => (byte)ReasonCode < 0x80;

    /// <summary>
    /// Gets the reason code: the granted QoS (0x00 to 0x02) or a refusal (0x80 or above).
    /// </summary>
    internal MqttReasonCode ReasonCode { get; }

    /// <summary>
    /// Gets the reason string of a refusal, at most <see cref="ServerLimits.MaxReasonStringBytes"/> bytes, or
    /// <see langword="null"/>.
    /// </summary>
    internal string? ReasonString { get; }

    /// <summary>
    /// Grants the subscription at a QoS.
    /// </summary>
    /// <param name="qos">
    /// The QoS granted; the server lowers it to its maximum if needed.
    /// </param>
    /// <returns>
    /// A grant.
    /// </returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// The QoS is not 0, 1 or 2.
    /// </exception>
    public static MqttSubscribeResult Granted(MqttQoS qos)
    {
        if (qos > MqttQoS.ExactlyOnce)
        {
            throw new ArgumentOutOfRangeException(paramName: nameof(qos), actualValue: qos,
                message: "A subscription is granted at QoS 0, 1 or 2.");
        }

        return new MqttSubscribeResult(reasonCode: (MqttReasonCode)(byte)qos, reasonString: null);
    }

    /// <summary>
    /// Refuses the subscription.
    /// </summary>
    /// <param name="reasonCode">
    /// The reason code, 0x80 or above.
    /// </param>
    /// <param name="reasonString">
    /// A human-readable reason, or <see langword="null"/>; it is cut to
    /// <see cref="ServerLimits.MaxReasonStringBytes"/> bytes.
    /// </param>
    /// <returns>
    /// A refusal.
    /// </returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// The reason code is below 0x80.
    /// </exception>
    public static MqttSubscribeResult Refuse(MqttReasonCode reasonCode, string? reasonString = null)
    {
        if ((byte)reasonCode < 0x80)
        {
            throw new ArgumentOutOfRangeException(paramName: nameof(reasonCode), actualValue: reasonCode,
                message: "A refusal needs a reason code of 0x80 or above.");
        }

        return new MqttSubscribeResult(reasonCode: reasonCode,
            reasonString: ReasonStrings.Cut(reasonString: reasonString));
    }
}
