using System.Runtime.InteropServices;
using MqttWire.Server.Internal;

namespace MqttWire.Server;

/// <summary>
/// The answer to <see cref="IMqttWireHandler.OnSubscribe"/>: grant at a QoS (the server lowers it to the highest
/// QoS it supports if needed), or refuse with a reason code and a reason string. Only <see cref="Granted"/>
/// grants: the default value is a refusal with 0x87 (not authorized) and no reason string, so a handler that
/// returns <c>default</c> grants nothing.
/// </summary>
[StructLayout(LayoutKind.Auto)]
public readonly struct MqttSubscribeResult
{
    /// <summary>
    /// Whether a factory member created the value. The default value, which no factory created, fails closed: it
    /// is a refusal with 0x87 (not authorized).
    /// </summary>
    private readonly bool _created;

    /// <summary>
    /// The reason code a factory member gave; meaningless in the default value.
    /// </summary>
    private readonly MqttReasonCode _reasonCode;

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
        _created = true;
        _reasonCode = reasonCode;
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
    /// Gets the reason code: the granted QoS (0x00 to 0x02) or a refusal (0x80 or above); 0x87 for the default
    /// value.
    /// </summary>
    internal MqttReasonCode ReasonCode => _created ? _reasonCode : MqttReasonCode.NotAuthorized;

    /// <summary>
    /// Gets the reason string of a refusal, at most <see cref="ServerLimits.MaxReasonStringBytes"/> bytes, or
    /// <see langword="null"/>.
    /// </summary>
    internal string? ReasonString { get; }

    /// <summary>
    /// Grants the subscription at a QoS.
    /// </summary>
    /// <param name="qos">
    /// The QoS granted; the server lowers it to <see cref="MqttWireServerOptions.MaxQoS"/> if needed.
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
    /// A human-readable reason, or <see langword="null"/>; it is cut to 256 UTF-8 bytes.
    /// </param>
    /// <returns>
    /// A refusal.
    /// </returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// The reason code is below 0x80.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// The reason string holds U+0000 or an unpaired surrogate, which no packet can carry.
    /// </exception>
    public static MqttSubscribeResult Refuse(MqttReasonCode reasonCode, string? reasonString = null)
    {
        if ((byte)reasonCode < 0x80)
        {
            throw new ArgumentOutOfRangeException(paramName: nameof(reasonCode), actualValue: reasonCode,
                message: "A refusal needs a reason code of 0x80 or above.");
        }

        ReasonStrings.ThrowIfNotEncodable(reasonString: reasonString, paramName: nameof(reasonString));

        return new MqttSubscribeResult(reasonCode: reasonCode,
            reasonString: ReasonStrings.Cut(reasonString: reasonString));
    }
}
