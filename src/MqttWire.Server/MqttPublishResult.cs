using System.Runtime.InteropServices;
using MqttWire.Server.Internal;

namespace MqttWire.Server;

/// <summary>
/// The answer to <see cref="IMqttWireHandler.OnPublish"/>: accept, or refuse with a reason code and a reason
/// string. The server sends the answer in the form the QoS of the message calls for. The default value is the
/// accepted result.
/// </summary>
[StructLayout(LayoutKind.Auto)]
public readonly struct MqttPublishResult
{
    /// <summary>
    /// Creates a result.
    /// </summary>
    /// <param name="reasonCode">
    /// The reason code: below 0x80 accepts.
    /// </param>
    /// <param name="reasonString">
    /// The reason string of a refusal, already cut.
    /// </param>
    private MqttPublishResult(MqttReasonCode reasonCode, string? reasonString)
    {
        ReasonCode = reasonCode;
        ReasonString = reasonString;
    }

    /// <summary>
    /// Gets the result that accepts the message; it is the default value.
    /// </summary>
    public static MqttPublishResult Accepted => default;

    /// <summary>
    /// Gets a value indicating whether the message is accepted.
    /// </summary>
    internal bool IsAccepted => (byte)ReasonCode < 0x80;

    /// <summary>
    /// Gets the reason code: below 0x80 for an acceptance.
    /// </summary>
    internal MqttReasonCode ReasonCode { get; }

    /// <summary>
    /// Gets the reason string of a refusal, at most <see cref="ServerLimits.MaxReasonStringBytes"/> bytes, or
    /// <see langword="null"/>.
    /// </summary>
    internal string? ReasonString { get; }

    /// <summary>
    /// Refuses the message; for QoS 0 the server drops it, for QoS 1 and 2 it answers with the reason code.
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
    /// <exception cref="ArgumentException">
    /// The reason string holds U+0000 or an unpaired surrogate, which no packet can carry.
    /// </exception>
    public static MqttPublishResult Refuse(MqttReasonCode reasonCode, string? reasonString = null)
    {
        if ((byte)reasonCode < 0x80)
        {
            throw new ArgumentOutOfRangeException(paramName: nameof(reasonCode), actualValue: reasonCode,
                message: "A refusal needs a reason code of 0x80 or above.");
        }

        ReasonStrings.ThrowIfNotEncodable(reasonString: reasonString, paramName: nameof(reasonString));

        return new MqttPublishResult(reasonCode: reasonCode, reasonString: ReasonStrings.Cut(reasonString: reasonString));
    }
}
