using System.Runtime.InteropServices;
using MqttWire.Server.Internal;

namespace MqttWire.Server;

/// <summary>
/// The answer to <see cref="IMqttWireHandler.OnAdmit"/>: accept or refuse (reason code and string). When the
/// connection to replace is not <see langword="null"/> and the host accepts, the new connection takes over and the
/// old one is closed with the reason "session taken over". The default value is the accepted result.
/// </summary>
[StructLayout(LayoutKind.Auto)]
public readonly struct MqttAdmitResult
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
    private MqttAdmitResult(MqttReasonCode reasonCode, string? reasonString)
    {
        ReasonCode = reasonCode;
        ReasonString = reasonString;
    }

    /// <summary>
    /// Gets the result that lets the client in; it is the default value.
    /// </summary>
    public static MqttAdmitResult Accepted => default;

    /// <summary>
    /// Gets a value indicating whether the client is let in.
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
    /// Refuses the client; the server sends CONNACK with the reason code and closes the connection.
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
    public static MqttAdmitResult Refuse(MqttReasonCode reasonCode, string? reasonString = null)
    {
        if ((byte)reasonCode < 0x80)
        {
            throw new ArgumentOutOfRangeException(paramName: nameof(reasonCode), actualValue: reasonCode,
                message: "A refusal needs a reason code of 0x80 or above.");
        }

        ReasonStrings.ThrowIfNotEncodable(reasonString: reasonString, paramName: nameof(reasonString));

        return new MqttAdmitResult(reasonCode: reasonCode, reasonString: ReasonStrings.Cut(reasonString: reasonString));
    }
}
