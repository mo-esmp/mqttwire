using System.Runtime.InteropServices;
using MqttWire.Codec;
using MqttWire.Server.Internal;

namespace MqttWire.Server;

/// <summary>
/// The answer to <see cref="IMqttWireHandler.OnConnectAsync"/>: accept, with the state object of the host for the
/// connection and optional user properties for CONNACK; or refuse, with a reason code and a reason string. Only
/// <see cref="Accept"/> accepts: the default value is a refusal with 0x87 (not authorized) and no reason string, so
/// a handler that returns <c>default</c> authenticates nobody.
/// </summary>
[StructLayout(LayoutKind.Auto)]
public readonly struct MqttConnectResult
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
    /// The reason code: below 0x80 accepts.
    /// </param>
    /// <param name="reasonString">
    /// The reason string of a refusal, already cut.
    /// </param>
    /// <param name="state">
    /// The state object of an acceptance.
    /// </param>
    /// <param name="userProperties">
    /// The user properties of an acceptance.
    /// </param>
    private MqttConnectResult(MqttReasonCode reasonCode, string? reasonString, object? state,
        MqttWireUserProperty[]? userProperties)
    {
        _created = true;
        _reasonCode = reasonCode;
        ReasonString = reasonString;
        State = state;
        UserProperties = userProperties;
    }

    /// <summary>
    /// Gets a value indicating whether the client is accepted.
    /// </summary>
    internal bool IsAccepted => (byte)ReasonCode < 0x80;

    /// <summary>
    /// Gets the reason code: below 0x80 for an acceptance; 0x87 for the default value.
    /// </summary>
    internal MqttReasonCode ReasonCode => _created ? _reasonCode : MqttReasonCode.NotAuthorized;

    /// <summary>
    /// Gets the reason string of a refusal, at most <see cref="ServerLimits.MaxReasonStringBytes"/> bytes, or
    /// <see langword="null"/>.
    /// </summary>
    internal string? ReasonString { get; }

    /// <summary>
    /// Gets the state object of an acceptance, or <see langword="null"/>.
    /// </summary>
    internal object? State { get; }

    /// <summary>
    /// Gets the user properties of an acceptance for CONNACK, or <see langword="null"/>.
    /// </summary>
    internal MqttWireUserProperty[]? UserProperties { get; }

    /// <summary>
    /// Accepts the client.
    /// </summary>
    /// <param name="state">
    /// The object the host keeps for the connection; it comes back as <see cref="MqttWireConnection.State"/>.
    /// The server never looks at it.
    /// </param>
    /// <param name="userProperties">
    /// User properties to send in CONNACK, or <see langword="null"/> for none. The server keeps this array until
    /// it has written CONNACK, so the caller must not change it.
    /// </param>
    /// <returns>
    /// An acceptance.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// The user properties are over 1,024 bytes encoded, or a name or value cannot be carried (U+0000, an unpaired
    /// surrogate or over 65,535 bytes).
    /// </exception>
    public static MqttConnectResult Accept(object? state = null, MqttWireUserProperty[]? userProperties = null)
    {
        if (userProperties is { Length: > 0 }
            && PropertyWriter.UserPropertiesSize(properties: userProperties) > ServerLimits.MaxConnAckUserPropertyBytes)
        {
            throw new ArgumentException(
                message: $"The user properties are over {ServerLimits.MaxConnAckUserPropertyBytes} bytes encoded.",
                paramName: nameof(userProperties));
        }

        return new MqttConnectResult(reasonCode: MqttReasonCode.Success, reasonString: null, state: state,
            userProperties: userProperties);
    }

    /// <summary>
    /// Refuses the client; the server sends CONNACK with the reason code and closes the connection.
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
    public static MqttConnectResult Refuse(MqttReasonCode reasonCode, string? reasonString = null)
    {
        if ((byte)reasonCode < 0x80)
        {
            throw new ArgumentOutOfRangeException(paramName: nameof(reasonCode), actualValue: reasonCode,
                message: "A refusal needs a reason code of 0x80 or above.");
        }

        ReasonStrings.ThrowIfNotEncodable(reasonString: reasonString, paramName: nameof(reasonString));

        return new MqttConnectResult(reasonCode: reasonCode, reasonString: ReasonStrings.Cut(reasonString: reasonString),
            state: null, userProperties: null);
    }
}
