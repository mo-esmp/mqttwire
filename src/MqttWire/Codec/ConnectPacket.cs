using System.Runtime.InteropServices;

namespace MqttWire.Codec;

/// <summary>
/// A decoded CONNECT packet: views over the bytes it was decoded from, so it is only valid while they are.
/// </summary>
[StructLayout(LayoutKind.Auto)]
internal readonly ref struct ConnectPacket
{
    /// <summary>
    /// The client identifier as UTF-8 bytes; empty when the client sent none, which the server decides about.
    /// </summary>
    public ReadOnlySpan<byte> ClientId { get; init; }

    /// <summary>
    /// Whether the user name flag was set; the name itself may be empty.
    /// </summary>
    public bool HasUserName { get; init; }

    /// <summary>
    /// The user name as UTF-8 bytes; empty when <see cref="HasUserName"/> is <see langword="false"/>.
    /// </summary>
    public ReadOnlySpan<byte> UserName { get; init; }

    /// <summary>
    /// Whether the password flag was set; the password itself may be empty.
    /// </summary>
    public bool HasPassword { get; init; }

    /// <summary>
    /// The password bytes; empty when <see cref="HasPassword"/> is <see langword="false"/>.
    /// </summary>
    public ReadOnlySpan<byte> Password { get; init; }

    /// <summary>
    /// The keep-alive interval in seconds; 0 means the client asks for none.
    /// </summary>
    public ushort KeepAliveSeconds { get; init; }

    /// <summary>
    /// Whether the client asks to start a new session rather than resume one.
    /// </summary>
    public bool CleanStart { get; init; }

    /// <summary>
    /// Whether the packet carries a Will; its content is validated but not exposed.
    /// </summary>
    public bool HasWill { get; init; }

    /// <summary>
    /// The validated CONNECT properties.
    /// </summary>
    public PropertyBlock Properties { get; init; }
}
