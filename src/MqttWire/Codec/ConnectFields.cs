namespace MqttWire.Codec;

/// <summary>
/// What the client writes into a CONNECT: protocol 5, clean start, no Will, and Receive Maximum as the only property.
/// </summary>
internal readonly ref struct ConnectFields
{
    /// <summary>
    /// The client identifier.
    /// </summary>
    public required string ClientId { get; init; }

    /// <summary>
    /// The user name, or <see langword="null"/> to write neither the flag nor the field.
    /// </summary>
    public string? UserName { get; init; }

    /// <summary>
    /// The password, or <see langword="null"/> to write neither the flag nor the field.
    /// </summary>
    public byte[]? Password { get; init; }

    /// <summary>
    /// The keep-alive interval in seconds.
    /// </summary>
    public ushort KeepAliveSeconds { get; init; }

    /// <summary>
    /// The Receive Maximum property: how many QoS 1 and 2 publications the client accepts at once. The client
    /// sends 1,024 (MQTT 5, 3.1.2.11.3). The standard forbids 0, so the encoder refuses it.
    /// </summary>
    public ushort ReceiveMaximum { get; init; }
}
