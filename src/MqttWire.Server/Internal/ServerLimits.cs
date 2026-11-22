namespace MqttWire.Server.Internal;

/// <summary>
/// The fixed limits and deadlines of the server. They are not options: each is a protection with one value.
/// </summary>
internal static class ServerLimits
{
    /// <summary>
    /// How often the sweep looks for connections whose deadline has passed.
    /// </summary>
    public static readonly TimeSpan SweepInterval = TimeSpan.FromSeconds(2);

    /// <summary>
    /// How long a new connection has to send its CONNECT packet.
    /// </summary>
    public static readonly TimeSpan ConnectDeadline = TimeSpan.FromSeconds(10);

    /// <summary>
    /// How long the authentication callback of a connection may take.
    /// </summary>
    public static readonly TimeSpan AuthenticationDeadline = TimeSpan.FromSeconds(10);

    /// <summary>
    /// How long a read buffer may be held without a complete packet: from its rent, or, for a packet that follows
    /// another in the same read, from that read.
    /// </summary>
    public static readonly TimeSpan PartialPacketDeadline = TimeSpan.FromSeconds(10);

    /// <summary>
    /// How long one write to a client may take.
    /// </summary>
    public static readonly TimeSpan WriteDeadline = TimeSpan.FromSeconds(15);

    /// <summary>
    /// How long a client may take to acknowledge a packet the server sent.
    /// </summary>
    public static readonly TimeSpan AcknowledgementDeadline = TimeSpan.FromSeconds(30);

    /// <summary>
    /// How long the server waits for a closing handshake before it drops the link.
    /// </summary>
    public static readonly TimeSpan CloseDeadline = TimeSpan.FromSeconds(2);

    /// <summary>
    /// The longest keep-alive interval the server accepts, in seconds; a client that asks for more, or for none,
    /// is given this one.
    /// </summary>
    public const int MaxKeepAliveSeconds = 60;

    /// <summary>
    /// The numerator of the factor applied to the keep-alive interval before a silent client is dropped (3/2).
    /// </summary>
    public const int KeepAliveFactorNumerator = 3;

    /// <summary>
    /// The denominator of the factor applied to the keep-alive interval before a silent client is dropped (3/2).
    /// </summary>
    public const int KeepAliveFactorDenominator = 2;

    /// <summary>
    /// The Receive Maximum the server announces: how many unacknowledged QoS 1 and 2 packets a client may have
    /// in flight to it.
    /// </summary>
    public const int ReceiveMaximum = 256;

    /// <summary>
    /// The most control packets (acknowledgements and the like) that may wait to be written to one client.
    /// </summary>
    public const int MaxWaitingControlPackets = 1_024;

    /// <summary>
    /// The largest packet, other than PUBLISH, that the server reads, in bytes.
    /// </summary>
    public const int MaxControlPacketBytes = 16_384;

    /// <summary>
    /// The largest buffer used to encode one packet for sending, in bytes.
    /// </summary>
    public const int MaxEncodingBufferBytes = 65_536;

    /// <summary>
    /// The size of a whole PUBLISH, in bytes, up to which a message's payload is copied behind its header into one
    /// encoding buffer and written at once; a larger one is written from the host's memory, in pieces, after its
    /// header.
    /// </summary>
    public const int CopyThresholdBytes = 16_384;

    /// <summary>
    /// The size of the buffer that holds the first read of a connection, in bytes.
    /// </summary>
    public const int FirstReadBufferBytes = 4_096;

    /// <summary>
    /// The room reserved for the fixed header, topic and properties of a PUBLISH packet, in bytes, on top of its
    /// payload limit.
    /// </summary>
    public const int PublishOverheadBytes = 5_120;

    /// <summary>
    /// The factor by which a packet that is to be skipped may exceed its limit before the connection is ended
    /// instead.
    /// </summary>
    public const int SkipFactor = 16;

    /// <summary>
    /// The smallest Maximum Packet Size a client may announce, in bytes; a CONNECT that announces a smaller one is
    /// refused with CONNACK 0x95, so that every control packet the server sends fits.
    /// </summary>
    public const int MinClientMaximumPacketSize = 4_096;

    /// <summary>
    /// The longest Reason String the server sends, in bytes.
    /// </summary>
    public const int MaxReasonStringBytes = 256;

    /// <summary>
    /// The most bytes of user properties the server adds to a CONNACK packet.
    /// </summary>
    public const int MaxConnAckUserPropertyBytes = 1_024;

    /// <summary>
    /// The longest client identifier the server accepts, in bytes.
    /// </summary>
    public const int MaxClientIdBytes = 256;

    /// <summary>
    /// The longest topic a host may send to (<see cref="MqttTopic"/>), in UTF-8 bytes: 9 fewer than MQTT allows, so
    /// that the header of every PUBLISH fits one buffer of <see cref="MaxEncodingBufferBytes"/>. What a client sends
    /// is bounded by the packet limits instead.
    /// </summary>
    public const int MaxTopicBytes = 65_526;

    /// <summary>
    /// The most idle objects of each kind (queues of waiting deliveries, exchange records) the server's pools keep
    /// for reuse.
    /// </summary>
    public const int MaxPooledObjects = 1_024;

    /// <summary>
    /// The largest queue of waiting deliveries, in slots, that goes back to the server's pool; a larger one, grown by
    /// a burst, is left to the garbage collector, so the pool never keeps a burst's storage.
    /// </summary>
    public const int MaxPooledQueueCapacity = 64;
}
