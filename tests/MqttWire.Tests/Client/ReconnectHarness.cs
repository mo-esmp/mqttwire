using System.Buffers;
using Microsoft.Extensions.Time.Testing;
using MqttWire.Codec;
using MqttWire.Tests.Link;

namespace MqttWire.Tests.Client;

/// <summary>
/// A reconnecting client over in-memory links with a fake clock and a random part of 0.5 (a factor of exactly 1),
/// and what its callbacks were called with, for the reconnecting client's tests. Unless <see cref="Create"/> is told
/// otherwise, every attempt's options are <c>ws://test/mqtt</c>, client id <c>c1</c>, user <c>u</c>, password
/// <c>p</c> and keep-alive 15 s, so each CONNECT is <see cref="ClientPackets.Connect"/>.
/// </summary>
internal sealed class ReconnectHarness
{
    /// <summary>
    /// The number of times the client asked for options.
    /// </summary>
    private int _optionsCalls;

    /// <summary>
    /// Creates the harness's parts; <see cref="Create"/> adds the client.
    /// </summary>
    private ReconnectHarness()
    {
        Time = new FakeTimeProvider();
        Start = Time.GetUtcNow();
        Clock = new TimerRecordingClock(Time);
        Links = new MemoryLinkConnector();
        Connector = new RecordingConnector(Links, Time);
    }

    /// <summary>
    /// Gets the longest a test waits in real time for something that should happen at once.
    /// </summary>
    public static TimeSpan Timeout { get; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Gets the fake clock; nothing happens in real time.
    /// </summary>
    public FakeTimeProvider Time { get; }

    /// <summary>
    /// Gets the clock the client and its connections are given, over <see cref="Time"/>, which records every timer.
    /// </summary>
    public TimerRecordingClock Clock { get; }

    /// <summary>
    /// Gets the connector that hands out the links; a test scripts failures and holds connects on it.
    /// </summary>
    public MemoryLinkConnector Links { get; }

    /// <summary>
    /// Gets the connector the client is given, which records when each attempt was made.
    /// </summary>
    public RecordingConnector Connector { get; }

    /// <summary>
    /// Gets what the callbacks were called with.
    /// </summary>
    public CallbackLog Log { get; } = new();

    /// <summary>
    /// Gets the reconnecting client.
    /// </summary>
    public MqttWireReconnectingClient Client { get; private set; } = null!;

    /// <summary>
    /// Gets the number of times the client asked for options.
    /// </summary>
    public int OptionsCalls => Volatile.Read(ref _optionsCalls);

    /// <summary>
    /// Gets the fake time at which the harness was created.
    /// </summary>
    public DateTimeOffset Start { get; }

    /// <summary>
    /// Creates a reconnecting client over in-memory links.
    /// </summary>
    /// <param name="clientOptions">
    /// Builds the options of the given call, numbered from 1; <see langword="null"/> for
    /// <see cref="DefaultOptions"/> every time.
    /// </param>
    /// <param name="onCallback">
    /// Called inside every callback after it was logged, with the log line, so a test can throw or act there; or
    /// <see langword="null"/>.
    /// </param>
    /// <returns>
    /// The harness, with the client not yet started.
    /// </returns>
    public static ReconnectHarness Create(
        Func<int, CancellationToken, ValueTask<MqttWireClientOptions>>? clientOptions = null,
        Action<string>? onCallback = null)
    {
        ReconnectHarness harness = new();
        MqttWireReconnectOptions options = new()
        {
            ClientOptions = cancellationToken =>
            {
                int call = Interlocked.Increment(ref harness._optionsCalls);
                return clientOptions is null
                    ? ValueTask.FromResult(DefaultOptions())
                    : clientOptions(call, cancellationToken);
            },
            OnConnected = info => harness.Record(info.IsReconnect ? "connected again" : "connected first", onCallback),
            OnDisconnected = info => harness.Record($"disconnected {info.Reason}", onCallback),
            OnSubscriptionRefused = (filter, result) => harness.Record($"refused {filter} {result.ReasonCode}", onCallback),
            OnStopped = ack => harness.Record($"stopped {ack.ReasonCode}", onCallback),
            OnMessage = (in MqttPublish message) => harness.Record($"message {message.GetTopic()}", onCallback),
            OnCallbackError = exception => harness.Record($"error {exception.GetType().Name}", onCallback),
        };
        harness.Client = new MqttWireReconnectingClient(options, harness.Connector, harness.Clock, static () => 0.5);
        return harness;
    }

    /// <summary>
    /// Builds the options every attempt uses unless a test says otherwise.
    /// </summary>
    /// <param name="clientId">
    /// The client identifier.
    /// </param>
    /// <returns>
    /// The options, with no <c>OnMessage</c> or <c>OnClosed</c>.
    /// </returns>
    public static MqttWireClientOptions DefaultOptions(string clientId = "c1") => new()
    {
        Uri = new Uri("ws://test/mqtt"),
        ClientId = clientId,
        UserName = "u",
        Password = "p",
        KeepAlive = TimeSpan.FromSeconds(15),
    };

    /// <summary>
    /// Builds a CONNACK with a reason code and no properties.
    /// </summary>
    /// <param name="reasonCode">
    /// The reason code.
    /// </param>
    /// <returns>
    /// The packet.
    /// </returns>
    public static byte[] ConnAck(byte reasonCode) => [0x20, 0x03, 0x00, reasonCode, 0x00];

    /// <summary>
    /// Builds the SUBSCRIBE the client writes for one filter, with no properties and no other option.
    /// </summary>
    /// <param name="packetId">
    /// The packet identifier.
    /// </param>
    /// <param name="filter">
    /// The topic filter, ASCII and shorter than 100 characters.
    /// </param>
    /// <param name="qos">
    /// The QoS asked for.
    /// </param>
    /// <returns>
    /// The packet.
    /// </returns>
    public static byte[] Subscribe(ushort packetId, string filter, MqttQoS qos)
    {
        List<byte> packet = [0x82, (byte)(6 + filter.Length), (byte)(packetId >> 8), (byte)packetId, 0x00, 0x00, (byte)filter.Length];
        foreach (char c in filter)
        {
            packet.Add((byte)c);
        }

        packet.Add((byte)qos);
        return [.. packet];
    }

    /// <summary>
    /// Builds the UNSUBSCRIBE the client writes for one filter, with no properties.
    /// </summary>
    /// <param name="packetId">
    /// The packet identifier.
    /// </param>
    /// <param name="filter">
    /// The topic filter, ASCII and shorter than 100 characters.
    /// </param>
    /// <returns>
    /// The packet.
    /// </returns>
    public static byte[] Unsubscribe(ushort packetId, string filter)
    {
        List<byte> packet = [0xA2, (byte)(5 + filter.Length), (byte)(packetId >> 8), (byte)packetId, 0x00, 0x00, (byte)filter.Length];
        foreach (char c in filter)
        {
            packet.Add((byte)c);
        }

        return [.. packet];
    }

    /// <summary>
    /// Builds a SUBACK for one filter, with no properties.
    /// </summary>
    /// <param name="packetId">
    /// The packet identifier.
    /// </param>
    /// <param name="reasonCode">
    /// The granted QoS, or a refusal of 0x80 or more.
    /// </param>
    /// <returns>
    /// The packet.
    /// </returns>
    public static byte[] SubAck(ushort packetId, byte reasonCode) => [0x90, 0x04, (byte)(packetId >> 8), (byte)packetId, 0x00, reasonCode];

    /// <summary>
    /// Builds a successful UNSUBACK for one filter, with no properties.
    /// </summary>
    /// <param name="packetId">
    /// The packet identifier.
    /// </param>
    /// <returns>
    /// The packet.
    /// </returns>
    public static byte[] UnsubAck(ushort packetId) => [0xB0, 0x04, (byte)(packetId >> 8), (byte)packetId, 0x00, 0x00];

    /// <summary>
    /// Splits what a client wrote into whole packets.
    /// </summary>
    /// <param name="written">
    /// The bytes written, which must end with a whole packet.
    /// </param>
    /// <returns>
    /// Each packet's bytes, oldest first.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// The bytes end with part of a packet.
    /// </exception>
    public static List<byte[]> Packets(byte[] written)
    {
        List<byte[]> packets = [];
        ReadOnlySpan<byte> rest = written;
        while (!rest.IsEmpty)
        {
            if (PacketHeader.TryReadPacket(rest, out PacketHeader header) != OperationStatus.Done)
            {
                throw new InvalidOperationException("The bytes end with part of a packet.");
            }

            packets.Add(rest[..header.TotalLength].ToArray());
            rest = rest[header.TotalLength..];
        }

        return packets;
    }

    /// <summary>
    /// Gets when each attempt to open a link was made, from the creation of the harness.
    /// </summary>
    /// <returns>
    /// The fake time of each attempt since <see cref="Start"/>, oldest first.
    /// </returns>
    public List<TimeSpan> AttemptTimes()
    {
        List<TimeSpan> times = [];
        foreach (DateTimeOffset attempt in Connector.Attempts)
        {
            times.Add(attempt - Start);
        }

        return times;
    }

    /// <summary>
    /// Waits for an attempt to open a link and returns the newest link.
    /// </summary>
    /// <param name="attempt">
    /// The attempt, from 1, which must hand out a link.
    /// </param>
    /// <returns>
    /// The link that attempt opened.
    /// </returns>
    public async Task<MemoryLink> LinkAsync(int attempt)
    {
        await Connector.WhenAttemptsAsync(attempt);
        return Links.Links[^1];
    }

    /// <summary>
    /// Waits for the next back-off or deadline timer of a due time to start, then moves the fake clock by it.
    /// </summary>
    /// <param name="dueTime">
    /// The timer's due time.
    /// </param>
    /// <returns>
    /// A task that completes once the clock has moved and the timer has fired.
    /// </returns>
    public async Task AdvanceAsync(TimeSpan dueTime)
    {
        await Clock.WhenTimerAsync(dueTime);
        Time.Advance(dueTime);
    }

    /// <summary>
    /// Connects the first attempt: starts the client, accepts its CONNECT and waits for the start to complete.
    /// </summary>
    /// <returns>
    /// The first link.
    /// </returns>
    public async Task<MemoryLink> StartConnectedAsync()
    {
        Task<MqttConnectAck> start = Client.StartAsync();
        MemoryLink link = await LinkAsync(1);
        link.Deliver(ClientPackets.ConnAckAccepted);
        (await start.WaitAsync(Timeout)).IsConnected.ShouldBeTrue();
        return link;
    }

    /// <summary>
    /// Subscribes through the reconnecting client while connected and grants the subscription.
    /// </summary>
    /// <param name="link">
    /// The current link.
    /// </param>
    /// <param name="filter">
    /// The topic filter.
    /// </param>
    /// <param name="qos">
    /// The QoS asked for and granted.
    /// </param>
    /// <param name="packetId">
    /// The packet identifier the client uses for it.
    /// </param>
    /// <returns>
    /// A task that completes once the subscription is granted.
    /// </returns>
    public async Task SubscribeAsync(MemoryLink link, string filter, MqttQoS qos, ushort packetId)
    {
        int writes = link.WriteCount;
        ValueTask<MqttRequestResult> subscribe = Client.SubscribeAsync(filter, qos);
        await link.WhenWrittenAsync(writes + 1);
        link.Deliver(SubAck(packetId, (byte)qos));
        (await subscribe.AsTask().WaitAsync(Timeout)).IsSuccess.ShouldBeTrue();
    }

    /// <summary>
    /// Disposes the client and lets every link end from the peer, so a close in progress finishes.
    /// </summary>
    /// <returns>
    /// A task that completes once the client is disposed.
    /// </returns>
    public async Task StopAsync()
    {
        Task dispose = Client.DisposeAsync().AsTask();
        foreach (MemoryLink link in Links.Links)
        {
            link.EndFromPeer();
        }

        await dispose.WaitAsync(Timeout);
    }

    /// <summary>
    /// Disposes the client while it is connected over a link, and ends the link from the peer only once the client
    /// has sent its DISCONNECT, so the end is reported as <see cref="MqttClientCloseReason.Disposed"/>.
    /// </summary>
    /// <param name="link">
    /// The current link.
    /// </param>
    /// <returns>
    /// A task that completes once the client is disposed.
    /// </returns>
    public async Task DisposeConnectedAsync(MemoryLink link)
    {
        int writes = link.WriteCount;
        Task dispose = Client.DisposeAsync().AsTask();
        await link.WhenWrittenAsync(writes + 1);
        link.EndFromPeer();
        await dispose.WaitAsync(Timeout);
    }

    /// <summary>
    /// Logs a callback and calls the test's hook inside it.
    /// </summary>
    /// <param name="entry">
    /// The log line.
    /// </param>
    /// <param name="onCallback">
    /// The test's hook, or <see langword="null"/>.
    /// </param>
    private void Record(string entry, Action<string>? onCallback)
    {
        Log.Enter(entry);
        try
        {
            onCallback?.Invoke(entry);
        }
        finally
        {
            Log.Exit();
        }
    }
}
