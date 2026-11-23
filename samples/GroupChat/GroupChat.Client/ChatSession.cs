using System.Globalization;
using System.Text;
using MqttWire;

namespace GroupChat.Client;

/// <summary>
/// One member's connection to the chat: an <see cref="MqttWireReconnectingClient"/>, the groups joined through it
/// and the messages received. Joining a group is subscribing to its topic, <c>groups/&lt;name&gt;</c>, and sending
/// is publishing to it. When the connection is lost the client connects again by itself and joins the same groups
/// again; messages sent meanwhile are not brought back.
/// </summary>
/// <remarks>
/// In a browser the WebSocket is the browser's own: a client cannot set request headers, TLS or proxy settings on
/// it, and the WebSocket keep-alive option is not supported there (design 6.3), so the session sets none of them.
/// MQTT's own keep-alive, which the client sends as packets, works as everywhere. Every callback runs on the
/// browser's one thread.
/// </remarks>
internal sealed class ChatSession : IAsyncDisposable
{
    /// <summary>
    /// What every group's topic starts with; the chat server grants no other subscription.
    /// </summary>
    private const string GroupPrefix = "groups/";

    /// <summary>
    /// The most messages kept for the page; older ones are dropped.
    /// </summary>
    private const int MaxMessages = 200;

    /// <summary>
    /// The reconnecting client.
    /// </summary>
    private readonly MqttWireReconnectingClient _client;

    /// <summary>
    /// The WebSocket address of the chat server's MQTT endpoint.
    /// </summary>
    private readonly Uri _endpoint;

    /// <summary>
    /// The client id: a new GUID for every session, kept for all of its connections. It is made apart from the
    /// name, so that two tabs with one name are two connections and neither takes the other over, and so that
    /// nothing a person types can make an id the server refuses.
    /// </summary>
    private readonly string _clientId;

    /// <summary>
    /// The groups joined, by name.
    /// </summary>
    private readonly List<string> _groups = [];

    /// <summary>
    /// The messages received, oldest first, at most <see cref="MaxMessages"/>.
    /// </summary>
    private readonly List<string> _messages = [];

    /// <summary>
    /// Creates a session; nothing is sent until <see cref="StartAsync"/>.
    /// </summary>
    /// <param name="pageAddress">
    /// The address of the page, which the chat server serves: the MQTT endpoint is <c>/mqtt</c> on the same host,
    /// over <c>wss</c> when the page came over <c>https</c> and over <c>ws</c> otherwise.
    /// </param>
    /// <param name="name">
    /// The member's name, which becomes the user name of the CONNECT.
    /// </param>
    public ChatSession(Uri pageAddress, string name)
    {
        ArgumentNullException.ThrowIfNull(pageAddress);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        Name = name;
        _clientId = string.Create(CultureInfo.InvariantCulture, $"chat-{Guid.NewGuid():N}");
        _endpoint = new UriBuilder(pageAddress)
        {
            Scheme = string.Equals(pageAddress.Scheme, Uri.UriSchemeHttps, StringComparison.Ordinal) ? "wss" : "ws",
            Path = "/mqtt",
            Query = string.Empty,
            Fragment = string.Empty,
        }.Uri;
        _client = new MqttWireReconnectingClient(new MqttWireReconnectOptions
        {
            ClientOptions = BuildOptions,
            OnConnected = _ => SetStatus("connected"),
            OnDisconnected = _ => SetStatus("reconnecting"),
            OnStopped = ack => SetStatus("stopped", ack.ReasonString ?? ack.ReasonCode.ToString()),
            OnSubscriptionRefused = (topicFilter, _) => Forget(topicFilter),
            OnMessage = Receive,
        });
    }

    /// <summary>
    /// Raised after the status, the groups or the messages changed, for the page to render again.
    /// </summary>
    public event Action? Changed;

    /// <summary>
    /// Gets the member's name.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Gets the state of the connection: <c>connecting</c>, <c>connected</c>, <c>reconnecting</c>, or
    /// <c>stopped</c> once the server refused the name for good.
    /// </summary>
    public string Status { get; private set; } = "connecting";

    /// <summary>
    /// Gets why the session stopped, or <see langword="null"/> while it has not.
    /// </summary>
    public string? StatusDetail { get; private set; }

    /// <summary>
    /// Gets the names of the groups joined, in the order they were joined.
    /// </summary>
    public IReadOnlyList<string> Groups => _groups;

    /// <summary>
    /// Gets the messages received, oldest first, each as <c>[group] name: text</c>.
    /// </summary>
    public IReadOnlyList<string> Messages => _messages;

    /// <summary>
    /// Starts connecting. A connection that cannot be made, or is lost later, is retried with a growing delay.
    /// </summary>
    /// <returns>
    /// A task that completes with the first connection, or with the refusal that stopped the session.
    /// </returns>
    public Task<MqttConnectAck> StartAsync() => _client.StartAsync();

    /// <summary>
    /// Joins a group: subscribes to its topic at QoS 1. The reconnecting client remembers a granted subscription
    /// and makes it again on every later connection.
    /// </summary>
    /// <param name="group">
    /// The group's name, which must not hold <c>/</c>, <c>+</c> or <c>#</c>.
    /// </param>
    /// <returns>
    /// <see langword="null"/> when the group was joined, or was joined already; otherwise why it was not: there is
    /// no connection, the name is not one the server grants, or the connection was lost before the answer.
    /// </returns>
    public async Task<string?> JoinAsync(string group)
    {
        if (string.IsNullOrWhiteSpace(group) || group.AsSpan().IndexOfAny('/', '+', '#') >= 0)
        {
            return "A group's name must not be empty or hold '/', '+' or '#'.";
        }

        if (_groups.Contains(group, StringComparer.Ordinal))
        {
            return null;
        }

        MqttRequestResult result = await _client.SubscribeAsync(GroupPrefix + group, MqttQoS.AtLeastOnce);
        if (!result.IsSuccess)
        {
            return Describe("Not joined", result);
        }

        _groups.Add(group);
        Changed?.Invoke();
        return null;
    }

    /// <summary>
    /// Sends a message to a group: publishes <c>name: text</c> as UTF-8 to the group's topic at QoS 1. The server
    /// sends it to every member, this one included, so the message appears in <see cref="Messages"/> when it comes
    /// back.
    /// </summary>
    /// <param name="group">
    /// The name of a group that was joined.
    /// </param>
    /// <param name="text">
    /// The text.
    /// </param>
    /// <returns>
    /// <see langword="null"/> when the server acknowledged the message; otherwise why it was not sent or not
    /// acknowledged. Nothing is queued while there is no connection, and nothing is sent a second time.
    /// </returns>
    public async Task<string?> SendAsync(string group, string text)
    {
        byte[] payload = Encoding.UTF8.GetBytes($"{Name}: {text}");
        MqttRequestResult result = await _client.PublishAsync(GroupPrefix + group, payload, MqttQoS.AtLeastOnce);
        return result.IsSuccess ? null : Describe("Not sent", result);
    }

    /// <summary>
    /// Ends the session: the connection is closed and no further attempt is made.
    /// </summary>
    /// <returns>
    /// A task that completes when the client has ended.
    /// </returns>
    public ValueTask DisposeAsync() => _client.DisposeAsync();

    /// <summary>
    /// Puts an outcome that is not a success into words.
    /// </summary>
    /// <param name="what">
    /// What did not happen, such as <c>Not sent</c>.
    /// </param>
    /// <param name="result">
    /// The outcome.
    /// </param>
    /// <returns>
    /// The server's reason code when the exchange completed, otherwise the status.
    /// </returns>
    private static string Describe(string what, MqttRequestResult result) =>
        result.Status == MqttRequestStatus.Completed
            ? $"{what}: the server answered {result.ReasonCode}."
            : $"{what}: {result.Status}.";

    /// <summary>
    /// Builds the options of one connection. The reconnecting client calls it before every attempt, which is
    /// where a real application would fetch a fresh token.
    /// </summary>
    /// <param name="cancellationToken">
    /// Not used: the options are built at once.
    /// </param>
    /// <returns>
    /// The options, completed.
    /// </returns>
    private ValueTask<MqttWireClientOptions> BuildOptions(CancellationToken cancellationToken) =>
        ValueTask.FromResult(new MqttWireClientOptions
        {
            Uri = _endpoint,
            ClientId = _clientId,
            UserName = Name,
        });

    /// <summary>
    /// Adds a received message to the list. The payload is valid only during this call, so it is turned into a
    /// string here.
    /// </summary>
    /// <param name="message">
    /// The message.
    /// </param>
    private void Receive(in MqttPublish message)
    {
        string topic = message.GetTopic();
        string group = topic.StartsWith(GroupPrefix, StringComparison.Ordinal) ? topic[GroupPrefix.Length..] : topic;
        if (_messages.Count == MaxMessages)
        {
            _messages.RemoveAt(0);
        }

        _messages.Add($"[{group}] {Encoding.UTF8.GetString(message.Payload)}");
        Changed?.Invoke();
    }

    /// <summary>
    /// Forgets a group whose subscription the server refused when it was made again on a new connection.
    /// </summary>
    /// <param name="topicFilter">
    /// The topic filter that was refused.
    /// </param>
    private void Forget(string topicFilter)
    {
        if (topicFilter.StartsWith(GroupPrefix, StringComparison.Ordinal) && _groups.Remove(topicFilter[GroupPrefix.Length..]))
        {
            Changed?.Invoke();
        }
    }

    /// <summary>
    /// Sets the state of the connection and tells the page.
    /// </summary>
    /// <param name="status">
    /// The new state.
    /// </param>
    /// <param name="detail">
    /// Why, for a session that stopped; otherwise <see langword="null"/>.
    /// </param>
    private void SetStatus(string status, string? detail = null)
    {
        Status = status;
        StatusDetail = detail;
        Changed?.Invoke();
    }
}
