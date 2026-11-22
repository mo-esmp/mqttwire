using System.Text;
using Microsoft.AspNetCore.Http;
using MqttWire.Server;

namespace MqttWire.Tests.Server;

/// <summary>
/// A handler for the server tests that records every call in <see cref="Calls"/> and answers through settable
/// delegates. A delegate left <see langword="null"/> accepts: the connection, the admission, the subscription at
/// the QoS asked for, and the publish. Safe to call from several threads at once.
/// </summary>
internal sealed class RecordingHandler : IMqttWireHandler
{
    /// <summary>
    /// Guards <see cref="Calls"/>, <see cref="Admitted"/> and <see cref="_byContext"/>.
    /// </summary>
    private readonly Lock _gate = new();

    /// <summary>
    /// Every connection <see cref="OnAdmit"/> was called with, by the HTTP context of its request, which the
    /// connection itself lets go of when its run ends.
    /// </summary>
    private readonly Dictionary<HttpContext, MqttWireConnection> _byContext = [];

    /// <summary>
    /// Answers <see cref="OnPublish"/> for a test that sets <see cref="Publish"/>.
    /// </summary>
    /// <param name="connection">
    /// The publishing connection.
    /// </param>
    /// <param name="publish">
    /// The message, valid only during the call.
    /// </param>
    /// <returns>
    /// The answer to the publish.
    /// </returns>
    public delegate MqttPublishResult PublishCallback(MqttWireConnection connection, in MqttPublish publish);

    /// <summary>
    /// Gets every call, oldest first, as text: <c>"OnConnectAsync c1"</c>, <c>"OnAdmit c1 replaces=-"</c>,
    /// <c>"OnSubscribe c1 a/b q1"</c>, <c>"OnUnsubscribe c1 a/b"</c>, <c>"OnPublish c1 a/b hi q0 retain=0"</c> and
    /// <c>"OnClosed c1 TakenOver"</c>.
    /// </summary>
    public List<string> Calls { get; } = [];

    /// <summary>
    /// Gets every connection <see cref="OnAdmit"/> was called with, oldest first.
    /// </summary>
    public List<MqttWireConnection> Admitted { get; } = [];

    /// <summary>
    /// Gets or sets the answer to <see cref="OnConnectAsync"/>, or <see langword="null"/> to accept with no state.
    /// </summary>
    public Func<MqttConnectRequest, CancellationToken, ValueTask<MqttConnectResult>>? Connect { get; set; }

    /// <summary>
    /// Gets or sets the answer to <see cref="OnAdmit"/>, or <see langword="null"/> to accept.
    /// </summary>
    public Func<MqttWireConnection, MqttWireConnection?, MqttAdmitResult>? Admit { get; set; }

    /// <summary>
    /// Gets or sets the answer to <see cref="OnSubscribe"/>, or <see langword="null"/> to grant the QoS asked for.
    /// </summary>
    public Func<MqttWireConnection, string, MqttQoS, MqttSubscribeResult>? Subscribe { get; set; }

    /// <summary>
    /// Gets or sets what <see cref="OnUnsubscribe"/> does besides recording, or <see langword="null"/> for nothing.
    /// </summary>
    public Action<MqttWireConnection, string>? Unsubscribe { get; set; }

    /// <summary>
    /// Gets or sets the answer to <see cref="OnPublish"/>, or <see langword="null"/> to accept.
    /// </summary>
    public PublishCallback? Publish { get; set; }

    /// <summary>
    /// Gets or sets what <see cref="OnClosed"/> does besides recording, or <see langword="null"/> for nothing.
    /// </summary>
    public Action<MqttWireConnection, MqttCloseReason>? Closed { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether calls are recorded in <see cref="Calls"/>; <see langword="true"/> by
    /// default. A test that measures what the server allocates turns it off, so that the handler's own text is not
    /// counted; the connections are still kept in <see cref="Admitted"/>.
    /// </summary>
    public bool RecordsCalls { get; set; } = true;

    /// <summary>
    /// Finds the connection <see cref="OnAdmit"/> was called with for a request, also after that connection has
    /// ended.
    /// </summary>
    /// <param name="httpContext">
    /// The HTTP context of the request.
    /// </param>
    /// <returns>
    /// The connection, or <see langword="null"/> when <see cref="OnAdmit"/> has not seen one for the request.
    /// </returns>
    public MqttWireConnection? FindAdmitted(HttpContext httpContext)
    {
        lock (_gate)
        {
            return _byContext.GetValueOrDefault(httpContext);
        }
    }

    /// <summary>
    /// Forgets every call and every connection recorded so far and gives their storage back, for a test that reads
    /// the heap and must not count the handler's own records.
    /// </summary>
    public void Forget()
    {
        lock (_gate)
        {
            Calls.Clear();
            Calls.TrimExcess();
            Admitted.Clear();
            Admitted.TrimExcess();
            _byContext.Clear();
            _byContext.TrimExcess();
        }
    }

    /// <summary>
    /// Copies <see cref="Calls"/> under the handler's lock, for a test that reads the calls while server threads may
    /// still add to them.
    /// </summary>
    /// <returns>
    /// Every call so far, oldest first.
    /// </returns>
    public List<string> CallsNow()
    {
        lock (_gate)
        {
            return [.. Calls];
        }
    }

    /// <summary>
    /// Records the call and answers with <see cref="Connect"/>, or accepts.
    /// </summary>
    /// <param name="request">
    /// The CONNECT.
    /// </param>
    /// <param name="cancellationToken">
    /// Passed to <see cref="Connect"/>.
    /// </param>
    /// <returns>
    /// The answer of <see cref="Connect"/>, or an acceptance with no state.
    /// </returns>
    public ValueTask<MqttConnectResult> OnConnectAsync(MqttConnectRequest request, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        Record($"OnConnectAsync {request.ClientId}");
        return Connect?.Invoke(request, cancellationToken) ?? ValueTask.FromResult(MqttConnectResult.Accept());
    }

    /// <summary>
    /// Records the call and the connection, and answers with <see cref="Admit"/>, or accepts.
    /// </summary>
    /// <param name="connection">
    /// The new connection.
    /// </param>
    /// <param name="replaces">
    /// The connection that has the client id now, or <see langword="null"/>; recorded as <c>-</c>.
    /// </param>
    /// <returns>
    /// The answer of <see cref="Admit"/>, or <see cref="MqttAdmitResult.Accepted"/>.
    /// </returns>
    public MqttAdmitResult OnAdmit(MqttWireConnection connection, MqttWireConnection? replaces)
    {
        ArgumentNullException.ThrowIfNull(connection);
        lock (_gate)
        {
            Admitted.Add(connection);
            if (connection.HttpContext is { } httpContext)
            {
                _byContext[httpContext] = connection;
            }
        }

        Record($"OnAdmit {connection.ClientId} replaces={replaces?.ClientId ?? "-"}");
        return Admit?.Invoke(connection, replaces) ?? MqttAdmitResult.Accepted;
    }

    /// <summary>
    /// Records the call and answers with <see cref="Subscribe"/>, or grants the QoS asked for.
    /// </summary>
    /// <param name="connection">
    /// The subscribing connection.
    /// </param>
    /// <param name="topicFilter">
    /// The topic filter.
    /// </param>
    /// <param name="requested">
    /// The QoS asked for.
    /// </param>
    /// <returns>
    /// The answer of <see cref="Subscribe"/>, or a grant of <paramref name="requested"/>.
    /// </returns>
    public MqttSubscribeResult OnSubscribe(MqttWireConnection connection, string topicFilter, MqttQoS requested)
    {
        ArgumentNullException.ThrowIfNull(connection);
        Record($"OnSubscribe {connection.ClientId} {topicFilter} q{(int)requested}");
        return Subscribe?.Invoke(connection, topicFilter, requested) ?? MqttSubscribeResult.Granted(requested);
    }

    /// <summary>
    /// Records the call and runs <see cref="Unsubscribe"/>.
    /// </summary>
    /// <param name="connection">
    /// The connection that unsubscribed.
    /// </param>
    /// <param name="topicFilter">
    /// The topic filter removed.
    /// </param>
    public void OnUnsubscribe(MqttWireConnection connection, string topicFilter)
    {
        ArgumentNullException.ThrowIfNull(connection);
        Record($"OnUnsubscribe {connection.ClientId} {topicFilter}");
        Unsubscribe?.Invoke(connection, topicFilter);
    }

    /// <summary>
    /// Records the call, with the payload as UTF-8 text, and answers with <see cref="Publish"/>, or accepts.
    /// </summary>
    /// <param name="connection">
    /// The publishing connection.
    /// </param>
    /// <param name="publish">
    /// The message.
    /// </param>
    /// <returns>
    /// The answer of <see cref="Publish"/>, or <see cref="MqttPublishResult.Accepted"/>.
    /// </returns>
    public MqttPublishResult OnPublish(MqttWireConnection connection, in MqttPublish publish)
    {
        ArgumentNullException.ThrowIfNull(connection);
        if (RecordsCalls)
        {
            string payload = Encoding.UTF8.GetString(publish.Payload);
            Record($"OnPublish {connection.ClientId} {publish.GetTopic()} {payload} q{(int)publish.QoS} retain={(publish.Retain ? 1 : 0)}");
        }

        return Publish is null ? MqttPublishResult.Accepted : Publish(connection, publish);
    }

    /// <summary>
    /// Records the call and runs <see cref="Closed"/>.
    /// </summary>
    /// <param name="connection">
    /// The connection that ended.
    /// </param>
    /// <param name="reason">
    /// Why it ended.
    /// </param>
    public void OnClosed(MqttWireConnection connection, MqttCloseReason reason)
    {
        ArgumentNullException.ThrowIfNull(connection);
        Record($"OnClosed {connection.ClientId} {reason}");
        Closed?.Invoke(connection, reason);
    }

    /// <summary>
    /// Adds a call to <see cref="Calls"/>, unless <see cref="RecordsCalls"/> is off.
    /// </summary>
    /// <param name="call">
    /// The call as text.
    /// </param>
    private void Record(string call)
    {
        if (!RecordsCalls)
        {
            return;
        }

        lock (_gate)
        {
            Calls.Add(call);
        }
    }
}
