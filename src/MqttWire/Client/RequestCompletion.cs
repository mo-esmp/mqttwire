using System.Threading.Tasks.Sources;

namespace MqttWire.Client;

/// <summary>
/// A pooled completion source for one request. It runs its continuation asynchronously, so application code
/// never runs on the read loop, and goes back to the pool it came from when its result is read.
/// </summary>
internal sealed class RequestCompletion : IValueTaskSource<MqttRequestResult>
{
    /// <summary>
    /// The table whose pool this completion returns to.
    /// </summary>
    private readonly OutgoingRecords _owner;

    /// <summary>
    /// The source core; a struct, so it is not readonly.
    /// </summary>
    private ManualResetValueTaskSourceCore<MqttRequestResult> _core = new() { RunContinuationsAsynchronously = true };

    /// <summary>
    /// Initializes a new instance of the <see cref="RequestCompletion"/> class.
    /// </summary>
    /// <param name="owner">
    /// The table whose pool the completion returns to.
    /// </param>
    internal RequestCompletion(OutgoingRecords owner) => _owner = owner;

    /// <summary>
    /// Gets the task the caller awaits. It must be awaited at most once, and only before the completion is
    /// returned to the pool.
    /// </summary>
    public ValueTask<MqttRequestResult> Task => new(source: this, token: _core.Version);

    /// <summary>
    /// Completes the request with its outcome. It must be called exactly once per rent.
    /// </summary>
    /// <param name="result">
    /// How the request ended.
    /// </param>
    public void Complete(MqttRequestResult result) => _core.SetResult(result: result);

    /// <inheritdoc />
    MqttRequestResult IValueTaskSource<MqttRequestResult>.GetResult(short token)
    {
        MqttRequestResult result = _core.GetResult(token: token);
        _core.Reset();
        _owner.ReturnCompletion(completion: this);
        return result;
    }

    /// <inheritdoc />
    ValueTaskSourceStatus IValueTaskSource<MqttRequestResult>.GetStatus(short token) => _core.GetStatus(token: token);

    /// <inheritdoc />
    void IValueTaskSource<MqttRequestResult>.OnCompleted(
        Action<object?> continuation,
        object? state,
        short token,
        ValueTaskSourceOnCompletedFlags flags) =>
        _core.OnCompleted(continuation: continuation, state: state, token: token, flags: flags);
}
