using System.Net.WebSockets;
using MqttWire.Client;
using MqttWire.Codec;
using MqttWire.Link;

namespace MqttWire;

/// <content>
/// The end of a connection: recording the first reason, closing or aborting the link, and finishing once.
/// </content>
public sealed partial class MqttWireClient
{
    /// <summary>
    /// Aborts the link when a close has taken too long. The state is always the client that created the timer.
    /// </summary>
    private static readonly TimerCallback s_closeTimedOut = static state => ((MqttWireClient)state!).AbortLink();

    /// <summary>
    /// Completes a drained record's request with <see cref="MqttRequestStatus.ConnectionLost"/>; a static lambda,
    /// so draining allocates no delegate.
    /// </summary>
    private static readonly Action<OutgoingRecord> s_completeConnectionLost =
        static record =>
            record.Completion.Complete(result: new MqttRequestResult(Status: MqttRequestStatus.ConnectionLost,
                ReasonCode: MqttReasonCode.UnspecifiedError));

    /// <summary>
    /// How the one who records the end treats the link.
    /// </summary>
    private enum EndMode
    {
        /// <summary>
        /// Abort the link at once: for timeouts and failed reads or writes.
        /// </summary>
        Abort,

        /// <summary>
        /// Close the link, after a DISCONNECT where one is due, and abort it if the close takes too long.
        /// </summary>
        Close,
    }

    /// <summary>
    /// Completes every request still in flight with <see cref="MqttRequestStatus.ConnectionLost"/> and frees its
    /// record. Called once, by the read loop after it has stopped and before <c>OnClosed</c>.
    /// </summary>
    private void CompleteInFlightRequests()
    {
        lock (_gate)
        {
            // Records are added only while connected, so every one was in place before the end was recorded, and
            // none is added after. Completing under the lock runs no caller code: completions continue
            // asynchronously (decision 6). Each completion is read before its record is recycled.
            _records?.DrainAll(each: s_completeConnectionLost);
        }
    }

    /// <summary>
    /// Aborts the link, if one is open.
    /// </summary>
    private void AbortLink() => _link?.Abort();

    /// <summary>
    /// Records the end of the connection unless one is already recorded, and aborts the link if the mode says so.
    /// Whether it recorded the end or found another recorded, it then ends the read loop's wait at the cap of
    /// waiting answers, if the read loop waits (<see cref="ReleaseAnswerRoom"/>), so that no way out, a fault of the
    /// send path included, leaves it waiting.
    /// </summary>
    /// <param name="info">
    /// Why the connection ends.
    /// </param>
    /// <param name="mode">
    /// How the link is treated. For <see cref="EndMode.Close"/> the caller then runs <see cref="CloseLinkAsync"/>.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when this call recorded the end; <see langword="false"/> when another reason was
    /// recorded first, or the connect never started, and nothing changed but the end of a wait at the cap.
    /// </returns>
    private bool TryBeginEnd(in MqttClientCloseInfo info, EndMode mode)
    {
        IMqttLink? link;
        bool recorded;
        lock (_gate)
        {
            recorded = TryRecordEndLocked(info: info, mode: mode);
            link = _link;
        }

        try
        {
            if (recorded && mode == EndMode.Abort)
            {
                link?.Abort();
            }
        }
        finally
        {
            // After the hold of the client's lock, so that the send path's next hold sees the end and creates no new
            // wait, and after the abort, so that the read loop finds the link aborted when it carries on.
            ReleaseAnswerRoom();
        }

        return recorded;
    }

    /// <summary>
    /// Records the end of the connection unless one is already recorded; for a close, starts the close deadline.
    /// The caller holds <see cref="_gate"/>.
    /// </summary>
    /// <param name="info">
    /// Why the connection ends.
    /// </param>
    /// <param name="mode">
    /// How the link is treated.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when this call recorded the end; <see langword="false"/> when the connection is not
    /// connecting or connected, and nothing changed.
    /// </returns>
    private bool TryRecordEndLocked(in MqttClientCloseInfo info, EndMode mode)
    {
        if (_phase is not (Phase.Connecting or Phase.Connected))
        {
            return false;
        }

        _wasConnected = _phase == Phase.Connected;
        _phase = Phase.Closing;
        _closeInfo = info;
        if (mode == EndMode.Close && _link is not null)
        {
            _closeTimer = _time.CreateTimer(callback: s_closeTimedOut, state: this, dueTime: ClientLimits.CloseTimeout,
                period: Timeout.InfiniteTimeSpan);
        }

        return true;
    }

    /// <summary>
    /// Closes the link, after a DISCONNECT when one is given. Run by whoever recorded the end with
    /// <see cref="EndMode.Close"/>; the close deadline aborts the link if this or the server's answer takes too long.
    /// </summary>
    /// <param name="link">
    /// The link.
    /// </param>
    /// <param name="disconnect">
    /// The DISCONNECT's reason code, or <see langword="null"/> to send none.
    /// </param>
    /// <returns>
    /// A task that completes when the close has been handed to the link, or has failed. It does not throw.
    /// </returns>
    private async ValueTask CloseLinkAsync(IMqttLink link, MqttReasonCode? disconnect)
    {
        // A DISCONNECT over the server's Maximum Packet Size may not be sent (MQTT-3.2.2-15); the close alone ends it.
        if (disconnect is { } reasonCode
            && FitsServerMaximumPacketSize(DisconnectCodec.GetSize(reasonCode, null)))
        {
            byte[] packet = RentDisconnect(reasonCode: reasonCode, length: out int length);

            // Written behind every write handed over before it, so the close follows them all.
            if (!await SendDisconnectAsync(packet: packet, length: length)
                    .ConfigureAwait(continueOnCapturedContext: false))
            {
                // Not written: the write failed and aborted the link, or the send path takes nothing any more; there
                // is nothing left to close.
                return;
            }
        }

        try
        {
            await link.CloseAsync(status: WebSocketCloseStatus.NormalClosure, cancellationToken: CancellationToken.None)
                .ConfigureAwait(continueOnCapturedContext: false);
        }
        catch (Exception)
        {
            // The close failed: end the link now rather than at the close deadline. The read loop then sees it end.
            link.Abort();
        }
    }

    /// <summary>
    /// Finishes the connection once the read loop has stopped: stops the close deadline and the keep-alive timer,
    /// completes what is in flight, returns the read buffer, drops the answers still waiting and returns their
    /// buffer, releases the link, and then reports the end once: to <c>OnClosed</c> for a connection that connected,
    /// to the connect otherwise.
    /// </summary>
    /// <param name="link">
    /// The link, which has ended.
    /// </param>
    private void FinishConnection(IMqttLink link)
    {
        ITimer? closeTimer;
        ITimer? keepAliveTimer;
        bool wasConnected;
        MqttClientCloseInfo info;
        lock (_gate)
        {
            closeTimer = _closeTimer;
            _closeTimer = null;
            keepAliveTimer = TakeKeepAliveTimerLocked();
            wasConnected = _wasConnected;
            info = _closeInfo;
        }

        // A tick already running sees the connection is no longer connected and does nothing.
        closeTimer?.Dispose();
        keepAliveTimer?.Dispose();
        CompleteInFlightRequests();
        _receive.Clear();

        // Answers still waiting are not sent once the end is recorded; their buffer goes back here.
        _answers.Release(_pool);

        // The link has ended; aborting it now only releases the socket under it.
        link.Abort();

        if (wasConnected)
        {
            InvokeOnClosed(info: info);
        }
        else
        {
            CompleteFailedConnect();
        }

        SetClosed();

        // Set with the link, before the read loop started.
        _ended!.TrySetResult();
    }

    /// <summary>
    /// Calls <c>OnClosed</c>, swallowing what it throws (decision 7).
    /// </summary>
    /// <param name="info">
    /// Why the connection ended.
    /// </param>
    private void InvokeOnClosed(in MqttClientCloseInfo info)
    {
        try
        {
            _onClosed?.Invoke(obj: info);
        }
#pragma warning disable RCS1075 // An empty catch of Exception is the point here.
        catch (Exception)
        {
            // Decision 7: what OnClosed throws is swallowed; the connection has ended and there is no one to tell.
        }
#pragma warning restore RCS1075
    }
}
