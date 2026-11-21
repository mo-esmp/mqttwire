using MqttWire.Client;
using MqttWire.Codec;

namespace MqttWire;

/// <content>
/// Keep-alive: one timer per connection that ticks every half interval, sends PINGREQ when nothing was sent, and
/// ends the connection when a PINGREQ or a request goes unanswered for too long (6.9, decision 8). A server that
/// turns keep-alive off (Server Keep Alive 0) stops the PINGREQs only: the acknowledgement deadline stays, at
/// twice the client's own interval, so no request waits for ever.
/// </content>
public sealed partial class MqttWireClient
{
    /// <summary>
    /// Runs one keep-alive tick. The state is always the client that created the timer.
    /// </summary>
    private static readonly TimerCallback s_keepAliveTick = static state => ((MqttWireClient)state!).OnKeepAliveTick();

    /// <summary>
    /// The keep-alive timer, from the accepting CONNACK until the connection is finished, also when the server
    /// turned keep-alive off; <see langword="null"/> before and after. Under <see cref="_gate"/>.
    /// </summary>
    private ITimer? _keepAliveTimer;

    /// <summary>
    /// The interval the timer works with: the CONNACK's Server Keep Alive, or the client's own when the CONNACK has
    /// none or turns keep-alive off. Set before the timer is created and never changed, so a tick reads it without
    /// the lock.
    /// </summary>
    private TimeSpan _keepAliveInterval;

    /// <summary>
    /// Whether the ticks send PINGREQ and keep its deadline: <see langword="false"/> when the server sent a Server
    /// Keep Alive of 0 (MQTT 5, 3.1.2.10). Set with <see cref="_keepAliveInterval"/> and never changed.
    /// </summary>
    private bool _sendsPings;

    /// <summary>
    /// The <see cref="TimeProvider.GetTimestamp"/> at which the write of the PINGREQ not yet answered began, or 0
    /// when none waits. Set by the send path just before the PINGREQ is written; cleared by any PINGRESP.
    /// </summary>
    private long _pingRequestTimestamp;

    /// <summary>
    /// 1 while a PINGREQ is queued for the send lock or being written, 0 otherwise. A tick queues no second
    /// PINGREQ while one is queued, so a stalled write ahead of it does not pile pings up behind it.
    /// </summary>
    private int _pingQueued;

    /// <summary>
    /// Reads the keep-alive interval of the connection from an accepting CONNACK: its Server Keep Alive when it has
    /// one (MQTT 5, 3.2.2.3.14), else the client's own.
    /// </summary>
    /// <param name="properties">
    /// The CONNACK's validated properties.
    /// </param>
    /// <returns>
    /// The interval; <see cref="TimeSpan.Zero"/> when the server sent a Server Keep Alive of 0, which turns the
    /// PINGREQs off (MQTT 5, 3.1.2.10) but not the acknowledgement deadline (see
    /// <see cref="StartKeepAliveLocked"/>).
    /// </returns>
    private TimeSpan ReadKeepAliveInterval(in PropertyBlock properties)
        => properties.TryGetInteger(PropertyId.ServerKeepAlive, out uint seconds) ? TimeSpan.FromSeconds(seconds) : KeepAlive;

    /// <summary>
    /// Creates the keep-alive timer, ticking every half interval. Called once, by the read loop under
    /// <see cref="_gate"/>, as the connection becomes connected.
    /// </summary>
    /// <param name="interval">
    /// The keep-alive interval of the connection. <see cref="TimeSpan.Zero"/>, keep-alive turned off by the server,
    /// still creates the timer, for the acknowledgement deadline alone: it then ticks every half of the client's
    /// own interval and sends no PINGREQ.
    /// </param>
    private void StartKeepAliveLocked(TimeSpan interval)
    {
        _sendsPings = interval != TimeSpan.Zero;
        _keepAliveInterval = _sendsPings ? interval : KeepAlive;
        TimeSpan period = _keepAliveInterval / 2;
        _keepAliveTimer = _time.CreateTimer(s_keepAliveTick, this, period, period);
    }

    /// <summary>
    /// Takes the keep-alive timer, so that it is disposed once, by the caller. Call under <see cref="_gate"/>.
    /// </summary>
    /// <returns>
    /// The timer, or <see langword="null"/> when there is none or it was already taken.
    /// </returns>
    private ITimer? TakeKeepAliveTimerLocked()
    {
        ITimer? timer = _keepAliveTimer;
        _keepAliveTimer = null;
        return timer;
    }

    /// <summary>
    /// Notes a PINGRESP: the PINGREQ waiting, if any, is answered. Called by the read loop.
    /// </summary>
    private void OnPingResponse()
    {
        long now = _time.GetTimestamp();
        Volatile.Write(ref _lastPingResponseTimestamp, now);
        Volatile.Write(ref _pingRequestTimestamp, 0);
    }

    /// <summary>
    /// One keep-alive tick. Ends the connection, aborting the link without a DISCONNECT (decision 9), with
    /// <see cref="MqttClientCloseReason.KeepAliveTimedOut"/> when a PINGREQ has waited a full interval, or with
    /// <see cref="MqttClientCloseReason.AcknowledgementTimedOut"/> when the oldest request has waited twice the
    /// interval for its next acknowledgement; otherwise sends PINGREQ when nothing was sent for half the interval,
    /// unless a PINGREQ is already queued or awaits its PINGRESP. The PINGRESP deadline counts from when the
    /// PINGREQ's write begins, so a write stalled ahead of it in the send lock does not end the connection as
    /// <see cref="MqttClientCloseReason.KeepAliveTimedOut"/>.
    /// With keep-alive turned off by the server only the acknowledgement deadline is checked, against the client's
    /// own interval. Does nothing unless connected, so a tick that comes after the end, as a timer's late callback
    /// may, changes nothing.
    /// </summary>
    /// <remarks>
    /// Runs on the timer's thread: it never blocks and holds the lock only to read the records. The PINGREQ goes
    /// through the send lock in turn with every other packet, in a buffer rented from the pool; in steady state the
    /// tick allocates nothing when the send lock is uncontended and the write completes synchronously. A write that
    /// completes asynchronously (always in the browser) allocates its state machine.
    /// </remarks>
    private void OnKeepAliveTick()
    {
        long? oldestRecord;
        lock (_gate)
        {
            if (_phase != Phase.Connected)
            {
                return;
            }

            oldestRecord = _records?.OldestPhaseStartedAt;
        }

        long now = _time.GetTimestamp();
        TimeSpan interval = _keepAliveInterval;
        bool sendsPings = _sendsPings;
        long pingRequest = Volatile.Read(ref _pingRequestTimestamp);
        if (sendsPings && pingRequest != 0 && _time.GetElapsedTime(pingRequest, now) >= interval)
        {
            TryBeginEnd(new MqttClientCloseInfo(MqttClientCloseReason.KeepAliveTimedOut, MqttReasonCode.Success, null, null), EndMode.Abort);
            return;
        }

        if (oldestRecord is long oldest && _time.GetElapsedTime(oldest, now) >= interval * 2)
        {
            TryBeginEnd(new MqttClientCloseInfo(MqttClientCloseReason.AcknowledgementTimedOut, MqttReasonCode.Success, null, null), EndMode.Abort);
            return;
        }

        if (!sendsPings || pingRequest != 0 || _time.GetElapsedTime(LastSendTimestamp, now) < interval / 2)
        {
            return;
        }

        ReadOnlySpan<byte> pingReq = PingCodec.PingReq;
        if (!FitsServerMaximumPacketSize(pingReq.Length))
        {
            EndOverServerMaximumPacketSize();
            return;
        }

        if (Interlocked.CompareExchange(ref _pingQueued, 1, 0) != 0)
        {
            // One is already waiting for the send lock; the send path clears this once its write has ended.
            return;
        }

        byte[] packet = _pool.Rent(pingReq.Length);
        pingReq.CopyTo(packet);

        // Not awaited: the send lock writes it in turn and returns the buffer. A failed write ends the connection.
        _ = SendAsync(packet, pingReq.Length, SendKind.PingRequest);
    }
}
