using System.Diagnostics;
using MqttWire.Codec;
using MqttWire.Server.Internal;

namespace MqttWire.Server;

/// <content>
/// The send path's fault handling: what a holder of the send path, or a caller that does not hold it, does when
/// something throws inside MqttWire's sending, and the giving back of what the Closing step took away.
/// </content>
public sealed partial class MqttWireConnection
{
    /// <summary>
    /// Gives back what the hold of <see cref="TakeNextStep"/> took away, once the lock is released, and tells whoever
    /// waits for the send path to be idle. Should the giving back throw after the hold gave the path up, the fault
    /// ends the connection without touching the path again, which may be another's by then; when the caller still
    /// holds the path, the exception goes on to the caller's own catch, which gives it up.
    /// </summary>
    /// <param name="step">
    /// The step the hold decided.
    /// </param>
    /// <param name="emptied">
    /// The queue of waiting deliveries to go back to the server's pool, or <see langword="null"/>.
    /// </param>
    /// <param name="records">
    /// The exchange records to go back to the server's pool, or <see langword="null"/>.
    /// </param>
    /// <param name="dropped">
    /// The control buffer whose array, never handed to the link, goes back to the buffer pool.
    /// </param>
    /// <param name="idle">
    /// What the end of the run waits on, or <see langword="null"/>; completed whatever happens.
    /// </param>
    private void ReleaseDropped(SendStep step, WaitingDeliveries? emptied, ExchangeRecords? records,
        ControlBuffer dropped, TaskCompletionSource? idle)
    {
        try
        {
            // The pool's policy clears the queue, so no payload or topic reference outlives the connection's use of
            // it.
            if (emptied is not null)
            {
                _server.Pools.Waiting.Return(emptied);
            }

            // The pool's policy clears the records, so the next connection starts with none.
            if (records is not null)
            {
                _server.Pools.Records.Return(records);
            }

            dropped.Release(_server.Pool);
        }
        catch (Exception exception) when (step == SendStep.Idle)
        {
            EndFaulted(exception);
        }
        finally
        {
            idle?.TrySetResult();
        }
    }

    /// <summary>
    /// The last resort of a holder of the send path that something threw at (an encoding whose payload memory throws,
    /// the pool, anything inside MqttWire): the connection is beyond saving. In this order: the close begins with
    /// <see cref="CloseCause.SendPathFaulted"/> and the link is aborted (<see cref="CloseAfterFault"/>); the one
    /// buffer the holder rented and has not handed to the link goes back; the path is given up through the Closing
    /// step, which drops what waits, the exchange records and the control buffer, clears the counts and tells whoever
    /// waits for the path to be idle (the last write is taken as failed, so no WebSocket close is attempted); and the
    /// fault is logged, last (<see cref="LogFault"/>). Called only by a holder, from its catch; it costs nothing on the
    /// paths that do not throw.
    /// </summary>
    /// <remarks>
    /// It cannot throw, so it runs at most once per holder: no catch site sees an exception from it and calls it
    /// again, a thread-pool work item never fails, and a host's <see cref="TrySend"/> gets no exception. When it
    /// returns, the connection is Closing or later, the link is aborted, and the send path has been given up through
    /// the Closing step. Each call it makes that could throw is guarded: the close's own throw can come only after its
    /// move to Closing (the clock), the abort and the pool's return are swallowed, the Closing step neither reads the
    /// clock nor lets a pool's exception out, and the log is swallowed.
    /// </remarks>
    /// <param name="exception">
    /// What was thrown.
    /// </param>
    /// <param name="unsent">
    /// The rented buffer the holder still owns and has not handed to the link, or <see langword="null"/>. A buffer
    /// whose write was started is never passed: whoever awaits that write returns it.
    /// </param>
    private void FaultSendPath(Exception exception, byte[]? unsent)
    {
        Exception? secondary = CloseAfterFault();
        if (unsent is not null)
        {
            try
            {
                _server.Pool.Return(unsent);
            }
            catch (Exception refused)
            {
                // A pool that refuses a buffer it lent: the buffer is left to the garbage collector, never returned
                // twice, and the refusal is logged with the fault.
                secondary ??= refused;
            }
        }

        // The connection is Closing, so this is the Closing step: it never hands the path on and cannot throw.
        Debug.Assert(_state >= ConnectionState.Closing, "The close began before the path is given up.");
        SendStep step = TakeNextStep(failed: true, handOver: false, out _, out _, out _, out _, out _);
        Debug.Assert(step == SendStep.Idle, "A Closing step after a failure always gives the send path up.");
        LogFault(exception, secondary);
    }

    /// <summary>
    /// Ends a connection after a fault inside MqttWire's sending, for a caller that does not hold the send path, the
    /// sweep whose check of the connection threw included: begins the close and aborts the link
    /// (<see cref="CloseAfterFault"/>), then logs the fault (<see cref="LogFault"/>). It never touches the send
    /// path's own state: when someone holds the path, the close finds it taken and aborts; when nobody does, the
    /// close takes it and writes its WebSocket close as any close does. It cannot throw.
    /// </summary>
    /// <param name="exception">
    /// What was thrown.
    /// </param>
    internal void EndFaulted(Exception exception) => LogFault(exception, CloseAfterFault());

    /// <summary>
    /// Begins the close with <see cref="CloseCause.SendPathFaulted"/> (reported to the host as
    /// <see cref="MqttCloseReason.ConnectionLost"/>), unless an earlier close recorded its own cause, and aborts the
    /// link (<see cref="TryAbortLink"/>: unless the run has ended, or an abort is in flight already), which a close
    /// that had begun already has not necessarily done. The close writes no log line of its own:
    /// the fault's line, which carries the exception, is written by <see cref="LogFault"/>. It cannot throw, and when
    /// it returns the connection is Closing or later: the close's own throw can come only after its move to Closing
    /// (the clock read for its stamp).
    /// </summary>
    /// <returns>
    /// The first exception the close or the abort threw, to be logged with the fault; <see langword="null"/> when
    /// neither threw.
    /// </returns>
    private Exception? CloseAfterFault()
    {
        Exception? secondary = null;
        try
        {
            BeginClose(CloseCause.SendPathFaulted, MqttReasonCode.Success, reasonString: null);
        }
        catch (Exception thrown)
        {
            // The connection is Closing all the same; the abort below ends the link.
            secondary = thrown;
        }

        // The abort is issued whether or not the close threw: a close that threw at its stamp has no close deadline
        // for the sweep to act on. A link that cannot even be aborted is left to that deadline, where there is one.
        Exception? abortFailure = TryAbortLink();
        return secondary ?? abortFailure;
    }

    /// <summary>
    /// Logs a fault inside MqttWire's sending as an error: event 20, the client id and the exception, never a payload
    /// or a password, through <see cref="LogEnd"/>, which swallows a logging provider that throws: the connection has
    /// already been ended and its send path given up, and throwing on would undo exactly what the fault path
    /// guarantees. It cannot throw.
    /// </summary>
    /// <param name="exception">
    /// What was thrown.
    /// </param>
    /// <param name="secondary">
    /// What the handling of the fault threw in its turn, or <see langword="null"/>; when there is one, both are
    /// logged together in an <see cref="AggregateException"/>.
    /// </param>
    private void LogFault(Exception exception, Exception? secondary)
    {
        Exception logged = secondary is null ? exception : new AggregateException(exception, secondary);
        LogEnd(CloseCause.SendPathFaulted, MqttReasonCode.Success, payloadLength: 0, method: null, logged);
    }
}
