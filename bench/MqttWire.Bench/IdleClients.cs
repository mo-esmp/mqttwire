using System.Globalization;
using System.Net.WebSockets;
using System.Text;

namespace MqttWire.Bench;

/// <summary>
/// What the endpoint of an <see cref="IdleClients"/> does with its connections.
/// </summary>
internal enum EndpointKind
{
    /// <summary>
    /// The floor: nothing is sent to it.
    /// </summary>
    Floor,

    /// <summary>
    /// The echo floor: it sends every message back.
    /// </summary>
    Echo,

    /// <summary>
    /// MqttWire's endpoint: every connection speaks MQTT 5.
    /// </summary>
    Mqtt,
}

/// <summary>
/// The connections the <c>run</c> role holds against a server: plain WebSockets with the sub-protocol
/// <c>mqtt</c> and no WebSocket keep-alive, which speak just enough MQTT 5, as bytes put together by hand, to
/// connect, publish, acknowledge a burst, ping and disconnect. They live in the <c>run</c> process, so what they
/// cost is never measured.
/// </summary>
/// <param name="endpoint">
/// The WebSocket address of the endpoint the connections are opened on.
/// </param>
/// <param name="kind">
/// What the endpoint does: on MqttWire's every connection sends a CONNECT; the floor's connections send
/// nothing; the echo floor's send the same bytes as MqttWire's during the activity and get them back.
/// </param>
internal sealed class IdleClients(Uri endpoint, EndpointKind kind) : IAsyncDisposable
{
    /// <summary>
    /// How many connections are opened, or worked with, at the same time.
    /// </summary>
    private const int BatchSize = 200;

    /// <summary>
    /// How long any one step of one connection may take before the run fails instead of hanging.
    /// </summary>
    private static readonly TimeSpan s_stepLimit = TimeSpan.FromSeconds(60);

    /// <summary>
    /// A PINGREQ.
    /// </summary>
    private static readonly byte[] s_pingReq = [0xC0, 0x00];

    /// <summary>
    /// A DISCONNECT with the reason code "normal disconnection".
    /// </summary>
    private static readonly byte[] s_disconnect = [0xE0, 0x00];

    /// <summary>
    /// A PUBLISH at QoS 0 to the topic <c>b</c> with 16 bytes of payload and no properties.
    /// </summary>
    private static readonly byte[] s_publishQoS0 = [0x30, 0x14, 0x00, 0x01, (byte)'b', 0x00, .. new byte[16]];

    /// <summary>
    /// A PUBLISH at QoS 2 with the packet identifier 1 to the topic <c>b</c>, with 16 bytes of payload and no
    /// properties.
    /// </summary>
    private static readonly byte[] s_publishQoS2 = [0x34, 0x16, 0x00, 0x01, (byte)'b', 0x00, 0x01, 0x00, .. new byte[16]];

    /// <summary>
    /// The PUBREL for the packet identifier 1.
    /// </summary>
    private static readonly byte[] s_pubRel = [0x62, 0x02, 0x00, 0x01];

    /// <summary>
    /// A message of the size of one PUBLISH of the burst, which the echo floor sends back: 269 bytes.
    /// </summary>
    private static readonly byte[] s_echoBurstMessage = new byte[269];

    /// <summary>
    /// The open connections.
    /// </summary>
    private readonly List<Connection> _connections = [];

    /// <summary>
    /// Gets the number of open connections.
    /// </summary>
    public int Count => _connections.Count;

    /// <summary>
    /// Gets the client id of the connection with an index, which is also how the server finds it for a burst.
    /// </summary>
    /// <param name="index">
    /// The connection's index, from 0.
    /// </param>
    /// <returns>
    /// <c>b</c> followed by the index.
    /// </returns>
    public static string ClientIdOf(int index) => string.Create(CultureInfo.InvariantCulture, $"b{index}");

    /// <summary>
    /// Opens connections, <see cref="BatchSize"/> at a time. On MqttWire's endpoint each sends a CONNECT with the
    /// client id <see cref="ClientIdOf"/> and reads an accepting CONNACK.
    /// </summary>
    /// <param name="count">
    /// How many to open, on top of none: the list must be empty.
    /// </param>
    /// <returns>
    /// A task that completes when all are open.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// Connections are open already, or the server did not accept a CONNECT.
    /// </exception>
    public async Task OpenAsync(int count)
    {
        if (_connections.Count != 0)
        {
            throw new InvalidOperationException("The connections are open already.");
        }

        Connection[] opened = new Connection[count];
        await Parallel.ForAsync(0, count, new ParallelOptions { MaxDegreeOfParallelism = BatchSize }, async (index, _) =>
        {
            using CancellationTokenSource limit = new(s_stepLimit);
            ClientWebSocket socket = new();
            socket.Options.AddSubProtocol("mqtt");
            socket.Options.KeepAliveInterval = TimeSpan.Zero;

            // The server's certificate is self-signed, made in memory for one run on the loopback interface.
            socket.Options.RemoteCertificateValidationCallback = static (_, _, _, _) => true;
            await socket.ConnectAsync(endpoint, limit.Token);
            Connection connection = new(socket);
            opened[index] = connection;
            if (kind == EndpointKind.Mqtt)
            {
                await connection.SendAsync(BuildConnect(ClientIdOf(index)), limit.Token);
                ReadOnlyMemory<byte> connAck = await connection.ReadPacketAsync(limit.Token);

                // A CONNACK: the flags, then the reason code, which must be success.
                if (connAck.Span[0] != 0x20 || connAck.Span[3] != 0x00)
                {
                    throw new InvalidOperationException($"Connection {index} was not accepted.");
                }
            }
        });
        _connections.AddRange(opened);
    }

    /// <summary>
    /// Pings every connection and reads the answer. The server gives a client that asks for no keep-alive 60
    /// seconds, and drops one that is silent for 90, so the run pings before every measurement. A connection is
    /// idle again as soon as the answer is written.
    /// </summary>
    /// <returns>
    /// A task that completes when every connection has its PINGRESP; at once on the floor and the echo floor,
    /// which have no keep-alive.
    /// </returns>
    public Task PingAsync() => kind != EndpointKind.Mqtt
        ? Task.CompletedTask
        : ForEachAsync(static async (connection, cancellationToken) =>
        {
            await connection.SendAsync(s_pingReq, cancellationToken);
            await connection.ExpectAsync(0xD0, cancellationToken);
        });

    /// <summary>
    /// The activity of design 6.14 item 3: every connection publishes one message at QoS 0 and one at QoS 2,
    /// with the full exchange; then the server sends every connection a burst of ten messages at QoS 1, which
    /// each connection acknowledges.
    /// </summary>
    /// <param name="burst">
    /// Asks the server to send the burst to this many connections, and returns how many messages it took.
    /// </param>
    /// <returns>
    /// A task that completes when every publish is acknowledged and every message of the burst has arrived and
    /// is acknowledged.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// The server did not take the whole burst, or a connection got something else than it expected.
    /// </exception>
    public async Task ActivityAsync(Func<int, Task<int>> burst)
    {
        await ForEachAsync(static async (connection, cancellationToken) =>
        {
            await connection.SendAsync(s_publishQoS0, cancellationToken);
            await connection.SendAsync(s_publishQoS2, cancellationToken);
            await connection.ExpectAsync(0x50, cancellationToken);
            await connection.SendAsync(s_pubRel, cancellationToken);
            await connection.ExpectAsync(0x70, cancellationToken);
        });

        // Every connection waits for its burst before the server is asked for it, so no delivery waits for a
        // connection's turn in a batch while its acknowledgement deadline runs.
        using CancellationTokenSource limit = new(s_stepLimit + s_stepLimit);
        Task[] receiving = new Task[_connections.Count];
        for (int i = 0; i < receiving.Length; i++)
        {
            receiving[i] = _connections[i].AcknowledgeBurstAsync(ServerRole.BurstMessages, limit.Token);
        }

        int taken = await burst(_connections.Count);
        if (taken != _connections.Count * ServerRole.BurstMessages)
        {
            throw new InvalidOperationException(
                $"The server took {taken} messages of a burst of {_connections.Count * ServerRole.BurstMessages}.");
        }

        await Task.WhenAll(receiving);
    }

    /// <summary>
    /// The same traffic as <see cref="ActivityAsync"/>, on the echo floor: every connection sends the bytes of the
    /// two publishes and of the PUBREL and gets each back; then every connection, all at the same time, sends ten
    /// messages of the size of the burst's and gets each back. So the echo floor writes what MqttWire writes in
    /// the burst, and reads at least as much.
    /// </summary>
    /// <returns>
    /// A task that completes when every message has come back.
    /// </returns>
    public async Task EchoActivityAsync()
    {
        await ForEachAsync(static async (connection, cancellationToken) =>
        {
            await connection.EchoAsync(s_publishQoS0, cancellationToken);
            await connection.EchoAsync(s_publishQoS2, cancellationToken);
            await connection.EchoAsync(s_pubRel, cancellationToken);
        });

        using CancellationTokenSource limit = new(s_stepLimit + s_stepLimit);
        Task[] echoing = new Task[_connections.Count];
        for (int i = 0; i < echoing.Length; i++)
        {
            echoing[i] = _connections[i].EchoBurstAsync(ServerRole.BurstMessages, limit.Token);
        }

        await Task.WhenAll(echoing);
    }

    /// <summary>
    /// Closes every connection as a client does: it sends a DISCONNECT, waits for the server to close the
    /// WebSocket, and answers the close. The server closes first, so the client's ports are free again at once.
    /// </summary>
    /// <returns>
    /// A task that completes when every connection is closed and the list is empty.
    /// </returns>
    public async Task CloseAsync()
    {
        await ForEachAsync(static (connection, cancellationToken) => connection.CloseAsync(cancellationToken));
        _connections.Clear();
    }

    /// <summary>
    /// Drops every connection that is still open, without a closing handshake.
    /// </summary>
    /// <returns>
    /// A completed task.
    /// </returns>
    public ValueTask DisposeAsync()
    {
        foreach (Connection connection in _connections)
        {
            connection.Abort();
        }

        _connections.Clear();
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Builds a CONNECT for MQTT 5 with a clean start, no keep-alive, no properties and a client id.
    /// </summary>
    /// <param name="clientId">
    /// The client id, of at most 100 ASCII characters, so that the packet's length fits one byte.
    /// </param>
    /// <returns>
    /// The packet's bytes.
    /// </returns>
    private static byte[] BuildConnect(string clientId)
    {
        byte[] id = Encoding.ASCII.GetBytes(clientId);
        return
        [
            0x10, (byte)(13 + id.Length),
            0x00, 0x04, (byte)'M', (byte)'Q', (byte)'T', (byte)'T',
            0x05, 0x02, 0x00, 0x00, 0x00,
            0x00, (byte)id.Length, .. id,
        ];
    }

    /// <summary>
    /// Runs one step on every connection, <see cref="BatchSize"/> at a time, each within
    /// <see cref="s_stepLimit"/>.
    /// </summary>
    /// <param name="step">
    /// The step.
    /// </param>
    /// <returns>
    /// A task that completes when the step has run on every connection; it fails with the first failure.
    /// </returns>
    private Task ForEachAsync(Func<Connection, CancellationToken, Task> step) =>
        Parallel.ForEachAsync(_connections, new ParallelOptions { MaxDegreeOfParallelism = BatchSize }, async (connection, _) =>
        {
            using CancellationTokenSource limit = new(s_stepLimit);
            await step(connection, limit.Token);
        });

    /// <summary>
    /// One connection: its WebSocket and the bytes read from it that are not yet a whole packet.
    /// </summary>
    /// <param name="socket">
    /// The connected WebSocket.
    /// </param>
    private sealed class Connection(ClientWebSocket socket)
    {
        /// <summary>
        /// What was read and not yet handed out. Every packet the benchmark receives is far smaller.
        /// </summary>
        private readonly byte[] _buffer = new byte[2_048];

        /// <summary>
        /// How many bytes of <see cref="_buffer"/> are filled.
        /// </summary>
        private int _filled;

        /// <summary>
        /// The length of the packet handed out last, which the next read drops from the front of the buffer.
        /// </summary>
        private int _handedOut;

        /// <summary>
        /// Sends one packet as one binary message.
        /// </summary>
        /// <param name="packet">
        /// The packet's bytes.
        /// </param>
        /// <param name="cancellationToken">
        /// Ends the wait.
        /// </param>
        /// <returns>
        /// A task that completes when the message is sent.
        /// </returns>
        public ValueTask SendAsync(ReadOnlyMemory<byte> packet, CancellationToken cancellationToken) =>
            socket.SendAsync(packet, WebSocketMessageType.Binary, endOfMessage: true, cancellationToken);

        /// <summary>
        /// Reads the next whole MQTT packet. Packets are framed by their own length, not by WebSocket messages.
        /// </summary>
        /// <param name="cancellationToken">
        /// Ends the wait.
        /// </param>
        /// <returns>
        /// The packet's bytes, fixed header included, valid until the next read.
        /// </returns>
        /// <exception cref="InvalidOperationException">
        /// The server closed the connection, or sent a packet larger than the buffer.
        /// </exception>
        public async ValueTask<ReadOnlyMemory<byte>> ReadPacketAsync(CancellationToken cancellationToken)
        {
            if (_handedOut > 0)
            {
                _filled -= _handedOut;
                Buffer.BlockCopy(_buffer, _handedOut, _buffer, 0, _filled);
                _handedOut = 0;
            }

            while (true)
            {
                if (TryFrame(out int length))
                {
                    _handedOut = length;
                    return _buffer.AsMemory(0, length);
                }

                if (_filled == _buffer.Length)
                {
                    throw new InvalidOperationException("The server sent a packet larger than the benchmark expects.");
                }

                ValueWebSocketReceiveResult received = await socket.ReceiveAsync(_buffer.AsMemory(_filled), cancellationToken);
                if (received.MessageType == WebSocketMessageType.Close)
                {
                    throw new InvalidOperationException(
                        $"The server closed a connection: {socket.CloseStatus} {socket.CloseStatusDescription}.");
                }

                _filled += received.Count;
            }
        }

        /// <summary>
        /// Reads the next packet and checks its type.
        /// </summary>
        /// <param name="firstByte">
        /// The first byte the packet must have, such as <c>0x50</c> for a PUBREC.
        /// </param>
        /// <param name="cancellationToken">
        /// Ends the wait.
        /// </param>
        /// <returns>
        /// A task that completes when the packet has arrived.
        /// </returns>
        /// <exception cref="InvalidOperationException">
        /// Another packet arrived, or an acknowledgement that carries a failure.
        /// </exception>
        public async Task ExpectAsync(byte firstByte, CancellationToken cancellationToken)
        {
            ReadOnlyMemory<byte> packet = await ReadPacketAsync(cancellationToken);

            // An acknowledgement longer than its identifier carries a reason code, which must not be a failure.
            if (packet.Span[0] != firstByte || (packet.Length > 4 && packet.Span[4] >= 0x80))
            {
                throw new InvalidOperationException(
                    $"Expected a packet starting with 0x{firstByte:X2}, got {Convert.ToHexString(packet.Span)}.");
            }
        }

        /// <summary>
        /// Reads the messages of a burst and acknowledges each with a PUBACK.
        /// </summary>
        /// <param name="messages">
        /// How many messages at QoS 1 to wait for.
        /// </param>
        /// <param name="cancellationToken">
        /// Ends the wait.
        /// </param>
        /// <returns>
        /// A task that completes when the last PUBACK is sent.
        /// </returns>
        /// <exception cref="InvalidOperationException">
        /// Something else than a PUBLISH at QoS 1 arrived.
        /// </exception>
        public async Task AcknowledgeBurstAsync(int messages, CancellationToken cancellationToken)
        {
            byte[] pubAck = [0x40, 0x02, 0x00, 0x00];
            for (int i = 0; i < messages; i++)
            {
                ReadOnlyMemory<byte> packet = await ReadPacketAsync(cancellationToken);
                if ((packet.Span[0] & 0xF6) != 0x32)
                {
                    throw new InvalidOperationException($"Expected a PUBLISH at QoS 1, got {Convert.ToHexString(packet.Span[..Math.Min(packet.Length, 8)])}.");
                }

                // The fixed header (the burst's packets are longer than 127 bytes, so three bytes), the topic
                // with its length, then the packet identifier.
                ReadOnlySpan<byte> span = packet.Span;
                int header = (span[1] & 0x80) == 0 ? 2 : 3;
                int identifier = header + 2 + ((span[header] << 8) | span[header + 1]);
                pubAck[2] = span[identifier];
                pubAck[3] = span[identifier + 1];
                await SendAsync(pubAck, cancellationToken);
            }
        }

        /// <summary>
        /// Sends one message to the echo floor and reads it back.
        /// </summary>
        /// <param name="message">
        /// The message, smaller than the buffer.
        /// </param>
        /// <param name="cancellationToken">
        /// Ends the wait.
        /// </param>
        /// <returns>
        /// A task that completes when the whole message has come back.
        /// </returns>
        /// <exception cref="InvalidOperationException">
        /// The server closed the connection, or sent back another number of bytes.
        /// </exception>
        public async Task EchoAsync(ReadOnlyMemory<byte> message, CancellationToken cancellationToken)
        {
            await SendAsync(message, cancellationToken);
            int read = 0;
            while (true)
            {
                ValueWebSocketReceiveResult received = await socket.ReceiveAsync(_buffer.AsMemory(read), cancellationToken);
                if (received.MessageType == WebSocketMessageType.Close)
                {
                    throw new InvalidOperationException("The echo floor closed a connection.");
                }

                read += received.Count;
                if (received.EndOfMessage)
                {
                    break;
                }
            }

            if (read != message.Length)
            {
                throw new InvalidOperationException($"The echo floor sent {read} bytes back for {message.Length}.");
            }
        }

        /// <summary>
        /// Sends the echo floor as many messages as a burst has, each of the size of one of its publishes, and
        /// reads each back.
        /// </summary>
        /// <param name="messages">
        /// How many messages.
        /// </param>
        /// <param name="cancellationToken">
        /// Ends the wait.
        /// </param>
        /// <returns>
        /// A task that completes when the last message has come back.
        /// </returns>
        public async Task EchoBurstAsync(int messages, CancellationToken cancellationToken)
        {
            for (int i = 0; i < messages; i++)
            {
                await EchoAsync(s_echoBurstMessage, cancellationToken);
            }
        }

        /// <summary>
        /// Sends a DISCONNECT, waits for the server's close and answers it. A connection that is gone already is
        /// left as it is.
        /// </summary>
        /// <param name="cancellationToken">
        /// Ends the wait.
        /// </param>
        /// <returns>
        /// A task that completes when the WebSocket is closed.
        /// </returns>
        public async Task CloseAsync(CancellationToken cancellationToken)
        {
            try
            {
                await SendAsync(s_disconnect, cancellationToken);
                while (true)
                {
                    ValueWebSocketReceiveResult received = await socket.ReceiveAsync(_buffer.AsMemory(), cancellationToken);
                    if (received.MessageType == WebSocketMessageType.Close)
                    {
                        break;
                    }
                }

                await socket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, statusDescription: null, cancellationToken);
            }
            catch (WebSocketException)
            {
                // The server has gone already.
            }
            finally
            {
                socket.Dispose();
            }
        }

        /// <summary>
        /// Drops the connection without a closing handshake.
        /// </summary>
        public void Abort()
        {
            socket.Abort();
            socket.Dispose();
        }

        /// <summary>
        /// Tells whether the buffer starts with a whole packet.
        /// </summary>
        /// <param name="length">
        /// The packet's length, fixed header included, when it is whole.
        /// </param>
        /// <returns>
        /// <see langword="true"/> when a whole packet is there; <see langword="false"/> when more bytes are
        /// needed.
        /// </returns>
        private bool TryFrame(out int length)
        {
            length = 0;
            int remaining = 0;
            for (int i = 1; i <= 4 && i < _filled; i++)
            {
                remaining |= (_buffer[i] & 0x7F) << (7 * (i - 1));
                if ((_buffer[i] & 0x80) == 0)
                {
                    length = 1 + i + remaining;
                    return _filled >= length;
                }
            }

            return false;
        }
    }
}
