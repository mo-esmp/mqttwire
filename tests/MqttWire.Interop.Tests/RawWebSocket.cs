using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;

namespace MqttWire.Interop.Tests;

/// <summary>
/// A WebSocket client written frame by frame over a plain TCP socket, for the tests that need what
/// <see cref="System.Net.WebSockets.ClientWebSocket"/> hides: the local end point, control frames such as an
/// unsolicited PONG, a receive buffer of a chosen size, and a client that does not read or does not answer a close.
/// Every frame it sends is final and masked; every frame it receives is read whole.
/// </summary>
internal sealed class RawWebSocket : IAsyncDisposable
{
    /// <summary>
    /// The opcode of a text frame.
    /// </summary>
    public const byte Text = 0x1;

    /// <summary>
    /// The opcode of a binary frame.
    /// </summary>
    public const byte Binary = 0x2;

    /// <summary>
    /// The opcode of a close frame.
    /// </summary>
    public const byte Close = 0x8;

    /// <summary>
    /// The opcode of a PONG frame.
    /// </summary>
    public const byte Pong = 0xA;

    /// <summary>
    /// The connected socket.
    /// </summary>
    private readonly Socket _socket;

    /// <summary>
    /// The stream over <see cref="_socket"/>.
    /// </summary>
    private readonly NetworkStream _stream;

    /// <summary>
    /// Creates the client around a socket whose handshake has completed.
    /// </summary>
    /// <param name="socket">
    /// The socket.
    /// </param>
    /// <param name="stream">
    /// The stream over it.
    /// </param>
    /// <param name="statusCode">
    /// The HTTP status of the handshake's answer.
    /// </param>
    /// <param name="subProtocol">
    /// The sub-protocol the server answered with, or <see langword="null"/>.
    /// </param>
    private RawWebSocket(Socket socket, NetworkStream stream, int statusCode, string? subProtocol)
    {
        _socket = socket;
        _stream = stream;
        StatusCode = statusCode;
        SubProtocol = subProtocol;
    }

    /// <summary>
    /// Gets the HTTP status of the handshake's answer: 101 when the WebSocket was accepted.
    /// </summary>
    public int StatusCode { get; }

    /// <summary>
    /// Gets the sub-protocol the server answered with, or <see langword="null"/> when it named none.
    /// </summary>
    public string? SubProtocol { get; }

    /// <summary>
    /// Gets the client's own address and port, as the server sees them.
    /// </summary>
    public IPEndPoint LocalEndPoint => (IPEndPoint)_socket.LocalEndPoint!; // A connected socket always has one.

    /// <summary>
    /// Opens a TCP connection and makes the WebSocket handshake, asking for the sub-protocol <c>mqtt</c>.
    /// </summary>
    /// <param name="uri">
    /// The <c>ws://</c> address.
    /// </param>
    /// <param name="receiveBufferSize">
    /// The socket's receive buffer in bytes, set before it connects, or 0 to keep the system's.
    /// </param>
    /// <returns>
    /// The client; its <see cref="StatusCode"/> says whether the handshake was accepted.
    /// </returns>
    public static async Task<RawWebSocket> ConnectAsync(Uri uri, int receiveBufferSize = 0)
    {
        Socket socket = new(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
        if (receiveBufferSize > 0)
        {
            socket.ReceiveBufferSize = receiveBufferSize;
        }

        await socket.ConnectAsync(IPAddress.Parse(uri.Host), uri.Port).ConfigureAwait(false);
        NetworkStream stream = new(socket, ownsSocket: true);
        string key = Convert.ToBase64String(RandomNumberGenerator.GetBytes(16));
        string request = $"GET {uri.AbsolutePath} HTTP/1.1\r\nHost: {uri.Authority}\r\nUpgrade: websocket\r\n"
                         + $"Connection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n"
                         + "Sec-WebSocket-Protocol: mqtt\r\n\r\n";
        await stream.WriteAsync(Encoding.ASCII.GetBytes(request)).ConfigureAwait(false);

        // Byte by byte, so that nothing the server sends right after the answer is taken with it.
        StringBuilder answer = new();
        byte[] one = new byte[1];
        while (!answer.ToString().EndsWith("\r\n\r\n", StringComparison.Ordinal))
        {
            await stream.ReadExactlyAsync(one).ConfigureAwait(false);
            answer.Append((char)one[0]);
        }

        string[] lines = answer.ToString().Split("\r\n");
        int statusCode = int.Parse(lines[0].Split(' ')[1], System.Globalization.CultureInfo.InvariantCulture);
        string? subProtocol = null;
        foreach (string line in lines)
        {
            if (line.StartsWith("Sec-WebSocket-Protocol:", StringComparison.OrdinalIgnoreCase))
            {
                subProtocol = line["Sec-WebSocket-Protocol:".Length..].Trim();
            }
        }

        return new RawWebSocket(socket, stream, statusCode, subProtocol);
    }

    /// <summary>
    /// Sends one final, masked frame.
    /// </summary>
    /// <param name="opcode">
    /// The frame's opcode.
    /// </param>
    /// <param name="payload">
    /// The frame's payload.
    /// </param>
    /// <returns>
    /// A task that completes when the frame has been handed to the socket.
    /// </returns>
    public async Task SendAsync(byte opcode, ReadOnlyMemory<byte> payload)
    {
        byte[] frame = new byte[14 + payload.Length];
        int at = 0;
        frame[at++] = (byte)(0x80 | opcode);
        if (payload.Length < 126)
        {
            frame[at++] = (byte)(0x80 | payload.Length);
        }
        else if (payload.Length <= ushort.MaxValue)
        {
            frame[at++] = 0x80 | 126;
            BinaryPrimitives.WriteUInt16BigEndian(frame.AsSpan(at), (ushort)payload.Length);
            at += 2;
        }
        else
        {
            frame[at++] = 0x80 | 127;
            BinaryPrimitives.WriteUInt64BigEndian(frame.AsSpan(at), (ulong)payload.Length);
            at += 8;
        }

        byte[] mask = RandomNumberGenerator.GetBytes(4);
        mask.CopyTo(frame, at);
        at += 4;
        ReadOnlySpan<byte> bytes = payload.Span;
        for (int i = 0; i < bytes.Length; i++)
        {
            frame[at + i] = (byte)(bytes[i] ^ mask[i % 4]);
        }

        await _stream.WriteAsync(frame.AsMemory(0, at + bytes.Length)).ConfigureAwait(false);
    }

    /// <summary>
    /// Receives the next frame whole, control frames included.
    /// </summary>
    /// <returns>
    /// The frame.
    /// </returns>
    /// <exception cref="EndOfStreamException">
    /// The server closed the TCP connection before a whole frame arrived.
    /// </exception>
    /// <exception cref="InvalidDataException">
    /// The frame is not one a server may send: masked, fragmented, or with reserved bits set.
    /// </exception>
    public async Task<Frame> ReceiveAsync()
    {
        byte[] header = new byte[8];
        await _stream.ReadExactlyAsync(header.AsMemory(0, 2)).ConfigureAwait(false);
        if (header[0] != (0x80 | (header[0] & 0x0F)) || (header[1] & 0x80) != 0)
        {
            throw new InvalidDataException($"Not a final, unmasked frame: {header[0]:X2} {header[1]:X2}.");
        }

        byte opcode = (byte)(header[0] & 0x0F);
        long length = header[1] & 0x7F;
        if (length == 126)
        {
            await _stream.ReadExactlyAsync(header.AsMemory(0, 2)).ConfigureAwait(false);
            length = BinaryPrimitives.ReadUInt16BigEndian(header);
        }
        else if (length == 127)
        {
            await _stream.ReadExactlyAsync(header.AsMemory(0, 8)).ConfigureAwait(false);
            length = (long)BinaryPrimitives.ReadUInt64BigEndian(header);
        }

        byte[] payload = new byte[length];
        await _stream.ReadExactlyAsync(payload).ConfigureAwait(false);
        return new Frame(opcode, payload);
    }

    /// <summary>
    /// Closes the TCP connection without a WebSocket close.
    /// </summary>
    /// <returns>
    /// A completed task.
    /// </returns>
    public ValueTask DisposeAsync() => _stream.DisposeAsync();

    /// <summary>
    /// One frame received.
    /// </summary>
    /// <param name="Opcode">
    /// The opcode.
    /// </param>
    /// <param name="Payload">
    /// The payload.
    /// </param>
    public sealed record Frame(byte Opcode, byte[] Payload)
    {
        /// <summary>
        /// Gets the status of a close frame, or 0 when the frame is not one or carries none.
        /// </summary>
        public int CloseStatus =>
            Opcode == Close && Payload.Length >= 2 ? BinaryPrimitives.ReadUInt16BigEndian(Payload) : 0;
    }
}
