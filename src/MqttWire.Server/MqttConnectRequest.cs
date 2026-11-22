using System.Net;
using Microsoft.AspNetCore.Http;

namespace MqttWire.Server;

/// <summary>
/// The CONNECT of a client as the host needs it: client id, user name, password, the user properties the client
/// sent, the remote end point, whether the connection is secure, and the HTTP context of the WebSocket request.
/// It owns copies of what the CONNECT held, so it may be used across awaits inside
/// <see cref="IMqttWireHandler.OnConnectAsync"/>; the HTTP context is the request's own.
/// </summary>
public sealed class MqttConnectRequest
{
    /// <summary>
    /// Creates a request.
    /// </summary>
    /// <param name="clientId">
    /// The client identifier.
    /// </param>
    /// <param name="userName">
    /// The user name, or <see langword="null"/> when the client sent none.
    /// </param>
    /// <param name="password">
    /// The password, or <see langword="null"/> when the client sent none; the request keeps this array.
    /// </param>
    /// <param name="userProperties">
    /// The user properties the client sent; the request keeps this list.
    /// </param>
    /// <param name="remoteEndPoint">
    /// The remote end point, or <see langword="null"/> when it is not known.
    /// </param>
    /// <param name="isSecure">
    /// Whether the connection is secure.
    /// </param>
    /// <param name="httpContext">
    /// The context of the WebSocket request.
    /// </param>
    internal MqttConnectRequest(string clientId, string? userName, byte[]? password,
        IReadOnlyList<MqttWireUserProperty> userProperties, IPEndPoint? remoteEndPoint, bool isSecure,
        HttpContext httpContext)
    {
        ClientId = clientId;
        UserName = userName;
        HasPassword = password is not null;
        Password = password;
        UserProperties = userProperties;
        RemoteEndPoint = remoteEndPoint;
        IsSecure = isSecure;
        HttpContext = httpContext;
    }

    /// <summary>
    /// Gets the client identifier the client sent: never empty and at most 256 UTF-8 bytes. The server assigns
    /// none: a CONNECT with an empty client identifier is refused with 0x85 before the host is asked.
    /// </summary>
    public string ClientId { get; }

    /// <summary>
    /// Gets the user name, or <see langword="null"/> when the client sent none.
    /// </summary>
    public string? UserName { get; }

    /// <summary>
    /// Gets a value indicating whether the client sent a password; an empty password is a password.
    /// </summary>
    public bool HasPassword { get; }

    /// <summary>
    /// Gets the password bytes; empty when the client sent none or an empty one (see <see cref="HasPassword"/>).
    /// </summary>
    public ReadOnlyMemory<byte> Password { get; }

    /// <summary>
    /// Gets the user properties the client sent, in order.
    /// </summary>
    public IReadOnlyList<MqttWireUserProperty> UserProperties { get; }

    /// <summary>
    /// Gets the remote end point, or <see langword="null"/> when it is not known.
    /// </summary>
    public IPEndPoint? RemoteEndPoint { get; }

    /// <summary>
    /// Gets a value indicating whether the WebSocket request came over HTTPS, as the request itself says
    /// (<see cref="HttpRequest.IsHttps"/>).
    /// </summary>
    public bool IsSecure { get; }

    /// <summary>
    /// Gets the context of the WebSocket request: headers, cookies and the authenticated user. It is the request's
    /// own object, not a copy: it is valid only while the connection lives, so a host that needs something from it
    /// later takes it out during <see cref="IMqttWireHandler.OnConnectAsync"/>.
    /// </summary>
    public HttpContext HttpContext { get; }
}
