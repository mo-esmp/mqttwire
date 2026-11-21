namespace MqttWire.Link;

/// <summary>
/// The outcome of opening a link: the link, or the network error that kept it from opening.
/// </summary>
/// <param name="Link">
/// The open link, or <see langword="null"/> when it could not be opened.
/// </param>
/// <param name="Error">
/// The network error when <paramref name="Link"/> is <see langword="null"/>; otherwise <see langword="null"/>.
/// </param>
internal readonly record struct LinkConnectResult(IMqttLink? Link, Exception? Error);
