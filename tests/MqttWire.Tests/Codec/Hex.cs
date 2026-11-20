namespace MqttWire.Tests.Codec;

/// <summary>
/// Builds byte arrays from hex text for the tests.
/// </summary>
internal static class Hex
{
    /// <summary>
    /// Parses hex digits written with optional spaces between the bytes.
    /// </summary>
    /// <param name="spaced">
    /// The hex text, such as <c>"30 0A"</c>; empty for no bytes.
    /// </param>
    /// <returns>
    /// The bytes the text spells.
    /// </returns>
    public static byte[] Parse(string spaced)
        => spaced.Length == 0 ? [] : Convert.FromHexString(spaced.Replace(" ", string.Empty));
}
