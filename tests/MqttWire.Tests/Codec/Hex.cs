namespace MqttWire.Tests.Codec;

internal static class Hex
{
    public static byte[] Parse(string spaced)
        => spaced.Length == 0 ? [] : Convert.FromHexString(spaced.Replace(" ", string.Empty));
}
