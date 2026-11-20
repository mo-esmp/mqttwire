using System.Text;
using MqttWire.Codec;

namespace MqttWire.Tests.Codec;

/// <summary>
/// Tests the topic name and topic filter rules.
/// </summary>
public class TopicsTests
{
    /// <summary>
    /// Proves a topic name is valid when it is not empty and holds no wildcard.
    /// </summary>
    /// <param name="name">
    /// The topic name.
    /// </param>
    /// <param name="valid">
    /// Whether the name is valid.
    /// </param>
    [Theory]
    [InlineData("a", true)]
    [InlineData("/", true)]
    [InlineData("ü", true)]
    [InlineData("", false)]
    [InlineData("a/+", false)]
    [InlineData("#", false)]
    public void IsValidName_Name_IsValidWithoutWildcards(string name, bool valid)
    {
        // Arrange
        byte[] utf8 = Encoding.UTF8.GetBytes(name);

        // Act
        bool result = Topics.IsValidName(utf8);

        // Assert
        result.ShouldBe(valid);
    }

    /// <summary>
    /// Proves each topic filter rule: wildcards only as whole levels, the multi-level one last, and the shared
    /// subscription form.
    /// </summary>
    /// <param name="filter">
    /// The topic filter.
    /// </param>
    /// <param name="valid">
    /// Whether the filter is valid.
    /// </param>
    [Theory]
    [InlineData("a/b", true)]
    [InlineData("#", true)]
    [InlineData("a/#", true)]
    [InlineData("+", true)]
    [InlineData("+/a/+", true)]
    [InlineData("/+", true)]
    [InlineData("$share/g/a/+", true)]
    [InlineData("$share/g/#", true)]
    [InlineData("$SYS/#", true)]
    [InlineData("a//b", true)]
    [InlineData("/", true)]
    [InlineData("+/+", true)]
    [InlineData("a/b/c", true)]
    [InlineData("a/+/c", true)]
    [InlineData("$share", true)]
    [InlineData("$sharex/a", true)]
    [InlineData("$share/g/+/a", true)]
    [InlineData("$share/g/a/#", true)]
    [InlineData("#/a", false)]
    [InlineData("a/#/", false)]
    [InlineData("$share/", false)]
    [InlineData("", false)]
    [InlineData("a/#/b", false)]
    [InlineData("a#", false)]
    [InlineData("#a", false)]
    [InlineData("a/b#", false)]
    [InlineData("a+", false)]
    [InlineData("a+/b", false)]
    [InlineData("+a", false)]
    [InlineData("$share/g", false)]
    [InlineData("$share/g/", false)]
    [InlineData("$share//a", false)]
    [InlineData("$share/g+/a", false)]
    [InlineData("$share/g#/a", false)]
    [InlineData("$share/g/a#", false)]
    public void IsValidFilter_Filter_FollowsTheWildcardAndShareRules(string filter, bool valid)
    {
        // Arrange
        byte[] utf8 = Encoding.UTF8.GetBytes(filter);

        // Act
        bool result = Topics.IsValidFilter(utf8);

        // Assert
        result.ShouldBe(valid);
    }
}
