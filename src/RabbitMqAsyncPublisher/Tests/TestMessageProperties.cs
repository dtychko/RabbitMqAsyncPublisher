using System.Collections.Generic;
using RabbitMqAsyncPublisher;

namespace Tests
{
    internal class TestMessageProperties : MessageProperties
    {
        public const string TagName = "__test__tag__";

        public TestMessageProperties(string testTag) : base(
            headers: new Dictionary<string, object>
            {
                [TagName] = testTag
            })
        {
        }
        
        public string TestTag => Headers.TryGetValue(TagName, out var value) ? value.ToString() : null;
    }
}