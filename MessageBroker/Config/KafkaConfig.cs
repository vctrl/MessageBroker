namespace MessageBroker.Config
{
    public class KafkaConfig
    {
        public string[] Brokers { get; set; }

        public string ConsumerGroup { get; set; }

        public string Topic { get; set; }

        public string SomeEventTopic { get; set; }

        /// <summary>
        /// A number of seconds to wait till sending messages,
        /// if number of consumed messages less than threads count.
        /// </summary>
        public int CheckInterval { get; set; }
    }
}
