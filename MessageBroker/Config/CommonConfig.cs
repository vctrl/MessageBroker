namespace MessageBroker.Config
{
    public class CommonConfig
    {
        public int BatchSize { get; set; }

        public bool InfinityRetryMode { get; set; }

        public int RetryCount { get; set; }

        /// <summary>
        /// Time waiting for retry attempt, in seconds
        /// </summary>
        public int RetryInterval { get; set; }
    }
}
