namespace MessageBroker.MessageData
{
    public class MessageData
    {
        /// <summary>
        /// Message content
        /// </summary>
        public byte[] Content { get; set; }

        /// <summary>
        /// Number of tries already done
        /// </summary>
        public int Tries { get; set; }

        /// <summary>
        /// Number of ticks which represents timestamp of last attempt
        /// </summary>
        public long Ticks { get; set; }

        public override string ToString()
        {
            return $"Content: {Content}, Tries left: {Tries}";
        }

        public MessageData()
        {
        }
    }
}
