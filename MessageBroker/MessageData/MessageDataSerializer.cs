using System;
using System.Text.Json;
using Confluent.Kafka;

namespace MessageBroker.MessageData
{
    public class MessageDataSerializer: ISerializer<MessageData>
    {
        public byte[] Serialize(MessageData data, SerializationContext context)
        {
            return JsonSerializer.SerializeToUtf8Bytes(data);
        }
    }
}
