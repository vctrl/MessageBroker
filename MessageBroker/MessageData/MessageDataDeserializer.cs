using System;
using System.Text.Json;
using Confluent.Kafka;

namespace MessageBroker.MessageData
{
    public class MessageDataDeserializer: IDeserializer<MessageData>
    {
        public MessageData Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            return JsonSerializer.Deserialize<MessageData>(data);
        }
    }
}
