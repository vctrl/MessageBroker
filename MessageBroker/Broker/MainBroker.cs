using System;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using MessageBroker.Config;
using MessageBroker.EventBusKafka;
using Microsoft.Extensions.Options;

namespace MessageBroker.Broker
{
    /// <summary>
    /// Main broker service
    /// </summary>
    public class MainBroker : Broker<byte[]>
    {
        public override async Task<Message<byte[], byte[]>[]> Consume()
        {
            Count = Consumer.ConsumeAndGetCount(CommonConfig.CurrentValue.BatchSize);
            return Count > 0 ? Consumer.GetConsumed() : null;
        }

        public override async Task<HttpStatusCode[]> SendToApi(Message<byte[], byte[]>[] messages, CancellationToken cancellationToken)
        {
            for (var i = 0; i < Count; i++)
            {
                Tasks[i] = ApiSender.Send(messages[i].Value, cancellationToken, BrokerStrategy.EventsDefault);
            }

            return await Task.WhenAll(Tasks.Take(Count));
        }

        public override void ProduceMessages(HttpStatusCode[] statusCodes, Message<byte[], byte[]>[] messages)
        {
            for (var i = 0; i < Count; i++)
            {
                if (IsSuccessStatusCode(statusCodes[i]))
                {
                    continue;
                }

                if (CommonConfig.CurrentValue.InfinityRetryMode && IsClientErrorStatusCode(statusCodes[i]))
                {
                    ByteProducer.ProduceToDead(messages[i].Key, messages[i].Value);
                }
                else
                {
                    MessageDataProducer.ProduceToRetry(messages[i].Key, new MessageData.MessageData
                    { Content = messages[i].Value, Ticks = DateTime.Now.Ticks, Tries = CommonConfig.CurrentValue.RetryCount });
                }
            }
        }

        public MainBroker(Consumer<byte[]> consumer, Producer<byte[]> byteProducer,
            Producer<MessageData.MessageData> messageDataProducer, IOptionsMonitor<CommonConfig> commonConfig, ApiSender apiSender)
            : base(consumer, byteProducer, messageDataProducer, commonConfig, apiSender)
        {
            Consumer.Subscribe(BrokerStrategy.EventsDefault);
        }
    }
}
