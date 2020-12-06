using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using MessageBroker.Config;
using MessageBroker.EventBusKafka;
using MessageBroker.Infrastructure;
using Microsoft.Extensions.Options;

namespace MessageBroker.Broker
{
    /// <summary>
    /// This broker takes messages from other topic and use custom client to send message
    /// </summary>
    public class SomeEventsBroker : Broker<byte[]>
    {
        private readonly IServiceClient _serviceClient;

        public override async Task<Message<byte[], byte[]>[]> Consume()
        {
            Count = Consumer.ConsumeAndGetCount(CommonConfig.CurrentValue.BatchSize);
            return Count > 0 ? Consumer.GetConsumed() : null;
        }

        public override async Task<HttpStatusCode[]> SendToApi(Message<byte[], byte[]>[] messages, CancellationToken cancellationToken)
        {
            // todo use ServiceClient
            throw new NotImplementedException();
        }

        public override void ProduceMessages(HttpStatusCode[] statusCodes, Message<byte[], byte[]>[] messages)
        {
            // todo no need to produce
            throw new NotImplementedException();
        }


        public SomeEventsBroker(Consumer<byte[]> consumer, Producer<byte[]> byteProducer,
            Producer<MessageData.MessageData> messageDataProducer, IOptionsMonitor<CommonConfig> commonConfig, ApiSender apiSender,
            IServiceClient serviceClient)
            : base(consumer, byteProducer, messageDataProducer, commonConfig, apiSender)
        {
            _serviceClient = serviceClient;
            Consumer.Subscribe(BrokerStrategy.EventsDefault);
        }
    }
}
