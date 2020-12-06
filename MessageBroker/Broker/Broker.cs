using System;
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
    /// Base broker class.
    /// </summary>
    /// <typeparam name="T">Type to consume.</typeparam>
    public abstract class Broker<T>: IDisposable
    {
        protected readonly Consumer<T> Consumer;

        protected readonly Producer<byte[]> ByteProducer;

        protected readonly Producer<MessageData.MessageData> MessageDataProducer;

        protected readonly IOptionsMonitor<CommonConfig> CommonConfig;

        protected readonly ApiSender ApiSender;

        protected readonly Task<HttpStatusCode>[] Tasks;

        protected int Count;

        protected Broker(Consumer<T> consumer, Producer<byte[]> byteProducer,
            Producer<MessageData.MessageData> messageDataProducer, IOptionsMonitor<CommonConfig> commonConfig, ApiSender apiSender)
        {
            Consumer = consumer;
            ByteProducer = byteProducer;
            MessageDataProducer = messageDataProducer;
            CommonConfig = commonConfig;
            ApiSender = apiSender;
            Tasks = new Task<HttpStatusCode>[commonConfig.CurrentValue.BatchSize];
        }

        protected bool IsSuccessStatusCode(HttpStatusCode statusCode) => statusCode == HttpStatusCode.OK;

        protected bool IsClientErrorStatusCode(HttpStatusCode statusCode) => statusCode == HttpStatusCode.BadRequest;

        protected bool IsServerErrorStatusCode(HttpStatusCode statusCode) => statusCode == HttpStatusCode.InternalServerError;

        public abstract Task<Message<byte[], T>[]> Consume();

        public abstract Task<HttpStatusCode[]> SendToApi(Message<byte[], T>[] messages, CancellationToken cancellationToken);

        public abstract void ProduceMessages(HttpStatusCode[] statusCodes, Message<byte[], T>[] messages);

        public void Commit()
        {
            Consumer.Commit();
        }

        public void Dispose()
        {
            Consumer?.Dispose();
            ByteProducer?.Dispose();
            MessageDataProducer?.Dispose();
            ApiSender?.Dispose();
        }
    }
}
