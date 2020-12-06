using System;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using MessageBroker.Config;
using MessageBroker.EventBusKafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MessageBroker.Broker
{
    /// <summary>
    /// Broker with retry functionality
    /// </summary>
    public class RetryBroker: Broker<MessageData.MessageData>
    {
        private readonly ILogger<RetryBroker> _logger;

        public override async Task<Message<byte[], MessageData.MessageData>[]> Consume()
        {
            Count = Consumer.ConsumeAndGetCount(CommonConfig.CurrentValue.BatchSize);
            if (Count <= 0)
            {
                return null;
            }

            var messages = Consumer.GetConsumed();
            await WaitTillNextSending(messages);
            return messages.Take(Count).Select(messages => new Message<byte[], MessageData.MessageData>
            { Key = messages.Key, Value = ModifyMessageData(messages.Value) }).ToArray();
        }

        public override async Task<HttpStatusCode[]> SendToApi(Message<byte[], MessageData.MessageData>[] messages, CancellationToken cancellationToken)
        {
            for (var i = 0; i < Count; i++)
            {
                Tasks[i] = ApiSender.Send(messages[i].Value, cancellationToken, BrokerStrategy.EventsRetry);
            }

            return await Task.WhenAll(Tasks.Take(Count));
        }

        public override void ProduceMessages(HttpStatusCode[] statusCodes, Message<byte[], MessageData.MessageData>[] messages)
        {
            for (var i = 0; i < Count; i++)
            {
                if (CommonConfig.CurrentValue.InfinityRetryMode && IsClientErrorStatusCode(statusCodes[i]) ||
                    !(CommonConfig.CurrentValue.InfinityRetryMode && IsServerErrorStatusCode(statusCodes[i])) &&
                    messages[i].Value.Tries <= 0)
                {
                    ByteProducer.ProduceToDead(messages[i].Key, messages[i].Value.Content);
                }
                else
                {
                    MessageDataProducer.ProduceToRetry(messages[i].Key, messages[i].Value);
                }
            }
        }

        private static MessageData.MessageData ModifyMessageData(MessageData.MessageData messageData)
        {
            if (messageData.Tries > 0)
            {
                messageData.Tries--;
            }

            messageData.Ticks = DateTime.Now.Ticks;
            return messageData;
        }

        private async Task WaitTillNextSending(Message<byte[], MessageData.MessageData>[] consumeResults)
        {
            var lastResult = consumeResults.Take(Count).LastOrDefault();
            if (lastResult == null)
            {
                return;
            }

            var lastTicks = lastResult.Value.Ticks;
            var timePassed = TimeSpan.FromTicks(DateTime.Now.Ticks - lastTicks);
            var retryInterval = TimeSpan.FromSeconds(CommonConfig.CurrentValue.RetryInterval);
            if (timePassed < retryInterval)
            {
                var diff = retryInterval - timePassed;
                _logger.LogInformation($"Waiting for {diff}...");
                await Task.Delay(diff);
            }
        }

        public RetryBroker(Consumer<MessageData.MessageData> consumer, Producer<byte[]> byteProducer,
            Producer<MessageData.MessageData> messageDataProducer, IOptionsMonitor<CommonConfig> commonConfig, ApiSender apiSender,
            ILogger<RetryBroker> logger)
    : base(consumer, byteProducer, messageDataProducer, commonConfig, apiSender)
        {
            Consumer.Subscribe(BrokerStrategy.EventsRetry);
            _logger = logger;
        }
    }
}
