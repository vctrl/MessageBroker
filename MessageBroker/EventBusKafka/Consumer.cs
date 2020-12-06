using System;
using Confluent.Kafka;
using MessageBroker.Broker;
using MessageBroker.Config;
using MessageBroker.MessageData;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MessageBroker.EventBusKafka
{
    public class Consumer<T>: IDisposable
    {
        private readonly IOptionsMonitor<KafkaConfig> _kafkaConfig;
        private readonly ILogger _logger;
        private readonly IConsumer<byte[], T> _consumer;

        private Message<byte[], T>[] _currentConsumeResults;
        private const int Day = 24 * 60 * 60 * 1000;

        public Consumer(IOptionsMonitor<KafkaConfig> kafkaConfig,
                ILogger<Consumer<T>> logger)
        {
            _kafkaConfig = kafkaConfig;
            _logger = logger;
            _consumer = InitializeConsumer();
            _currentConsumeResults = null;
        }

        public int ConsumeAndGetCount(int batchSize)
        {
            if (_currentConsumeResults == null)
            {
                _currentConsumeResults = new Message<byte[], T>[batchSize];
            }

            var timeout = new TimeSpan(_kafkaConfig.CurrentValue.CheckInterval * TimeSpan.TicksPerSecond);
            int cnt;
            for (cnt = 0; cnt < batchSize; cnt++)
            {
                var cr = _consumer.Consume(timeout);
                if (cr == null)
                {
                    break;
                }

                _currentConsumeResults[cnt] = cr.Message;
                _logger.LogInformation($"Consumed message Key: '{cr.Key.ToString()}', Value: '{cr.Value.ToString()}' at: '{cr.TopicPartition}'");
            }

            return cnt;
        }

        public Message<byte[], T>[] GetConsumed()
        {
            return _currentConsumeResults;
        }

        public void Commit()
        {
            _consumer.Commit();
        }

        public void Subscribe(BrokerStrategy strategy)
        {
            string topic;
            switch (strategy)
            {
                case BrokerStrategy.EventsRetry:
                    topic = $"{_kafkaConfig.CurrentValue.Topic}_retry";
                    break;
                case BrokerStrategy.SomeEvents:
                    topic = _kafkaConfig.CurrentValue.SomeEventTopic;
                    break;
                default:
                    topic = _kafkaConfig.CurrentValue.Topic;
                    break;
            }
        }

        public void Dispose()
        {
            _consumer?.Dispose();
        }

        private IConsumer<byte[], T> InitializeConsumer()
        {
            var config = new ConsumerConfig
            {
                GroupId = _kafkaConfig.CurrentValue.ConsumerGroup,
                BootstrapServers = string.Join(',', _kafkaConfig.CurrentValue.Brokers),
                MaxPollIntervalMs = Day,
                EnableAutoCommit = false,
                EnableAutoOffsetStore = false
            };

            var consumerBuilder = new ConsumerBuilder<byte[], T>(config);
            if (typeof(T) == typeof(MessageData.MessageData))
            {
                consumerBuilder.SetValueDeserializer((IDeserializer<T>)new MessageDataDeserializer());
            }

            var consumer = consumerBuilder.Build();
            return consumer;
        }
    }
}
