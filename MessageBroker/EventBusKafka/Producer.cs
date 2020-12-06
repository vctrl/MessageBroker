using System;
using Confluent.Kafka;
using MessageBroker.Config;
using MessageBroker.MessageData;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MessageBroker.EventBusKafka
{
    public class Producer<T>: IDisposable
    {
        private readonly IOptionsMonitor<KafkaConfig> _kafkaConfig;
        private readonly ILogger _logger;
        private readonly IProducer<byte[], T> _producer;

        public Producer(
            IOptionsMonitor<KafkaConfig> kafkaConfig,
            ILogger<Producer<T>> logger)
        {
            _kafkaConfig = kafkaConfig;
            _logger = logger;
            _producer = InitializeProducer();
        }

        public void ProduceToDead(byte[] key, T value)
        {
            Produce(key, value, $"{_kafkaConfig.CurrentValue.Topic}_dead");
        }

        public void ProduceToRetry(byte[] key, T value)
        {
            Produce(key, value, $"{_kafkaConfig.CurrentValue.Topic}_retry");
        }

        public void Dispose()
        {
            _producer?.Dispose();
        }

        private IProducer<byte[], T> InitializeProducer()
        {
            var config = new ProducerConfig { BootstrapServers = string.Join(',', _kafkaConfig.CurrentValue.Brokers) };
            var producerBuilder = new ProducerBuilder<byte[], T>(config);
            if (typeof(T) == typeof(MessageData.MessageData))
            {
                producerBuilder.SetValueSerializer((ISerializer<T>)new MessageDataSerializer());
            }

            return producerBuilder.Build();
        }

        private void Produce(byte[] key, T value, string topic)
        {
            void Handler(DeliveryReport<byte[], T> r) =>
                _logger.LogInformation(!r.Error.IsError
                ? $"Delivered message Key: '{key.ToString()}', Value: '{value.ToString()}' to {r.TopicPartitionOffset}"
                : $"Delivery Error: {r.Error.Reason}");

            _producer.Produce(topic, new Message<byte[], T> { Key = key, Value = value }, Handler);
        }
    }
}
