using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using MessageBroker.Broker;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace MessageBroker
{
    public class BrokerService<T> : IHostedService
    {
        private readonly Broker<T> _broker;
        private readonly ILogger<BrokerService<T>> _logger;

        public BrokerService(Broker<T> broker, ILogger<BrokerService<T>> logger)
        {
            _broker = broker;
            _logger = logger;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            Task.Run(() =>
            {
                while (true)
                {
                    if (!ProcessMessages(cancellationToken))
                    {
                        break;
                    }
                }
            }, cancellationToken);

            return Task.CompletedTask;
        }

        private bool ProcessMessages(CancellationToken cancellationToken)
        {
            try
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    return false;
                }

                var consumedMessages = _broker.Consume().Result;
                if (consumedMessages == null) return true;
                // don't need to produce
                // just don't commit if there's bad status codes
                // if (_broker.TrySendApi(consumedMessages, cancellationToken, out var statusCodes))
                // {
                // _broker.ProduceMessages(statusCodes, consumedMessages);
                // _broker.Commit();
                // }

                var statusCodes = _broker.SendToApi(consumedMessages, cancellationToken).Result;
                _broker.ProduceMessages(statusCodes, consumedMessages);
                _broker.Commit();
            }
            catch (ConsumeException e)
            {
                _logger.LogError($"ConsumeAndGetCount failed: {e.Error.Reason}");
            }
            catch (ProduceException<Null, string> e)
            {
                _logger.LogError($"Produce failed: {e.Error.Reason}");
            }
            catch (Exception e)
            {
                _logger.LogError(e.Message);
            }

            return true;

        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _broker.Dispose();
            return Task.CompletedTask;
        }
    }
}