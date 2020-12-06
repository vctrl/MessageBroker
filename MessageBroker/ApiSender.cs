using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using MessageBroker.Broker;
using MessageBroker.Config;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Uri = MessageBroker.Config.Uri;

namespace MessageBroker
{
    public class ApiSender: IDisposable
    {
        private readonly HttpClient _httpClient;
        private readonly IOptionsMonitor<ApiConfig> _apiConfig;
        private readonly ILogger<ApiSender> _logger;

        public ApiSender(IOptionsMonitor<ApiConfig> apiConfig, IHttpClientFactory httpClientFactory,
            ILogger<ApiSender> logger)
        {
            _apiConfig = apiConfig;
            _httpClient = httpClientFactory.CreateClient();
            _logger = logger;
        }

        public Task<HttpStatusCode> Send(byte[] message, CancellationToken cancellationToken, BrokerStrategy strategy)
        {
            _logger.LogInformation($"Sending {message}...");
            return SendInternal(new ByteArrayContent(message), cancellationToken, strategy);
        }

        public Task<HttpStatusCode> Send(MessageData.MessageData message, CancellationToken cancellationToken, BrokerStrategy strategy)
        {
            _logger.LogInformation($"Sending {message}...");
            return SendInternal(new ByteArrayContent(message.Content), cancellationToken, strategy);
        }

        private async Task<HttpStatusCode> SendInternal(HttpContent httpContent, CancellationToken cancellationToken, BrokerStrategy strategy)
        {
            try
            {
                var request = new HttpRequestMessage(HttpMethod.Post, GetUri(strategy))
                {
                    Content = httpContent
                };

                request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
                _logger.LogInformation(
                    $"Request:\nMethod: {request.Method}, RequestUri: {request.RequestUri}, Version: {request.Version}, content: {request.Content}");
                var response = await _httpClient.SendAsync(request, cancellationToken);
                _logger.LogInformation(
                    $"Response:\nStatusCode: {response.StatusCode}, ReasonPhrase: {response.ReasonPhrase}, Version: {response.Version}");

                return response.StatusCode;
            }
            catch (Exception e)
            {
                _logger.LogError($"{e.Message}");
                return 0;
            }
        }

        public void Dispose()
        {
            _httpClient?.Dispose();
        }

        private string GetUri(BrokerStrategy strategy)
        {
            Uri apiConfig;
            switch (strategy)
            {
                case BrokerStrategy.SomeEvents:
                    apiConfig = _apiConfig.CurrentValue.EventsConfig;
                    break;
                default:
                    apiConfig = _apiConfig.CurrentValue.DefaultConfig;
                    break;
            }

            return $"{apiConfig.Host}:{apiConfig.Port}/{apiConfig.Url}";
        }
    }
}
