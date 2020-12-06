using MessageBroker.Broker;
using MessageBroker.Config;
using MessageBroker.EventBusKafka;
using MessageBroker.Infrastructure;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace MessageBroker
{
    public class Startup
    {
        private readonly IWebHostEnvironment _env;

        public Startup(IConfiguration configuration, IWebHostEnvironment env)
        {
            Configuration = configuration;
            _env = env;
        }

        public IConfiguration Configuration { get; }

        public void ConfigureServices(IServiceCollection services)
        {
            services.Configure<KafkaConfig>(Configuration.GetSection(nameof(KafkaConfig)));
            services.Configure<ApiConfig>(Configuration.GetSection(nameof(ApiConfig)));
            services.Configure<CommonConfig>(Configuration.GetSection(nameof(CommonConfig)));
            services.AddSingleton<Consumer<byte[]>>();
            services.AddSingleton<Consumer<MessageData.MessageData>>();
            services.AddSingleton<Producer<byte[]>>();
            services.AddSingleton<Producer<MessageData.MessageData>>();
            services.AddHttpClient();
            services.AddSingleton<ApiSender>();

            // Main broker service
            services.AddSingleton<Broker<byte[]>, MainBroker>();
            services.AddHostedService<BrokerService<byte[]>>();

            // Retry broker service
            services.AddSingleton<Broker<MessageData.MessageData>, RetryBroker>();
            services.AddHostedService<BrokerService<MessageData.MessageData>>();

            // Some events broker service
            var config = Configuration.GetSection(nameof(ApiConfig)).Get<ApiConfig>().EventsConfig;
            var serviceConfig = new ServiceConfig
            {
                ServiceAddress = $"{config.Host}:{config.Port}/{config.Url}",
                AuthKey = config.AuthKey
            };

            services.AddSingleton<IServiceClient, ServiceClient>(c =>
                new ServiceClient(serviceConfig));

            services.AddSingleton<Broker<byte[]>, SomeEventsBroker>();
            services.AddHostedService<BrokerService<byte[]>>();
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }

            app.UseRouting();

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapGet("/", async context => { await context.Response.WriteAsync("Hello Broker!"); });
            });
        }
    }
}
