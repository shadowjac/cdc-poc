using CdcConsumer;
using OpenTelemetry;
using OpenTelemetry.Resources;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddHostedService<OrdersCdcConsumer>();
builder.Services
    .AddOptions<KafkaOptions>()
    .BindConfiguration(KafkaOptions.SectionName)
    .ValidateDataAnnotations()
    .ValidateOnStart();

builder.Services
    .AddOpenTelemetry()
    .ConfigureResource(r => r.AddService("cdc-consumer"))
    .WithLogging()
    .WithTracing(t => t.AddSource(OrdersCdcConsumer.ActivitySourceName))
    .UseOtlpExporter();

var host = builder.Build();
await host.RunAsync();
