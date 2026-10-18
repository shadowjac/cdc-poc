using CdcConsumer;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddHostedService<OrdersCdcConsumer>();
builder.Services
    .AddOptions<KafkaOptions>()
    .BindConfiguration(KafkaOptions.SectionName)
    .ValidateDataAnnotations()
    .ValidateOnStart();

var host = builder.Build();
await host.RunAsync();
