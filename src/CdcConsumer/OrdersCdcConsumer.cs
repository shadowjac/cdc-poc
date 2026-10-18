using System.Diagnostics;
using System.Text.Json;
using CdcConsumer.Models;
using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace CdcConsumer;

public sealed partial class OrdersCdcConsumer : BackgroundService
{
    public const string ActivitySourceName = "CdcConsumer";
    private static readonly ActivitySource Source = new(ActivitySourceName);
    
    private readonly KafkaOptions _kafkaOptions;
    private readonly ILogger<OrdersCdcConsumer> _logger;

    public OrdersCdcConsumer(IOptions<KafkaOptions> kafkaOptions, ILogger<OrdersCdcConsumer> logger)
    {
        _kafkaOptions = kafkaOptions.Value;
        _logger = logger;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        Task.Run(() =>
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = _kafkaOptions.BootstrapServers,
                GroupId = _kafkaOptions.GroupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            };

            using var consumer = new ConsumerBuilder<string, string>(config)
                .Build();

            consumer.Subscribe(_kafkaOptions.Topic);
            LogSubscribed(_kafkaOptions.Topic);

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    var cr = consumer.Consume(stoppingToken);
                    using var activity = Source.StartActivity("process order change", ActivityKind.Consumer);
                    activity?.SetTag("messaging.system", "kafka");
                    activity?.SetTag("messaging.destination.name", cr.Topic);
                    activity?.SetTag("messaging.kafka.partition", cr.Partition.Value);
                    activity?.SetTag("messaging.kafka.offset", cr.Offset.Value);
                    
                    LogMessageReceived(cr.Message.Value);

                    if (cr.Message.Value is not null)
                    {
                        var order = JsonSerializer.Deserialize<OrderRow>(cr.Message.Value,
                            CdcJsonContext.Default.OrderRow!);
                        
                        activity?.SetTag("order.id", order!.Id);
                        activity?.SetTag("cdc.op", order!.Op);
                        
                        LogOrderChange(order!.Op, order.Id, order.Customer, order.Total, order.Deleted);
                    }

                    consumer.Commit();
                }
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("Operation cancelled");
            }
            finally
            {
                consumer.Close();
            }
        }, stoppingToken);

    [LoggerMessage(LogLevel.Information, Message = "Subscribed to topic: {Topic}")]
    private partial void LogSubscribed(string topic);

    [LoggerMessage(Level = LogLevel.Information,
        Message = "{Op} Id={Id} Customer={Customer} Total={Total} Deleted={Deleted}")]
    private partial void LogOrderChange(string op, int id, string? customer, decimal total, string deleted);

    [LoggerMessage(Level = LogLevel.Information, Message = "Message received: {Message}")]
    private partial void LogMessageReceived(string message);
}