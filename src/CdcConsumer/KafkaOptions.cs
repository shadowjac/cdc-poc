using System.ComponentModel.DataAnnotations;

public sealed class KafkaOptions
{
    public const string SectionName = "Kafka";

    [Required] public string BootstrapServers { get; set; } = string.Empty;
    [Required] public string GroupId { get; set; } = string.Empty;
    [Required] public string Topic { get; set; } = string.Empty;
}