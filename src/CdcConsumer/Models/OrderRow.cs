using System.Text.Json.Serialization;

namespace CdcConsumer.Models;

public record OrderRow(
    int Id,
    string? Customer,
    decimal Total,
    [property: JsonPropertyName("__op")]string Op,
    [property: JsonPropertyName("__deleted")]string Deleted);