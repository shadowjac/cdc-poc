using System.Text.Json.Serialization;

namespace CdcConsumer.Models;

[JsonSourceGenerationOptions(NumberHandling = System.Text.Json.Serialization.JsonNumberHandling.AllowReadingFromString)]
[JsonSerializable(typeof(OrderRow))]
internal partial class CdcJsonContext : JsonSerializerContext
{
}