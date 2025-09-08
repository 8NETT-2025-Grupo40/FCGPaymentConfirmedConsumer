// =====================================================================================================
//  LAMBDA: Disparado a partir do evento "payment.confirmed" vindo do SQS
// -----------------------------------------------------------------------------------------------------
//  VISÃO GERAL DO FLUXO
//  1) A Payment API publica no SQS um JSON com o evento "payment.confirmed" contendo userId, purchaseId e items.
//  2) Esta Lambda é acionada pelo SQS (trigger). Para cada mensagem:
//     2.1) Valida se o evento é "payment.confirmed". Outros eventos são ignorados.
//     2.2) Monta um POST para a Game API (endpoint de grants), usando:
//          - Idempotency-Key: <purchaseId>              -> para permitir retries seguros
//          - X-Correlation-Id: <CorrelationId|purchaseId>  -> rastreabilidade ponta a ponta
//          - X-API-Key: <opcional, serviço->serviço>    -> autenticação entre serviços
//     2.3) Política de resiliência (Polly):
//          - Retry com backoff exponencial + jitter em 429/5xx/timeout
//          - 2xx e 409 (já concedido) contam como sucesso
//          - 4xx (!= 409) é erro de contrato -> não-retryable -> descarta a mensagem (não entra em failures)
//  3) Retornamos Partial Batch Response: apenas mensagens realmente "retryable" (exemplo: 5xx) entram em BatchItemFailures.
// -----------------------------------------------------------------------------------------------------
//  EXEMPLO DE MENSAGEM DO SQS (MessageBody):
//  {
//    "event": "payment.confirmed",
//    "version": "1",
//    "occurredAt": "2025-09-05T12:34:56Z",
//    "userId": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
//    "purchaseId": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
//    "items": [
//      { "gameId": "G-123", "quantity": 1, "price": 49.90 },
//      { "gameId": "G-456", "quantity": 2, "price": 19.90 }
//    ]
//  }
// -----------------------------------------------------------------------------------------------------
//  VARIÁVEIS DE AMBIENTE (config da Lambda):
//  - GAME_API_BASE_URL     (obrigatória)
//  - GAME_API_GRANT_PATH   (opcional)     Default: /internal/grants
//  - GAME_API_KEY          (opcional)     Enviada no header X-API-Key
//  - CORRELATION_HEADER    (opcional)     Default: X-Correlation-Id
//  - HTTP_TIMEOUT_SECONDS  (opcional)     Default: 10
//  - HTTP_RETRY_ATTEMPTS   (opcional)     Default: 3
// -----------------------------------------------------------------------------------------------------
//  NOTAS DIDÁTICAS
//  - Idempotência: a Game API deve rejeitar duplicados (por exemplo, chave única por (UserId, GameId, PurchaseId)) e retornar 409 quando o grant já existe. Assim retries são seguros.
//  - Partial Batch: importante para não reprocessar com erro aquilo que já foi bem-sucedido no mesmo lote.
//  - Novo HttpRequest por tentativa: cada retry cria uma HttpRequestMessage nova (evita problemas de reuso de content).
//  - Json camelCase: usamos JsonSerializerDefaults.Web para produzir "purchaseId", "userId", etc.
// =====================================================================================================

using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using Microsoft.Extensions.Logging;
using Polly;

// Serializer padrão da Lambda para (de)serialização de eventos
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace FCGPaymentConfirmedConsumer;

/*
 * Representa o payload do evento de pagamento confirmado publicado no SQS pela Payment API.
 * Este contrato deve bater com o que a Payment API grava na Outbox/Queue (nomes camelCase).
 */
public sealed class PaymentConfirmedEvent
{
    /// <summary>Tipo do evento. Esperado: "payment.confirmed".</summary>
    [JsonPropertyName("event")] public string Event { get; set; } = "";

    /// <summary>Versão do contrato do evento.</summary>
    [JsonPropertyName("version")] public string Version { get; set; } = "1";

    /// <summary>Momento em que o evento ocorreu (útil para auditoria).</summary>
    [JsonPropertyName("occurredAt")] public DateTimeOffset OccurredAt { get; set; }

    /// <summary>Usuário que efetuou a compra.</summary>
    [JsonPropertyName("userId")] public string UserId { get; set; } = "";

    /// <summary>Identificador da compra (também usado como idempotency-key).</summary>
    [JsonPropertyName("purchaseId")] public string PurchaseId { get; set; } = "";

    /// <summary>Lista de itens/jogos adquiridos.</summary>
    [JsonPropertyName("items")] public List<PaymentItem> Items { get; set; } = new();
}

/// <summary>Item comprado em um "payment.confirmed".</summary>
public sealed class PaymentItem
{
    [JsonPropertyName("gameId")] public string GameId { get; set; } = "";
    [JsonPropertyName("quantity")] public int Quantity { get; set; }
    [JsonPropertyName("price")] public decimal Price { get; set; }
}

/*
 * Requisição enviada à Game API para conceder todos os jogos de uma compra (grant "em lote").
 * Mantemos o mesmo shape camelCase; a Game API deve ser idempotente por purchaseId.
 */
public sealed class GrantRequest
{
    public string PurchaseId { get; set; } = "";
    public string UserId { get; set; } = "";
    public List<GrantItem> Items { get; set; } = new();
}

/// <summary>Representa um jogo a ser concedido na Game API.</summary>
public sealed class GrantItem
{
    public string GameId { get; set; } = "";
    public int Quantity { get; set; }
    public decimal Price { get; set; }
}

#region Lambda Function

/// <summary>
/// Lambda que consome mensagens do SQS contendo <c>payment.confirmed</c>
/// e chama a Game API para liberar os jogos ao usuário.
/// </summary>
/// <remarks>
/// Regras principais:
/// - Apenas eventos "payment.confirmed" são processados; demais são ignorados (não geram erro).
/// - Idempotência: enviamos "Idempotency-Key = purchaseId" para a Game API.
/// - Sucesso: 2xx e 409 (já concedido) contam como processado.
/// - Retry: 429/5xx/timeout disparam retries com backoff exponencial + jitter (Polly).
/// - Partial batch: apenas mensagens que realmente falharam (após retries) entram em BatchItemFailures.
/// </remarks>
public class Function
{
    // Opções de Json para leitura (case-insensitive) e escrita (camelCase/Web).
    private static readonly JsonSerializerOptions JsonOpts = new() { PropertyNameCaseInsensitive = true };
    private static readonly JsonSerializerOptions JsonWriteOpts = new(JsonSerializerDefaults.Web);

    // Dependências/Config
    private readonly HttpClient _http;
    private readonly ILogger<Function> _logger;

    // Config externa (env vars)
    private readonly string _gameApiBase; // BaseAddress do HttpClient
    private readonly string _grantPath;   // Caminho relativo do endpoint (exemplo: "/internal/grants")
    private readonly string? _apiKey;     // X-API-Key opcional
    private readonly string _corrHeader;  // Nome do header de correlação (default: X-Correlation-Id)

    // Política de retry (Polly)
    private readonly AsyncPolicy<HttpResponseMessage> _retryPolicy;

    /// <summary>
    /// Construtor padrão usado pela AWS.
    /// </summary>
    public Function() : this(new HttpClient(), LoggerFactory.Create(b => b.AddConsole()).CreateLogger<Function>()) { }

    /// <summary>Construtor para injeção de dependências em testes.</summary>
    public Function(HttpClient http, ILogger<Function> logger)
    {
        this._http = http;
        this._logger = logger;

        // Leitura de configuração via ENV
        this._gameApiBase = Env("GAME_API_BASE_URL", required: true)!;
        this._grantPath = Env("GAME_API_GRANT_PATH") ?? "/internal/grants";
        this._apiKey = Env("GAME_API_KEY");
        this._corrHeader = Env("CORRELATION_HEADER") ?? "X-Correlation-Id";

        // Garante BaseAddress com "/" no final para combinar com RequestUri relativa.
        if (!this._gameApiBase.EndsWith("/"))
        {
            this._gameApiBase += "/";
        }

        this._http.BaseAddress = new Uri(this._gameApiBase);

        // Timeout de HttpClient (não confundir com timeout da Lambda)
        this._http.Timeout = TimeSpan.FromSeconds(int.TryParse(Env("HTTP_TIMEOUT_SECONDS"), out int s) && s > 0 ? s : 10);

        // Para o Retry, usa backoff exponencial + jitter para 429/5xx/timeout
        int retries = int.TryParse(Env("HTTP_RETRY_ATTEMPTS"), out int r) && r > 0 ? r : 2;
        Random jitter = new();

        this._retryPolicy = Policy<HttpResponseMessage>
            // falhas de rede
            .Handle<HttpRequestException>()
            // timeouts
            .Or<TaskCanceledException>()
            .OrResult(rsp => rsp.StatusCode == HttpStatusCode.TooManyRequests || (int)rsp.StatusCode >= 500)
            .WaitAndRetryAsync(
                retryCount: retries,
                sleepDurationProvider: attempt =>
                    TimeSpan.FromSeconds(Math.Pow(2, attempt)) + TimeSpan.FromMilliseconds(jitter.Next(0, 250)),
                onRetryAsync: (outcome, delay, attempt, _) =>
                {
                    this._logger.LogWarning("Retry {Attempt} em {Delay} por {Reason}",
                        attempt, delay, outcome.Exception?.Message ?? outcome.Result.StatusCode.ToString());
                    return Task.CompletedTask;
                });
    }

    /// <summary>
    /// Handler principal acionado pelo SQS.
    /// Implementa "partial batch response": apenas itens realmente falhos entram em <c>BatchItemFailures</c>.
    /// </summary>
    public async Task<SQSBatchResponse> FunctionHandler(SQSEvent evnt, ILambdaContext context)
    {
        List<SQSBatchResponse.BatchItemFailure> failures = new List<SQSBatchResponse.BatchItemFailure>();

        foreach (SQSEvent.SQSMessage? record in evnt.Records)
        {
            try
            {
                bool ok = await this.ProcessRecordAsync(record);

                if (!ok)
                {
                    // Caso não-retryable (exemplo: 400/401/403 etc.), não adicionamos aos failures.
                    // Isso evita "envenenar" a fila com mensagens inválidas permanentemente.
                    this._logger.LogWarning("Mensagem descartada (4xx não-retryable). MessageId={MessageId}", record.MessageId);
                }
            }
            catch (Exception ex)
            {
                // Qualquer exceção aqui é considerada "retryable": colocamos apenas este item em failures.
                this._logger.LogError(ex, "Erro retryable. MessageId={MessageId}", record.MessageId);
                failures.Add(new SQSBatchResponse.BatchItemFailure { ItemIdentifier = record.MessageId });
            }
        }

        return new SQSBatchResponse { BatchItemFailures = failures };
    }

    /// <summary>
    /// Processa UMA mensagem do SQS.
    /// Retorna true se a mensagem é considerada "processada" (não deve reprocessar),
    /// ou false se é não-retryable (descartada). Erros/retries levantam exceção.
    /// </summary>
    private async Task<bool> ProcessRecordAsync(SQSEvent.SQSMessage msg)
    {
        // Checa se é um evento esperado (payment.confirmed); caso contrário, ignora silenciosamente.
        using JsonDocument doc = JsonDocument.Parse(msg.Body);
        if (!doc.RootElement.TryGetProperty("event", out JsonElement e))
        {
            this._logger.LogWarning("Sem 'event'. Ignorando. Body={Body}", msg.Body);
            return true;
        }

        string? evt = e.GetString();
        if (!string.Equals(evt, "payment.confirmed", StringComparison.OrdinalIgnoreCase))
        {
            this._logger.LogInformation("Evento ignorado: {Event}", evt);
            return true;
        }

        // Desserializa o payload do evento (case-insensitive por segurança).
        PaymentConfirmedEvent data = JsonSerializer.Deserialize<PaymentConfirmedEvent>(msg.Body, JsonOpts) ?? throw new InvalidOperationException("Payload inválido (payment.confirmed).");

        // Monta o corpo da requisição para a Game API (grant em lote).
        GrantRequest grant = new()
        {
            PurchaseId = data.PurchaseId,
            UserId = data.UserId,
            Items = data.Items.Select(i => new GrantItem
            {
                GameId = i.GameId,
                Quantity = i.Quantity,
                Price = i.Price
            }).ToList()
        };

        // Correlação: usa atributo do SQS se existir, senão cai para purchaseId.
        string? correlationId = msg.MessageAttributes.TryGetValue("CorrelationId", out SQSEvent.MessageAttribute? attr) && !string.IsNullOrWhiteSpace(attr.StringValue)
                ? attr.StringValue
                : data.PurchaseId;

        // Envia a requisição sob política de retry (Polly).
        using HttpResponseMessage? resp = await this._retryPolicy.ExecuteAsync(ct =>
        {
            // Cria uma nova HttpRequestMessage a cada tentativa de retry.
            // IMPORTANTE: não reutilize HttpRequestMessage entre tentativas, pois o Content pode ser "consumido" e causar erros sutis
            HttpRequestMessage req = new(HttpMethod.Post, this._grantPath)
            {
                // Json camelCase (JsonSerializerDefaults.Web) para compatibilidade com a Game API
                Content = JsonContent.Create(grant, options: JsonWriteOpts)
            };

            // garante safety em retries
            req.Headers.Add("Idempotency-Key", data.PurchaseId);
            // identificação do cliente
            req.Headers.UserAgent.ParseAdd("grant-lambda/1.0");
            // exemplo: X-Correlation-Id
            req.Headers.TryAddWithoutValidation(this._corrHeader, correlationId);

            // Autorização serviço->serviço
            if (!string.IsNullOrWhiteSpace(this._apiKey))
            {
                req.Headers.TryAddWithoutValidation("X-API-Key", this._apiKey);
            }

            return this._http.SendAsync(req, ct);
        }, CancellationToken.None);

        // Mapeamento de respostas da Game API para a semântica do SQS:
        if ((int)resp.StatusCode is >= 200 and < 300)
        {
            this._logger.LogInformation("Grant OK. purchaseId={PurchaseId}", data.PurchaseId);
            return true; // sucesso
        }

        if (resp.StatusCode == HttpStatusCode.Conflict)
        {
            // Já concedido em chamada anterior -> idempotente -> sucesso.
            this._logger.LogInformation("Grant idempotente (409). purchaseId={PurchaseId}", data.PurchaseId);
            return true;
        }

        if (resp.StatusCode == HttpStatusCode.TooManyRequests || (int)resp.StatusCode >= 500)
        {
            // Lança exceção para reprocessar a mensagem (entra em BatchItemFailures)
            string txt = await SafeReadAsync(resp);
            throw new HttpRequestException($"Game API {resp.StatusCode}: {txt}");
        }

        // 4xx não-retryable (exemplo: 400/401/403/404): loga e DESCARTA a mensagem.
        string body = await SafeReadAsync(resp);
        this._logger.LogError("Game API {Status} (não-retryable). purchaseId={PurchaseId}. Body={Body}", resp.StatusCode, data.PurchaseId, body);

        // não-retryable não entra em failures
        return false;
    }

    /// <summary>
    /// Lê o corpo de uma resposta com tolerância a erros.
    /// </summary>
    private static async Task<string> SafeReadAsync(HttpResponseMessage r)
    {
        try
        {
            return await r.Content.ReadAsStringAsync();
        }
        catch
        {
            return "<no-body>";
        }
    }

    /// <summary>
    /// Helper para ler variável de ambiente com validação opcional.
    /// </summary>
    private static string? Env(string name, bool required = false)
    {
        string? v = Environment.GetEnvironmentVariable(name);
        if (required && string.IsNullOrWhiteSpace(v))
        {
            throw new InvalidOperationException($"Env var '{name}' não configurada.");
        }

        return v;
    }
}

#endregion