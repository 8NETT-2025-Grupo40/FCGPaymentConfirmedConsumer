using System.Net;
using System.Text.Json;
using Xunit;
using Amazon.Lambda.SQSEvents;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace ExampleCS.Tests;

public class GrantLambdaTests
{
    /// <summary>
    /// Gera um corpo de mensagem "payment.confirmed" válido para a fila SQS.
    /// Mantém o formato camelCase (compatível com JsonSerializerDefaults.Web).
    /// </summary>
    private static string ConfirmedBody(
        string purchaseId = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        string userId = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        string gameId = "G-123",
        int qty = 1,
        decimal price = 49.90m)
    {
        return JsonSerializer.Serialize(new
        {
            // o campo "event" será usado para decidir processar/ignorar
            @event = "payment.confirmed",
            version = "1",
            occurredAt = DateTimeOffset.UtcNow,
            userId,
            purchaseId,
            items = new[] { new { gameId, quantity = qty, price } }
        });
    }

    /// <summary>
    /// Monta um evento SQS com um único registro, podendo incluir CorrelationId.
    /// </summary>
    private static SQSEvent Ev(string body, string id = "mid-1", string? corrId = null)
    {
        var attrs = new Dictionary<string, SQSEvent.MessageAttribute>();
        if (corrId != null)
            attrs["CorrelationId"] = new SQSEvent.MessageAttribute { DataType = "String", StringValue = corrId };

        return new SQSEvent
        {
            Records = new List<SQSEvent.SQSMessage> {
                    new() {
                        Body = body,
                        MessageId = id,
                        MessageAttributes = attrs
                    }
                }
        };
    }

    /// <summary>
    /// Define variáveis de ambiente usadas pela Function (simula config da Lambda).
    /// </summary>
    private static void SetEnv(
        string baseUrl = "http://game.local/",
        string path = "/internal/grants",
        string? apiKey = "test-key",
        string corrHeader = "X-Correlation-Id",
        int timeoutSec = 5,
        int retries = 2)
    {
        Environment.SetEnvironmentVariable("GAME_API_BASE_URL", baseUrl);
        Environment.SetEnvironmentVariable("GAME_API_GRANT_PATH", path);
        Environment.SetEnvironmentVariable("GAME_API_KEY", apiKey);
        Environment.SetEnvironmentVariable("CORRELATION_HEADER", corrHeader);
        Environment.SetEnvironmentVariable("HTTP_TIMEOUT_SECONDS", timeoutSec.ToString());
        Environment.SetEnvironmentVariable("HTTP_RETRY_ATTEMPTS", retries.ToString());
    }

    /// <summary>
    /// Cria a Function injetando nosso HttpMessageHandler de teste e um logger substituto.
    /// </summary>
    private static Function MakeFunction(TestHttpMessageHandler handler, ILogger<Function>? logger = null)
        => new(new HttpClient(handler), logger ?? Substitute.For<ILogger<Function>>());

    [Fact(DisplayName = "[200] envia headers corretos e não retorna failures")]
    public async Task Success_200_SendsHeaders_And_NoFailures()
    {
        // Arrange
        SetEnv();
        var handler = new TestHttpMessageHandler
        {
            OnSendAsync = (req, _) =>
            {
                Assert.Equal(HttpMethod.Post, req.Method);
                Assert.EndsWith("/internal/grants", req.RequestUri!.ToString());

                // Idempotência -> purchaseId
                Assert.True(req.Headers.TryGetValues("Idempotency-Key", out var idem));
                Assert.Contains("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", idem!);

                // Autorização service-to-service (quando configurada)
                Assert.True(req.Headers.TryGetValues("X-API-Key", out var apiKey));
                Assert.Contains("test-key", apiKey!);

                // Correlação
                Assert.True(req.Headers.TryGetValues("X-Correlation-Id", out var corr));
                Assert.Contains("corr-123", corr!);
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK));
            }
        };
        var fn = MakeFunction(handler);

        // Act
        var resp = await fn.FunctionHandler(Ev(ConfirmedBody(), corrId: "corr-123"), Substitute.For<Amazon.Lambda.Core.ILambdaContext>());

        // Assert
        Assert.Empty(resp.BatchItemFailures);
        Assert.NotNull(handler.LastRequest);
    }

    [Fact(DisplayName = "[409] tratado como sucesso idempotente")]
    public async Task Idempotent_409_Treated_As_Success()
    {
        // Arrange
        SetEnv();
        var handler = new TestHttpMessageHandler
        {
            OnSendAsync = (_, __) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.Conflict))
        };
        var fn = MakeFunction(handler);

        // Act
        var resp = await fn.FunctionHandler(Ev(ConfirmedBody()), Substitute.For<Amazon.Lambda.Core.ILambdaContext>());

        // Assert
        Assert.Empty(resp.BatchItemFailures);
    }

    [Fact(DisplayName = "[400] inválido -> não-retryable -> não entra em failures")]
    public async Task BadRequest_400_NotRetryable_NotInFailures()
    {
        // Arrange
        SetEnv();
        var handler = new TestHttpMessageHandler
        {
            OnSendAsync = (_, __) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.BadRequest))
        };
        var fn = MakeFunction(handler);

        // Act
        var resp = await fn.FunctionHandler(Ev(ConfirmedBody()), Substitute.For<Amazon.Lambda.Core.ILambdaContext>());

        // Assert
        Assert.Empty(resp.BatchItemFailures);
    }

    [Fact(DisplayName = "[401] não autorizado -> não-retryable -> não entra em failures")]
    public async Task Unauthorized_401_NotRetryable_NotInFailures()
    {
        // Arrange
        SetEnv();
        var handler = new TestHttpMessageHandler
        {
            OnSendAsync = (_, __) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.Unauthorized))
        };
        var fn = MakeFunction(handler);

        // Act
        var resp = await fn.FunctionHandler(Ev(ConfirmedBody()), Substitute.For<Amazon.Lambda.Core.ILambdaContext>());

        // Assert
        Assert.Empty(resp.BatchItemFailures);
    }

    [Fact(DisplayName = "Evento diferente de 'payment.confirmed' é ignorado (sem HTTP)")]
    public async Task NonPaymentEvent_Ignored_NoHttpCall()
    {
        // Arrange
        SetEnv();
        var calls = 0;
        var handler = new TestHttpMessageHandler
        {
            OnSendAsync = (_, __) => { calls++; return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)); }
        };
        var body = JsonSerializer.Serialize(new { @event = "other.event" });
        var fn = MakeFunction(handler);

        // Act
        var resp = await fn.FunctionHandler(Ev(body), Substitute.For<Amazon.Lambda.Core.ILambdaContext>());

        // Assert
        Assert.Empty(resp.BatchItemFailures);
        Assert.Equal(0, calls);
    }

    [Fact(DisplayName = "Ausência do campo 'event' -> ignorado (sem HTTP)")]
    public async Task Event_Null_Is_Ignored_NoHttpCall()
    {
        // Arrange
        SetEnv();
        var calls = 0;
        var handler = new TestHttpMessageHandler
        {
            OnSendAsync = (_, __) => { calls++; return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)); }
        };
        var body = JsonSerializer.Serialize(new { @event = (string?)null });
        var fn = MakeFunction(handler);

        // Act
        var resp = await fn.FunctionHandler(Ev(body), Substitute.For<Amazon.Lambda.Core.ILambdaContext>());

        // Assert
        Assert.Empty(resp.BatchItemFailures);
        Assert.Equal(0, calls);
    }

    [Fact(DisplayName = "Sem CorrelationId no SQS -> usa purchaseId como fallback")]
    public async Task MissingCorrelation_FallsBack_To_PurchaseId()
    {
        // Arrange
        SetEnv();
        string? sent = null;
        var handler = new TestHttpMessageHandler
        {
            OnSendAsync = (req, __) =>
            {
                req.Headers.TryGetValues("X-Correlation-Id", out var vals);
                sent = vals!.FirstOrDefault();
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK));
            }
        };
        var fn = MakeFunction(handler);

        // Act
        await fn.FunctionHandler(Ev(ConfirmedBody(), corrId: null), Substitute.For<Amazon.Lambda.Core.ILambdaContext>());

        // Assert
        Assert.Equal("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", sent);
    }

    [Fact(DisplayName = "Sem API key + header de correlação customizado")]
    public async Task CustomHeaders_ApiKeyOptional_And_CustomCorrelationHeader()
    {
        // Arrange
        SetEnv(apiKey: null, corrHeader: "X-Trace-Id");
        string? trace = null;

        var handler = new TestHttpMessageHandler
        {
            OnSendAsync = (req, __) =>
            {
                // não deve enviar X-API-Key quando não configurada
                Assert.False(req.Headers.Contains("X-API-Key"));

                // deve enviar o header customizado de correlação
                Assert.True(req.Headers.TryGetValues("X-Trace-Id", out var vals));
                trace = vals!.FirstOrDefault();

                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK));
            }
        };
        var fn = MakeFunction(handler);

        // Act
        await fn.FunctionHandler(Ev(ConfirmedBody()), Substitute.For<Amazon.Lambda.Core.ILambdaContext>());

        // Assert
        Assert.Equal("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", trace);
    }

    [Fact(DisplayName = "BaseUrl sem barra + caminho custom -> RequestUri correta")]
    public async Task Uses_CustomPath_And_BaseUrl_NoTrailingSlash()
    {
        // Arrange
        SetEnv(baseUrl: "http://game.local", path: "grants/apply");
        var handler = new TestHttpMessageHandler
        {
            OnSendAsync = (req, __) =>
            {
                // BaseAddress "http://game.local/" + relative "grants/apply" => http://game.local/grants/apply
                Assert.Equal("http://game.local/grants/apply", req.RequestUri!.ToString());
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK));
            }
        };
        var fn = MakeFunction(handler);

        // Act
        await fn.FunctionHandler(Ev(ConfirmedBody()), Substitute.For<Amazon.Lambda.Core.ILambdaContext>());
    }

    [Fact(DisplayName = "User-Agent enviado")]
    public async Task Sends_UserAgent_Header()
    {
        // Arrange
        SetEnv();
        string? ua = null;
        var handler = new TestHttpMessageHandler
        {
            OnSendAsync = (req, __) =>
            {
                ua = string.Join(" ", req.Headers.UserAgent.Select(p => p.ToString()));
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK));
            }
        };
        var fn = MakeFunction(handler);

        // Act
        await fn.FunctionHandler(Ev(ConfirmedBody()), Substitute.For<Amazon.Lambda.Core.ILambdaContext>());

        // Assert
        Assert.Contains("grant-lambda/1.0", ua);
    }

    // =============================================================================================
    // Corpo JSON enviado (shape camelCase)
    // =============================================================================================

    [Fact(DisplayName = "Corpo JSON enviado à Game API (shape camelCase correto)")]
    public async Task Body_JSON_Shape_Is_Correct()
    {
        // Arrange
        SetEnv();
        string? sentBody = null;
        var handler = new TestHttpMessageHandler
        {
            OnSendAsync = async (req, __) =>
            {
                sentBody = await req.Content!.ReadAsStringAsync();
                return new HttpResponseMessage(HttpStatusCode.OK);
            }
        };
        var fn = MakeFunction(handler);

        // Act
        await fn.FunctionHandler(
            Ev(ConfirmedBody(purchaseId: "P-1", userId: "U-1", gameId: "G-9", qty: 2, price: 10m)),
            Substitute.For<Amazon.Lambda.Core.ILambdaContext>());

        // Assert
        using var doc = JsonDocument.Parse(sentBody!);
        var root = doc.RootElement;
        Assert.Equal("P-1", root.GetProperty("purchaseId").GetString());
        Assert.Equal("U-1", root.GetProperty("userId").GetString());
        var item0 = root.GetProperty("items")[0];
        Assert.Equal("G-9", item0.GetProperty("gameId").GetString());
        Assert.Equal(2, item0.GetProperty("quantity").GetInt32());
        Assert.Equal(10m, item0.GetProperty("price").GetDecimal());
    }

    [Fact(DisplayName = "[500] falha nas 2 primeiras tentativas e sucesso na terceira -> sem failures")]
    public async Task Retry_then_success_for_500_ResultsIn_NoFailures()
    {
        // Arrange
        SetEnv(retries: 2); // 2 retries => até 3 tentativas no total
        var attempts = 0;
        var handler = new TestHttpMessageHandler
        {
            OnSendAsync = (_, __) => Task.FromResult(
                new HttpResponseMessage(++attempts <= 2 ? HttpStatusCode.InternalServerError : HttpStatusCode.OK))
        };
        var fn = MakeFunction(handler);

        // Act
        var resp = await fn.FunctionHandler(Ev(ConfirmedBody()), Substitute.For<Amazon.Lambda.Core.ILambdaContext>());

        // Assert
        Assert.Equal(3, attempts); // 2 falhas + 1 sucesso
        Assert.Empty(resp.BatchItemFailures);
    }

    [Fact(DisplayName = "[503] retries esgotados -> entra em BatchItemFailures")]
    public async Task Retry_exhausted_for_503_AddsItemToFailures()
    {
        // Arrange
        SetEnv(retries: 2);
        var attempts = 0;
        var handler = new TestHttpMessageHandler
        {
            OnSendAsync = (_, __) => Task.FromResult(
                new HttpResponseMessage(++attempts <= 3 ? HttpStatusCode.ServiceUnavailable : HttpStatusCode.OK))
        };
        var fn = MakeFunction(handler);

        // Act
        var resp = await fn.FunctionHandler(Ev(ConfirmedBody(), id: "X-1"), Substitute.For<Amazon.Lambda.Core.ILambdaContext>());

        // Assert
        Assert.True(attempts >= 3); // garantiu que tentou todas as vezes
        Assert.Single(resp.BatchItemFailures);
        Assert.Equal("X-1", resp.BatchItemFailures[0].ItemIdentifier);
    }

    [Fact(DisplayName = "[429] 1ª tentativa falha e 2ª sucesso -> sem failures")]
    public async Task Retry_then_success_for_429_NoFailures()
    {
        // Arrange
        SetEnv(retries: 2);
        var attempts = 0;
        var handler = new TestHttpMessageHandler
        {
            OnSendAsync = (_, __) => Task.FromResult(
                new HttpResponseMessage(++attempts == 1 ? HttpStatusCode.TooManyRequests : HttpStatusCode.OK))
        };
        var fn = MakeFunction(handler);

        // Act
        var resp = await fn.FunctionHandler(Ev(ConfirmedBody()), Substitute.For<Amazon.Lambda.Core.ILambdaContext>());

        // Assert
        Assert.Equal(2, attempts);
        Assert.Empty(resp.BatchItemFailures);
    }

    [Fact(DisplayName = "Timeout na 1ª tentativa e sucesso na 2ª -> sem failures")]
    public async Task Timeout_then_success_NoFailures()
    {
        // Arrange
        SetEnv(retries: 2);
        var attempts = 0;
        var handler = new TestHttpMessageHandler
        {
            OnSendAsync = (_, __) =>
            {
                attempts++;
                if (attempts == 1) throw new TaskCanceledException("timeout simulado");
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK));
            }
        };
        var fn = MakeFunction(handler);

        // Act
        var resp = await fn.FunctionHandler(Ev(ConfirmedBody()), Substitute.For<Amazon.Lambda.Core.ILambdaContext>());

        // Assert
        Assert.Equal(2, attempts);
        Assert.Empty(resp.BatchItemFailures);
    }

    [Theory(DisplayName = "[202/204] são tratados como sucesso")]
    [InlineData(204)]
    [InlineData(202)]
    public async Task NoContent_or_Accepted_TreatedAsSuccess(int status)
    {
        // Arrange
        SetEnv();
        var handler = new TestHttpMessageHandler
        {
            OnSendAsync = (_, __) => Task.FromResult(new HttpResponseMessage((HttpStatusCode)status))
        };
        var fn = MakeFunction(handler);

        // Act
        var resp = await fn.FunctionHandler(Ev(ConfirmedBody()), Substitute.For<Amazon.Lambda.Core.ILambdaContext>());

        // Assert
        Assert.Empty(resp.BatchItemFailures);
    }

    [Fact(DisplayName = "Partial batch: OK + 400 (não-retryable) + 503 (retryable) => só 503 entra em failures")]
    public async Task Partial_batch_OnlyRetryablesAreReturned()
    {
        // Arrange
        SetEnv(retries: 1); // mantém rápido
        var handler = new TestHttpMessageHandler
        {
            OnSendAsync = (req, __) =>
            {
                var body = req.Content!.ReadAsStringAsync().Result;

                // OK
                if (body.Contains("G-OK")) return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK));

                // Não-retryable (descarta, não deve ir para failures)
                if (body.Contains("G-NR")) return Task.FromResult(new HttpResponseMessage(HttpStatusCode.BadRequest));

                // Retryable
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable));
            }
        };

        var fn = MakeFunction(handler);

        // 3 mensagens no mesmo batch: uma OK, uma 4xx (não-retryable), uma 5xx (retryable)
        string ok = ConfirmedBody(gameId: "G-OK"),
               nr = ConfirmedBody(gameId: "G-NR"),
               re = ConfirmedBody(gameId: "G-RETRY");

        var batch = new SQSEvent
        {
            Records = new()
                {
                    new() { Body = ok, MessageId = "ok",    MessageAttributes = [] },
                    new() { Body = nr, MessageId = "bad",   MessageAttributes = [] },
                    new() { Body = re, MessageId = "retry", MessageAttributes = [] }
                }
        };

        // Act
        var resp = await fn.FunctionHandler(batch, Substitute.For<Amazon.Lambda.Core.ILambdaContext>());

        // Assert
        Assert.Single(resp.BatchItemFailures);                  // apenas 1 falhou de fato
        Assert.Equal("retry", resp.BatchItemFailures[0].ItemIdentifier); // o 5xx
    }

    [Fact(DisplayName = "JSON inválido -> adiciona a failures (para DLQ)")]
    public async Task Invalid_JSON_Adds_To_Failures()
    {
        // Arrange
        SetEnv();
        var handler = new TestHttpMessageHandler(); // não será chamado, a falha ocorre antes
        var fn = MakeFunction(handler);

        // Act
        var resp = await fn.FunctionHandler(Ev("{{not-json}}", id: "bad-1"), Substitute.For<Amazon.Lambda.Core.ILambdaContext>());

        // Assert
        Assert.Single(resp.BatchItemFailures);
        Assert.Equal("bad-1", resp.BatchItemFailures[0].ItemIdentifier);
    }
}

internal sealed class TestHttpMessageHandler : HttpMessageHandler
{
    /// <summary>
    /// Delegate que o teste define para controlar o comportamento do SendAsync.
    /// </summary>
    public Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>>? OnSendAsync { get; set; }

    /// <summary>
    /// Guarda a última requisição enviada (útil para asserts adicionais).
    /// </summary>
    public HttpRequestMessage? LastRequest { get; private set; }

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        LastRequest = request;
        if (OnSendAsync is not null) return await OnSendAsync(request, cancellationToken);
        return new HttpResponseMessage(HttpStatusCode.OK);
    }
}