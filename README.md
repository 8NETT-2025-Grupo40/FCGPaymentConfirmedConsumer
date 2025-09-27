# FCG – Payment Confirmed Consumer (AWS Lambda)

---

Lambda **.NET 8** acionada por **SQS** que consome eventos `payment.confirmed` da Payment API e chama o **Games API** para conceder jogos. Implementa **Partial Batch Response**, **Polly (retry com backoff + jitter)** e **tracing com AWS X-Ray (HttpClientXRayTracingHandler)**.

## Visão Geral

* **Trigger:** SQS (MessageAttribute `Type = "payment.confirmed"`).
* **Validação:** eventos com outro `Type` são **ignorados** (sucesso).
* **Chamada externa:** `POST {GAME_API_BASE_URL}{GAME_API_LIBRARY_PATH}`
  Headers: `Idempotency-Key: <purchaseId>`, `X-Correlation-Id: <purchaseId>`, `X-API-Key` (se configurado).
* **Erros:**

  * `2xx` → OK.
  * `5xx/timeout` → **retryable** → volta no `BatchItemFailures`.
  * `4xx` → **não-retryable** → descarta e loga.

---

## Handler, runtime e nome da função

* **Handler:** `FCGPaymentConfirmedConsumer::FCGPaymentConfirmedConsumer.Function::FunctionHandler`
* **Runtime:** `dotnet8` • **Arquitetura:** `x86_64`
* **Nome da Lambda:** `FcgPaymentConfirmedConsumer`

---

## CI/CD
* **Build & Test** (.NET 8) em PR e `main`.
* **Deploy** automático para a Lambda em `main` 
* Publica nova **version** e atualiza/cria o **alias `prod`**.