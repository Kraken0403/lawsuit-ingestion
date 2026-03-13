import { env } from "../config/env.js";

type OpenAIEmbeddingResponse = {
  data: Array<{
    index: number;
    embedding: number[];
  }>;
};

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function requestEmbeddings(
  inputs: string[],
  timeoutMs = 90000
): Promise<number[][]> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  const startedAt = Date.now();

  try {
    const response = await fetch(`${env.embedding.baseUrl}/embeddings`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${env.embedding.apiKey}`,
      },
      body: JSON.stringify({
        model: env.embedding.model,
        input: inputs,
      }),
      signal: controller.signal,
    });

    const tookMs = Date.now() - startedAt;

    if (!response.ok) {
      const text = await response.text();
      const error = new Error(
        `Embedding failed: ${response.status} ${text} (took ${tookMs}ms)`
      );
      (error as any).status = response.status;
      throw error;
    }

    const json = (await response.json()) as OpenAIEmbeddingResponse;
    return json.data.map((item) => item.embedding);
  } catch (err: any) {
    if (err?.name === "AbortError") {
      const error = new Error(
        `Embedding request timed out after ${timeoutMs}ms`
      );
      (error as any).status = 408;
      (error as any).code = "EMBED_TIMEOUT";
      throw error;
    }

    throw err;
  } finally {
    clearTimeout(timeout);
  }
}

async function requestEmbeddingsWithRetry(
  inputs: string[],
  maxRetries = 8
): Promise<number[][]> {
  let attempt = 0;

  while (true) {
    try {
      return await requestEmbeddings(inputs, 90000);
    } catch (err: any) {
      attempt += 1;
      const status = err?.status;
      const code = err?.code;
      const message = err?.message;

      if (attempt > maxRetries) {
        console.error(
          `Embedding permanently failed after ${maxRetries} retries. status=${status ?? "unknown"} code=${code ?? "unknown"} message=${message ?? "unknown"}`
        );
        throw err;
      }

      const retryable =
        status === 408 ||
        status === 429 ||
        status >= 500 ||
        code === "EMBED_TIMEOUT" ||
        code === "ECONNRESET" ||
        code === "ETIMEDOUT" ||
        code === "UND_ERR_CONNECT_TIMEOUT" ||
        code === "UND_ERR_HEADERS_TIMEOUT" ||
        code === "UND_ERR_BODY_TIMEOUT";

      if (!retryable) {
        throw err;
      }

      const baseDelay = status === 429 ? 5000 : 2000;
      const delay =
        Math.min(baseDelay * Math.pow(2, attempt - 1), 60000) +
        Math.floor(Math.random() * 1000);

      console.warn(
        `Embedding request failed (attempt ${attempt}/${maxRetries}, status=${status ?? "unknown"}, code=${code ?? "unknown"}). Retrying in ${delay}ms... message=${message ?? "unknown"}`
      );

      await sleep(delay);
    }
  }
}

export async function embedTextsInBatches(
  texts: string[],
  batchSize = 16
): Promise<number[][]> {
  const allEmbeddings: number[][] = [];

  for (let i = 0; i < texts.length; i += batchSize) {
    const batchNumber = Math.floor(i / batchSize) + 1;
    const totalBatches = Math.ceil(texts.length / batchSize);
    const batch = texts.slice(i, i + batchSize);

    console.log(
      `Embedding batch ${batchNumber}/${totalBatches} with ${batch.length} texts`
    );

    const startedAt = Date.now();
    const embeddings = await requestEmbeddingsWithRetry(batch);
    const tookMs = Date.now() - startedAt;

    console.log(
      `Finished embedding batch ${batchNumber}/${totalBatches} with ${batch.length} texts in ${tookMs}ms`
    );

    allEmbeddings.push(...embeddings);

    await sleep(500);
  }

  return allEmbeddings;
}