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

async function requestEmbeddings(inputs: string[]): Promise<number[][]> {
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
  });

  if (!response.ok) {
    const text = await response.text();
    const error = new Error(`Embedding failed: ${response.status} ${text}`);
    (error as any).status = response.status;
    throw error;
  }

  const json = (await response.json()) as OpenAIEmbeddingResponse;
  return json.data.map((item) => item.embedding);
}

async function requestEmbeddingsWithRetry(
  inputs: string[],
  maxRetries = 8
): Promise<number[][]> {
  let attempt = 0;

  while (true) {
    try {
      return await requestEmbeddings(inputs);
    } catch (err: any) {
      attempt += 1;
      const status = err?.status;

      if (attempt > maxRetries) {
        throw err;
      }

      // Exponential backoff with jitter
      const baseDelay = status === 429 ? 5000 : 2000;
      const delay =
        Math.min(baseDelay * Math.pow(2, attempt - 1), 60000) +
        Math.floor(Math.random() * 1000);

      console.warn(
        `Embedding request failed (attempt ${attempt}/${maxRetries}, status=${status ?? "unknown"}). Retrying in ${delay}ms...`
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
    const batch = texts.slice(i, i + batchSize);

    console.log(
      `Embedding batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(
        texts.length / batchSize
      )} with ${batch.length} texts`
    );

    const embeddings = await requestEmbeddingsWithRetry(batch);

    allEmbeddings.push(...embeddings);

    // Small throttle between batches to reduce burst pressure
    await sleep(500);
  }

  return allEmbeddings;
}