import { env } from "../config/env.js";

type OpenAIEmbeddingResponse = {
  data: Array<{
    index: number;
    embedding: number[];
  }>;
};

export async function embedTexts(texts: string[]): Promise<number[][]> {
  if (!texts.length) return [];

  const response = await fetch(`${env.embedding.baseUrl}/embeddings`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${env.embedding.apiKey}`,
    },
    body: JSON.stringify({
      model: env.embedding.model,
      input: texts,
    }),
  });

  if (!response.ok) {
    const errText = await response.text();
    throw new Error(`Embedding API failed: ${response.status} ${errText}`);
  }

  const json = (await response.json()) as OpenAIEmbeddingResponse;

  const ordered = json.data.sort((a, b) => a.index - b.index);
  return ordered.map((item) => item.embedding);
}

export async function embedTextsInBatches(
  texts: string[],
  batchSize = 32
): Promise<number[][]> {
  const all: number[][] = [];

  for (let i = 0; i < texts.length; i += batchSize) {
    const batch = texts.slice(i, i + batchSize);
    const vectors = await embedTexts(batch);
    all.push(...vectors);
  }

  return all;
}