import { env } from "../config/env.js";

function normalizeQdrantUrl(url: string): string {
  return url.replace(/\/+$/, "");
}

async function qdrantRequest<T>(
  path: string,
  init?: RequestInit
): Promise<T> {
  const baseUrl = normalizeQdrantUrl(env.qdrant.url);

  const response = await fetch(`${baseUrl}${path}`, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      ...(env.qdrant.apiKey ? { "api-key": env.qdrant.apiKey } : {}),
      ...(init?.headers ?? {}),
    },
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(
      `Qdrant request failed: ${response.status} ${response.statusText} :: ${text}`
    );
  }

  if (response.status === 204) {
    return {} as T;
  }

  return (await response.json()) as T;
}

async function main() {
  const collectionName = process.argv[2] || env.qdrant.hybridCollection;

  console.log(`Resetting courtId index on collection=${collectionName}`);

  try {
    await qdrantRequest(
      `/collections/${collectionName}/index/courtId?wait=true`,
      { method: "DELETE" }
    );
    console.log("Deleted old courtId index");
  } catch (error: any) {
    console.log("Delete skipped:", String(error?.message || error));
  }

  await qdrantRequest(
    `/collections/${collectionName}/index?wait=true`,
    {
      method: "PUT",
      body: JSON.stringify({
        field_name: "courtId",
        field_schema: "integer",
      }),
    }
  );

  console.log("Created integer courtId index");
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});