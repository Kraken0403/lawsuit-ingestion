import { qdrant } from "./client.js";

export async function ensureCollection(
  collectionName: string,
  vectorSize: number
): Promise<void> {
  const collections = await qdrant.getCollections();
  const exists = collections.collections.some(
    (c) => c.name === collectionName
  );

  if (exists) return;

  await qdrant.createCollection(collectionName, {
    vectors: {
      size: vectorSize,
      distance: "Cosine",
    },
  });
}