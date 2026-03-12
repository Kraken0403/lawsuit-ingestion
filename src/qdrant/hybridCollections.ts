import { qdrant } from "./client.js";

export async function ensureHybridCollection(
  collectionName: string,
  vectorSize: number
): Promise<void> {
  const collections = await qdrant.getCollections();
  const exists = collections.collections.some((c) => c.name === collectionName);

  if (exists) return;

  await qdrant.createCollection(collectionName, {
    vectors: {
      dense: {
        size: vectorSize,
        distance: "Cosine",
        on_disk: true,
      },
    },
    sparse_vectors: {
      sparse: {
        modifier: "idf",
      },
    },
    on_disk_payload: true,
    hnsw_config: {
      m: 32,
      ef_construct: 256,
      full_scan_threshold: 10000,
      max_indexing_threads: 0,
      on_disk: true,
    },
    optimizers_config: {
      memmap_threshold: 20000,
      indexing_threshold: 20000,
    },
    quantization_config: {
      scalar: {
        type: "int8",
        quantile: 0.99,
        always_ram: false,
      },
    },
  });
}