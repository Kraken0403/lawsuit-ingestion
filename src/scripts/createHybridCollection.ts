import { env } from "../config/env.js";
import { ensureHybridCollection } from "../qdrant/hybridCollections.js";

async function main() {
  await ensureHybridCollection(
    env.qdrant.hybridCollection,
    env.embedding.dimensions
  );

  console.log(
    `Hybrid collection ensured: ${env.qdrant.hybridCollection} (dense=${env.embedding.dimensions}, sparse=bm25)`
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});