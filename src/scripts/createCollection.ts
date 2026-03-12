import { env } from "../config/env.js";
import { ensureCollection } from "../qdrant/collections.js";

async function main() {
  const collectionName = "lawsuit_cases";
  await ensureCollection(collectionName, env.embedding.dimensions);
  console.log(
    `Collection ensured: ${collectionName} (dim=${env.embedding.dimensions})`
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});