import { qdrant } from "../qdrant/client.js";
import { env } from "../config/env.js";
import { embedTexts } from "../embeddings/embed.js";

async function main() {
  const queryText = process.argv.slice(2).join(" ").trim();

  if (!queryText) {
    throw new Error('Usage: npm run hybrid-search -- "your query here"');
  }

  const [denseVector] = await embedTexts([queryText]);

  const results = await qdrant.query(env.qdrant.hybridCollection, {
    prefetch: [
      {
        query: {
          text: queryText,
          model: "Qdrant/bm25",
        },
        using: "sparse",
        limit: 20,
      },
      {
        query: denseVector,
        using: "dense",
        limit: 20,
      },
    ],
    query: {
      fusion: "rrf",
    },
    limit: 10,
    with_payload: true,
  });

  const points = results.points ?? results;

  for (const [idx, result] of points.entries()) {
    const payload = result.payload as Record<string, unknown>;

    console.log("\n" + "=".repeat(80));
    console.log(`Result ${idx + 1} | score=${result.score}`);
    console.log(`id: ${result.id}`);
    console.log(`title: ${payload.title}`);
    console.log(`citation: ${payload.citation}`);
    console.log(`chunkId: ${payload.chunkId}`);
    console.log(`paras: ${payload.paragraphStart} - ${payload.paragraphEnd}`);
    console.log(String(payload.text || "").slice(0, 1800));
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});