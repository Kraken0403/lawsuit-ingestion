import { qdrant } from "../qdrant/client.js";
import { embedTexts } from "../embeddings/embed.js";

const COLLECTION_NAME = "lawsuit_cases";

async function main() {
  const query = process.argv.slice(2).join(" ").trim();

  if (!query) {
    throw new Error('Usage: npm run search -- "your query here"');
  }

  const [vector] = await embedTexts([query]);

  const results = await qdrant.search(COLLECTION_NAME, {
    vector,
    limit: 5,
    with_payload: true,
  });

  for (const [idx, result] of results.entries()) {
    console.log("\n" + "=".repeat(80));
    console.log(`Result ${idx + 1} | score=${result.score}`);
    console.log(`id: ${result.id}`);

    const payload = result.payload as Record<string, unknown>;
    console.log(`title: ${payload.title}`);
    console.log(`citation: ${payload.citation}`);
    console.log(`chunkId: ${payload.chunkId}`);
    console.log(`paras: ${payload.paragraphStart} - ${payload.paragraphEnd}`);
    console.log(String(payload.text || "").slice(0, 1500));
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});