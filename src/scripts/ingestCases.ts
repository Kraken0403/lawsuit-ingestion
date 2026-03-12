import { fetchCasesBatch } from "../db/sql.js";
import { parseCase } from "../parser/parseCase.js";
import { chunkParagraphs } from "../parser/chunker.js";
import type { RawCaseRow } from "../parser/types.js";
import { embedTextsInBatches } from "../embeddings/embed.js";
import { ensureCollection } from "../qdrant/collections.js";
import { buildChunkPoints, upsertChunkPoints } from "../qdrant/upsert.js";
import { env } from "../config/env.js";

const COLLECTION_NAME = "lawsuit_cases";

async function main() {
  const startAfterId = Number(process.argv[2] || 0);
  const caseLimit = Number(process.argv[3] || 10);
  const chunkWordTarget = Number(process.argv[4] || 600);

  await ensureCollection(COLLECTION_NAME, env.embedding.dimensions);

  const rows = await fetchCasesBatch(startAfterId, caseLimit);

  console.log(`Fetched rows: ${rows.length}`);

  let totalCases = 0;
  let totalChunks = 0;

  for (const row of rows) {
    if (!row.jtext) continue;

    const raw: RawCaseRow = {
      fileName: row.file_name,
      ftype: row.ftype,
      flag: row.flag,
      html: row.jtext,
    };

    const parsed = parseCase(raw);
    const chunks = chunkParagraphs(parsed.caseId, parsed.paragraphs, chunkWordTarget, 1);

    if (!chunks.length) {
      console.log(`Skipped case ${parsed.caseId}: no chunks`);
      continue;
    }

    const texts = chunks.map((c) => c.text);
    const vectors = await embedTextsInBatches(texts, 32);
    const points = buildChunkPoints(parsed, chunks, vectors);

    await upsertChunkPoints(COLLECTION_NAME, points);

    totalCases += 1;
    totalChunks += chunks.length;

    console.log(
      `Ingested case=${parsed.caseId} title="${parsed.title}" chunks=${chunks.length}`
    );
  }

  console.log(`Done. cases=${totalCases}, chunks=${totalChunks}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});