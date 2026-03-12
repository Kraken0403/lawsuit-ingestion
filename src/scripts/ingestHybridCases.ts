import { fetchCasesBatch } from "../db/sql.js";
import { parseCase } from "../parser/parseCase.js";
import { chunkParagraphs } from "../parser/chunker.js";
import type { RawCaseRow } from "../parser/types.js";
import { embedTextsInBatches } from "../embeddings/embed.js";
import { ensureHybridCollection } from "../qdrant/hybridCollections.js";
import { buildHybridPoints, upsertHybridPoints } from "../qdrant/hybridUpsert.js";
import { env } from "../config/env.js";

async function main() {
  const startAfterId = Number(process.argv[2] || 100000);
  const caseLimit = Number(process.argv[3] || 10);
  const chunkWordTarget = Number(process.argv[4] || 600);

  console.log(`Starting hybrid ingestion after file_name=${startAfterId}`);
  console.log(`Case limit=${caseLimit}`);
  console.log(`Chunk target words=${chunkWordTarget}`);

  await ensureHybridCollection(
    env.qdrant.hybridCollection,
    env.embedding.dimensions
  );

  const rows = await fetchCasesBatch(startAfterId, caseLimit);

  console.log(`Fetched rows: ${rows.length}`);
  if (rows.length > 0) {
    console.log(
      `Fetched file_name range: ${rows[0].file_name} -> ${rows[rows.length - 1].file_name}`
    );
  }

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

    const denseVectors = await embedTextsInBatches(
      chunks.map((c) => c.text),
      32
    );

    const points = buildHybridPoints(parsed, chunks, denseVectors);
    await upsertHybridPoints(env.qdrant.hybridCollection, points);

    totalCases += 1;
    totalChunks += chunks.length;

    console.log(
      `Hybrid-ingested case=${parsed.caseId} title="${parsed.title}" chunks=${chunks.length}`
    );
  }

  console.log(`Done. cases=${totalCases}, chunks=${totalChunks}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});