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
  const totalCaseTarget = Number(process.argv[3] || 1000);
  const chunkWordTarget = Number(process.argv[4] || 600);
  const dbBatchSize = Number(process.argv[5] || 100);

  console.log(`Starting hybrid ingestion after file_name=${startAfterId}`);
  console.log(`Total case target=${totalCaseTarget}`);
  console.log(`Chunk target words=${chunkWordTarget}`);
  console.log(`DB batch size=${dbBatchSize}`);

  await ensureHybridCollection(
    env.qdrant.hybridCollection,
    env.embedding.dimensions
  );

  let cursor = startAfterId;
  let totalCases = 0;
  let totalChunks = 0;
  const startedAt = Date.now();

  while (totalCases < totalCaseTarget) {
    const remaining = totalCaseTarget - totalCases;
    const batchLimit = Math.min(dbBatchSize, remaining);

    console.log(
      `\nFetching next DB batch after file_name=${cursor}, limit=${batchLimit}...`
    );

    const rows = await fetchCasesBatch(cursor, batchLimit);

    if (!rows.length) {
      console.log("No more rows returned from DB. Stopping.");
      break;
    }

    console.log(
      `Fetched ${rows.length} rows: ${rows[0].file_name} -> ${
        rows[rows.length - 1].file_name
      }`
    );

    for (const row of rows) {
      cursor = row.file_name;

      if (!row.jtext) {
        console.log(`Skipped case ${row.file_name}: empty jtext`);
        continue;
      }

      const raw: RawCaseRow = {
        fileName: row.file_name,
        ftype: row.ftype,
        flag: row.flag,
        html: row.jtext,
      };

      const parsed = parseCase(raw);
      const chunks = chunkParagraphs(
        parsed.caseId,
        parsed.paragraphs,
        chunkWordTarget,
        1
      );

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

      const elapsedMinutes = (Date.now() - startedAt) / 1000 / 60;
      const casesPerMin = totalCases / Math.max(elapsedMinutes, 0.001);
      const chunksPerMin = totalChunks / Math.max(elapsedMinutes, 0.001);

      console.log(
        `Ingested case=${parsed.caseId} title="${parsed.title}" chunks=${chunks.length} | totalCases=${totalCases}/${totalCaseTarget} totalChunks=${totalChunks} cases/min=${casesPerMin.toFixed(
          2
        )} chunks/min=${chunksPerMin.toFixed(2)}`
      );

      if (totalCases >= totalCaseTarget) break;
    }
  }

  const totalMinutes = (Date.now() - startedAt) / 1000 / 60;

  console.log("\nDone.");
  console.log(`Final cursor=${cursor}`);
  console.log(`cases=${totalCases}`);
  console.log(`chunks=${totalChunks}`);
  console.log(`elapsedMinutes=${totalMinutes.toFixed(2)}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});