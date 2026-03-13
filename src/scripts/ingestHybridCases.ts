import fs from "node:fs";
import path from "node:path";
import { fetchCasesBatchInRange } from "../db/sql.js";
import { parseCase } from "../parser/parseCase.js";
import { chunkParagraphs } from "../parser/chunker.js";
import type { RawCaseRow, Chunk } from "../parser/types.js";
import { embedTextsInBatches } from "../embeddings/embed.js";
import { ensureHybridCollection } from "../qdrant/hybridCollections.js";
import { buildHybridPoints, upsertHybridPoints } from "../qdrant/hybridUpsert.js";
import { env } from "../config/env.js";

function saveProgress(filePath: string, data: Record<string, unknown>) {
  fs.writeFileSync(filePath, JSON.stringify(data, null, 2));
}

async function embedChunksSafely(
  chunks: Chunk[],
  batchSize = 16
): Promise<{
  keptChunks: Chunk[];
  denseVectors: number[][];
  skipped: Array<{ index: number; reason: string }>;
}> {
  const keptChunks: Chunk[] = [];
  const denseVectors: number[][] = [];
  const skipped: Array<{ index: number; reason: string }> = [];

  for (let i = 0; i < chunks.length; i += batchSize) {
    const batchChunks = chunks.slice(i, i + batchSize);
    const batchTexts = batchChunks.map((c) => c.text);

    try {
      console.log(
        `Embedding safe batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(chunks.length / batchSize)} with ${batchChunks.length} chunks`
      );

      const batchVectors = await embedTextsInBatches(batchTexts, batchSize);

      keptChunks.push(...batchChunks);
      denseVectors.push(...batchVectors);
    } catch (err: any) {
      const message = err?.message || "Unknown embedding error";

      const isContextLengthError =
        err?.status === 400 &&
        message.includes("maximum context length");

      if (!isContextLengthError) {
        throw err;
      }

      console.warn(
        `Batch embedding failed due to oversized chunk(s). Falling back to per-chunk embedding for this batch...`
      );

      for (let j = 0; j < batchChunks.length; j++) {
        const chunk = batchChunks[j];

        try {
          const vectors = await embedTextsInBatches([chunk.text], 1);
          keptChunks.push(chunk);
          denseVectors.push(vectors[0]);
        } catch (innerErr: any) {
          const innerMessage = innerErr?.message || "Unknown embedding error";

          if (
            innerErr?.status === 400 &&
            innerMessage.includes("maximum context length")
          ) {
            const originalIndex = i + j;
            console.warn(
              `Skipping oversized chunk at index=${originalIndex} for case=${chunk.caseId}: ${innerMessage}`
            );
            skipped.push({
              index: originalIndex,
              reason: "maximum context length",
            });
            continue;
          }

          throw innerErr;
        }
      }
    }
  }

  return { keptChunks, denseVectors, skipped };
}

async function main() {
  const startAfterId = Number(process.argv[2] || 100000);
  const endId = Number(process.argv[3] || 120000);
  const chunkWordTarget = Number(process.argv[4] || 600);
  const dbBatchSize = Number(process.argv[5] || 100);
  const workerName = process.argv[6] || "worker";
  const progressFile = path.resolve(`./progress-${workerName}.json`);

  if (!Number.isFinite(startAfterId) || !Number.isFinite(endId)) {
    throw new Error("startAfterId and endId must be numbers");
  }

  if (endId <= startAfterId) {
    throw new Error("endId must be greater than startAfterId");
  }

  console.log(`Starting hybrid ingestion in range (${startAfterId}, ${endId}]`);
  console.log(`Chunk target words=${chunkWordTarget}`);
  console.log(`DB batch size=${dbBatchSize}`);
  console.log(`Worker name=${workerName}`);
  console.log(`Progress file=${progressFile}`);

  await ensureHybridCollection(
    env.qdrant.hybridCollection,
    env.embedding.dimensions
  );

  let cursor = startAfterId;
  let totalCases = 0;
  let totalChunks = 0;
  const startedAt = Date.now();

  while (true) {
    console.log(
      `\nFetching next DB batch in range after file_name=${cursor}, endId=${endId}, limit=${dbBatchSize}...`
    );

    const rows = await fetchCasesBatchInRange(cursor, endId, dbBatchSize);

    if (!rows.length) {
      console.log("No more rows returned in this range. Stopping.");
      break;
    }

    console.log(
      `Fetched ${rows.length} rows: ${rows[0].file_name} -> ${rows[rows.length - 1].file_name}`
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

      console.log(
        `Preparing embeddings for case=${parsed.caseId} title="${parsed.title}" chunks=${chunks.length}`
      );

      const { keptChunks, denseVectors, skipped } = await embedChunksSafely(
        chunks,
        16
      );

      if (!keptChunks.length) {
        console.warn(
          `Skipped case=${parsed.caseId} because all chunks failed embedding`
        );
        continue;
      }

      if (skipped.length) {
        console.warn(
          `Case=${parsed.caseId} skipped ${skipped.length} oversized chunks`
        );
      }

      console.log(
        `Finished embeddings for case=${parsed.caseId}, building Qdrant points...`
      );

      const points = buildHybridPoints(parsed, keptChunks, denseVectors);

      const upsertStartedAt = Date.now();
      console.log(
        `Upserting ${points.length} points to Qdrant for case=${parsed.caseId}...`
      );

      await upsertHybridPoints(env.qdrant.hybridCollection, points);

      console.log(
        `Finished Qdrant upsert for case=${parsed.caseId} in ${Date.now() - upsertStartedAt}ms`
      );

      totalCases += 1;
      totalChunks += keptChunks.length;

      const elapsedMinutes = (Date.now() - startedAt) / 1000 / 60;
      const casesPerMin = totalCases / Math.max(elapsedMinutes, 0.001);
      const chunksPerMin = totalChunks / Math.max(elapsedMinutes, 0.001);

      saveProgress(progressFile, {
        workerName,
        startAfterId,
        endId,
        currentCursor: cursor,
        lastCompletedCaseId: parsed.caseId,
        totalCases,
        totalChunks,
        skippedChunksInLastCase: skipped.length,
        updatedAt: new Date().toISOString(),
      });

      console.log(
        `Ingested case=${parsed.caseId} title="${parsed.title}" keptChunks=${keptChunks.length} skippedChunks=${skipped.length} | totalCases=${totalCases} totalChunks=${totalChunks} cases/min=${casesPerMin.toFixed(
          2
        )} chunks/min=${chunksPerMin.toFixed(2)}`
      );
    }

    if (cursor >= endId) {
      console.log(`Cursor reached endId (${endId}). Stopping.`);
      break;
    }
  }

  const totalMinutes = (Date.now() - startedAt) / 1000 / 60;

  console.log("\nDone.");
  console.log(`Range=(${startAfterId}, ${endId}]`);
  console.log(`Final cursor=${cursor}`);
  console.log(`cases=${totalCases}`);
  console.log(`chunks=${totalChunks}`);
  console.log(`elapsedMinutes=${totalMinutes.toFixed(2)}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});