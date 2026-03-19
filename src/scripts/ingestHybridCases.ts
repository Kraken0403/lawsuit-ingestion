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

type ProgressState = {
  workerName: string;
  startAfterId: number;
  endId: number;
  currentCursor: number;
  lastCompletedCaseId: number | null;
  totalCases: number;
  totalChunks: number;
  skippedChunksInLastCase: number;
  updatedAt: string;
};

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

function saveProgressAtomic(filePath: string, data: ProgressState) {
  const tempPath = `${filePath}.${process.pid}.tmp`;
  fs.writeFileSync(tempPath, JSON.stringify(data, null, 2));
  fs.renameSync(tempPath, filePath);
}

function loadProgress(filePath: string): ProgressState | null {
  if (!fs.existsSync(filePath)) return null;

  try {
    const parsed = JSON.parse(fs.readFileSync(filePath, "utf8"));

    if (!parsed || typeof parsed !== "object") {
      return null;
    }

    return parsed as ProgressState;
  } catch (err) {
    console.warn(`Could not read progress file ${filePath}:`, err);
    return null;
  }
}

function isValidProgressForRun(
  progress: ProgressState | null,
  workerName: string,
  startAfterId: number,
  endId: number
): progress is ProgressState {
  return Boolean(
    progress &&
      progress.workerName === workerName &&
      isFiniteNumber(progress.startAfterId) &&
      progress.startAfterId === startAfterId &&
      isFiniteNumber(progress.endId) &&
      progress.endId === endId &&
      isFiniteNumber(progress.currentCursor) &&
      progress.currentCursor >= startAfterId &&
      progress.currentCursor < endId &&
      isFiniteNumber(progress.totalCases) &&
      progress.totalCases >= 0 &&
      isFiniteNumber(progress.totalChunks) &&
      progress.totalChunks >= 0
  );
}

async function fetchCasesBatchWithRetry(
  cursor: number,
  endId: number,
  dbBatchSize: number,
  maxRetries = 8
) {
  let attempt = 0;

  while (true) {
    try {
      return await fetchCasesBatchInRange(cursor, endId, dbBatchSize);
    } catch (err: any) {
      attempt += 1;
      const message = err?.message || "Unknown DB error";
      const code = err?.code || "UNKNOWN";

      if (attempt > maxRetries) {
        console.error(
          `DB fetch permanently failed after ${maxRetries} retries. code=${code} message=${message}`
        );
        throw err;
      }

      const retryable =
        code === "ETIMEOUT" ||
        message.includes("TimeoutError") ||
        message.includes("Failed to connect") ||
        message.includes("timed out");

      if (!retryable) {
        throw err;
      }

      const delay = Math.min(2000 * Math.pow(2, attempt - 1), 30000);

      console.warn(
        `DB fetch failed (attempt ${attempt}/${maxRetries}, code=${code}). Retrying in ${delay}ms... message=${message}`
      );

      await sleep(delay);
    }
  }
}

async function embedChunksSafely(
  chunks: Chunk[],
  batchSize = 24
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

      await sleep(300);
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

          await sleep(150);
        } catch (innerErr: any) {
          const innerMessage =
            innerErr?.message || "Unknown embedding error";

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

  if (!Number.isFinite(chunkWordTarget) || chunkWordTarget <= 0) {
    throw new Error("chunkWordTarget must be a positive number");
  }

  if (!Number.isFinite(dbBatchSize) || dbBatchSize <= 0) {
    throw new Error("dbBatchSize must be a positive number");
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

  const savedProgress = loadProgress(progressFile);

  let cursor = startAfterId;
  let totalCases = 0;
  let totalChunks = 0;

  if (isValidProgressForRun(savedProgress, workerName, startAfterId, endId)) {
    cursor = savedProgress.currentCursor;
    totalCases = savedProgress.totalCases;
    totalChunks = savedProgress.totalChunks;

    console.log(
      `Resuming from saved cursor=${cursor} using progress file ${progressFile}`
    );
  } else if (savedProgress) {
    console.warn(
      `Ignoring stale or mismatched progress file ${progressFile}. Starting fresh for this exact worker/range.`
    );
  }

  let shouldStop = false;

  const requestStop = (signal: string) => {
    if (!shouldStop) {
      shouldStop = true;
      console.warn(`Received ${signal}. Will stop after the current case finishes.`);
    }
  };

  process.on("SIGINT", () => requestStop("SIGINT"));
  process.on("SIGTERM", () => requestStop("SIGTERM"));

  const startedAt = Date.now();
  let sessionCases = 0;
  let sessionChunks = 0;

  let lastBatchSignature: string | null = null;
  let repeatedBatchCount = 0;

  while (cursor < endId && !shouldStop) {
    const fetchCursor = cursor;

    console.log(
      `\nFetching next DB batch in range after file_name=${fetchCursor}, endId=${endId}, limit=${dbBatchSize}...`
    );

    const rows = await fetchCasesBatchWithRetry(fetchCursor, endId, dbBatchSize);

    if (!rows.length) {
      console.log("No more rows returned in this range. Stopping.");
      break;
    }

    const firstFileName = Number(rows[0].file_name);
    const lastFileName = Number(rows[rows.length - 1].file_name);

    if (!Number.isFinite(firstFileName) || !Number.isFinite(lastFileName)) {
      throw new Error("DB returned non-numeric file_name values");
    }

    if (firstFileName <= fetchCursor) {
      throw new Error(
        `Non-forward batch detected: first row ${firstFileName} <= cursor ${fetchCursor}. Refusing to loop.`
      );
    }

    const batchSignature = `${firstFileName}:${lastFileName}:${rows.length}`;
    if (batchSignature === lastBatchSignature) {
      repeatedBatchCount += 1;

      if (repeatedBatchCount >= 2) {
        throw new Error(
          `Same DB batch repeated multiple times (${batchSignature}). Refusing to continue.`
        );
      }
    } else {
      lastBatchSignature = batchSignature;
      repeatedBatchCount = 0;
    }

    console.log(
      `Fetched ${rows.length} rows: ${firstFileName} -> ${lastFileName}`
    );

    let progressedThisBatch = false;

    for (const row of rows) {
      if (shouldStop) break;

      const rowFileName = Number(row.file_name);

      if (!Number.isFinite(rowFileName)) {
        console.warn(`Skipping row with invalid file_name: ${row.file_name}`);
        continue;
      }

      if (rowFileName <= cursor) {
        console.warn(
          `Skipping non-forward row file_name=${rowFileName} at cursor=${cursor}`
        );
        continue;
      }

      progressedThisBatch = true;

      let skippedChunksInLastCase = 0;
      let parsedCaseIdForProgress: number | null = rowFileName;

      if (!row.jtext) {
        cursor = rowFileName;

        saveProgressAtomic(progressFile, {
          workerName,
          startAfterId,
          endId,
          currentCursor: cursor,
          lastCompletedCaseId: parsedCaseIdForProgress,
          totalCases,
          totalChunks,
          skippedChunksInLastCase,
          updatedAt: new Date().toISOString(),
        });

        console.log(`Skipped case ${rowFileName}: empty jtext`);
        continue;
      }

      const raw: RawCaseRow = {
        fileName: rowFileName,
        ftype: row.ftype,
        flag: row.flag,
        html: row.jtext,
      };

      const parsed = parseCase(raw);
      parsedCaseIdForProgress = parsed.caseId;

      const chunks = chunkParagraphs(
        parsed.caseId,
        parsed.paragraphs,
        chunkWordTarget,
        1
      );

      if (!chunks.length) {
        cursor = rowFileName;

        saveProgressAtomic(progressFile, {
          workerName,
          startAfterId,
          endId,
          currentCursor: cursor,
          lastCompletedCaseId: parsedCaseIdForProgress,
          totalCases,
          totalChunks,
          skippedChunksInLastCase,
          updatedAt: new Date().toISOString(),
        });

        console.log(`Skipped case ${parsed.caseId}: no chunks`);
        continue;
      }

      if (chunks.length > 300) {
        console.warn(
          `Large case detected: case=${parsed.caseId} title="${parsed.title}" chunks=${chunks.length}`
        );
      }

      console.log(
        `Preparing embeddings for case=${parsed.caseId} title="${parsed.title}" chunks=${chunks.length}`
      );

      const adaptiveBatchSize =
        chunks.length > 500 ? 8 :
        chunks.length > 200 ? 12 :
        24;

      console.log(
        `Using adaptive embedding batch size=${adaptiveBatchSize} for case=${parsed.caseId}`
      );

      const { keptChunks, denseVectors, skipped } = await embedChunksSafely(
        chunks,
        adaptiveBatchSize
      );

      skippedChunksInLastCase = skipped.length;

      if (!keptChunks.length) {
        cursor = rowFileName;

        saveProgressAtomic(progressFile, {
          workerName,
          startAfterId,
          endId,
          currentCursor: cursor,
          lastCompletedCaseId: parsedCaseIdForProgress,
          totalCases,
          totalChunks,
          skippedChunksInLastCase,
          updatedAt: new Date().toISOString(),
        });

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

      cursor = rowFileName;
      totalCases += 1;
      totalChunks += keptChunks.length;
      sessionCases += 1;
      sessionChunks += keptChunks.length;

      saveProgressAtomic(progressFile, {
        workerName,
        startAfterId,
        endId,
        currentCursor: cursor,
        lastCompletedCaseId: parsed.caseId,
        totalCases,
        totalChunks,
        skippedChunksInLastCase,
        updatedAt: new Date().toISOString(),
      });

      const elapsedMinutes = (Date.now() - startedAt) / 1000 / 60;
      const sessionCasesPerMin = sessionCases / Math.max(elapsedMinutes, 0.001);
      const sessionChunksPerMin = sessionChunks / Math.max(elapsedMinutes, 0.001);

      console.log(
        `Ingested case=${parsed.caseId} title="${parsed.title}" keptChunks=${keptChunks.length} skippedChunks=${skipped.length} | totalCases=${totalCases} totalChunks=${totalChunks} sessionCases/min=${sessionCasesPerMin.toFixed(
          2
        )} sessionChunks/min=${sessionChunksPerMin.toFixed(2)}`
      );
    }

    if (!progressedThisBatch) {
      throw new Error(
        `Fetched ${rows.length} rows but made no forward progress from cursor=${fetchCursor}. Refusing to continue.`
      );
    }

    if (cursor >= endId) {
      console.log(`Cursor reached endId (${endId}). Stopping.`);
      break;
    }
  }

  const totalMinutes = (Date.now() - startedAt) / 1000 / 60;

  if (shouldStop) {
    console.log("\nStopped cleanly after signal.");
  } else {
    console.log("\nDone.");
  }

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