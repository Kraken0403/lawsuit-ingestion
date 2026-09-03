import fs from "node:fs";
import path from "node:path";
import { env } from "../config/env.js";
import { getPool, fetchOneCase } from "../db/sql.js";
import { embedTextsInBatches } from "../embeddings/embed.js";
import { chunkParagraphs } from "../parser/chunker.js";
import { parseCase } from "../parser/parseCase.js";
import type { RawCaseRow } from "../parser/types.js";
import type { HybridPoint } from "../qdrant/hybridUpsert.js";

type CliOptions = {
  apply: boolean;
  collectionName: string;
  chunkWordTarget: number;
  embedBatchSize: number;
  limit: number | null;
  logDir: string;
};

type AuditRow = {
  file_name: number | string | null;
};

type QdrantOffset = number | string | Record<string, unknown> | null;

type PointIdScrollResponse = {
  result?: {
    points?: Array<{ id?: number | string }>;
    next_page_offset?: QdrantOffset;
  };
};

type ExistingCaseState = {
  count: number;
  courtId: number | null;
  decisionDate: string | null;
  decisionYear: number | null;
};

type RunCounters = {
  pendingCases: number;
  successfulCases: number;
  failedCases: number;
  dryRunCases: number;
  oldPoints: number;
  newPoints: number;
  acknowledgedAuditRows: number;
};

const AUDIT_TABLE = "[unilexDB].[dbo].[AuditlogForWebsite]";

const VALID_COURT_IDS = new Set<number>([
  1, 2, 3, 4, 5, 6, 7, 8, 9,
  10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
  24, 25, 26, 27, 28, 29, 30, 40,
  82, 83, 84, 91, 97, 98, 104,
]);

const MONTHS: Record<string, number> = {
  january: 1,
  february: 2,
  march: 3,
  april: 4,
  may: 5,
  june: 6,
  july: 7,
  august: 8,
  september: 9,
  october: 10,
  november: 11,
  december: 12,
};

function usage(): never {
  console.log(`
Usage:
  npm run sync-updated-hybrid -- [options]

Options:
  --dry-run                 Default. Read SQL and Qdrant, parse cases, but make no changes.
  --apply                   Replace Qdrant case chunks and then set isexportAI=1.
  --collection=<name>       Default: QDRANT_HYBRID_COLLECTION.
  --chunk-words=<number>    Default: 600.
  --embed-batch=<number>    Default: 24; reduced automatically for large cases.
  --limit=<number>          Process only the first N distinct pending file names.
  --log-dir=<path>          Default: logs.
  --help                    Show this message.

Examples:
  npm run sync-updated-hybrid -- --dry-run --limit=5
  npm run sync-updated-hybrid -- --apply --limit=1
  npm run sync-updated-hybrid -- --apply
`);
  process.exit(0);
}

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = {
    apply: false,
    collectionName: env.qdrant.hybridCollection,
    chunkWordTarget: 600,
    embedBatchSize: 24,
    limit: null,
    logDir: "logs",
  };

  for (const arg of argv) {
    if (arg === "--help" || arg === "-h") usage();
    if (arg === "--apply") {
      options.apply = true;
      continue;
    }
    if (arg === "--dry-run") {
      options.apply = false;
      continue;
    }

    const [key, value] = arg.split("=", 2);
    if (key === "--collection" && value) options.collectionName = value;
    else if (key === "--chunk-words" && value) options.chunkWordTarget = Number(value);
    else if (key === "--embed-batch" && value) options.embedBatchSize = Number(value);
    else if (key === "--limit" && value) options.limit = Number(value);
    else if (key === "--log-dir" && value) options.logDir = value;
    else throw new Error(`Unknown option: ${arg}`);
  }

  if (!options.collectionName.trim()) {
    throw new Error("Qdrant collection name must not be empty");
  }
  if (!Number.isFinite(options.chunkWordTarget) || options.chunkWordTarget <= 0) {
    throw new Error("--chunk-words must be a positive number");
  }
  if (!Number.isFinite(options.embedBatchSize) || options.embedBatchSize <= 0) {
    throw new Error("--embed-batch must be a positive number");
  }
  if (
    options.limit !== null &&
    (!Number.isSafeInteger(options.limit) || options.limit <= 0)
  ) {
    throw new Error("--limit must be a positive integer");
  }

  return options;
}

function normalizeFileName(value: unknown): number | null {
  const raw = String(value ?? "").replace(/,/g, "").trim();
  if (!/^\d+$/.test(raw)) return null;

  const fileName = Number(raw);
  if (!Number.isSafeInteger(fileName) || fileName <= 0) return null;
  return fileName;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function ensureLogDir(logDir: string): string {
  const resolved = path.resolve(logDir);
  fs.mkdirSync(resolved, { recursive: true });
  return resolved;
}

function appendJsonLine(filePath: string, value: Record<string, unknown>): void {
  fs.appendFileSync(filePath, `${JSON.stringify(value)}\n`, "utf8");
}

function normalizeQdrantUrl(url: string): string {
  return url.replace(/\/+$/, "");
}

async function qdrantRequest<T>(
  pathName: string,
  init?: RequestInit,
  maxRetries = 5
): Promise<T> {
  const baseUrl = normalizeQdrantUrl(env.qdrant.url);
  let attempt = 0;

  while (true) {
    try {
      const response = await fetch(`${baseUrl}${pathName}`, {
        ...init,
        headers: {
          "Content-Type": "application/json",
          ...(env.qdrant.apiKey ? { "api-key": env.qdrant.apiKey } : {}),
          ...(init?.headers ?? {}),
        },
      });

      if (!response.ok) {
        const responseText = await response.text();
        const error = new Error(
          `Qdrant request failed: ${response.status} ${response.statusText} :: ${responseText}`
        );
        (error as Error & { status?: number }).status = response.status;
        throw error;
      }

      if (response.status === 204) return {} as T;
      return (await response.json()) as T;
    } catch (error: any) {
      attempt += 1;
      const status = Number(error?.status);
      const errorCode = error?.code ?? error?.cause?.code;
      const retryable =
        status === 408 ||
        status === 429 ||
        status >= 500 ||
        errorCode === "ECONNRESET" ||
        errorCode === "ETIMEDOUT" ||
        errorCode === "UND_ERR_CONNECT_TIMEOUT";

      if (!retryable || attempt > maxRetries) throw error;

      const delay = Math.min(1_000 * 2 ** (attempt - 1), 15_000);
      console.warn(
        `Qdrant request retry ${attempt}/${maxRetries} in ${delay}ms: ${error?.message ?? error}`
      );
      await sleep(delay);
    }
  }
}

function caseFilter(fileName: number): Record<string, unknown> {
  return {
    must: [{ key: "caseId", match: { value: fileName } }],
  };
}

async function countPoints(
  collectionName: string,
  filter: Record<string, unknown>
): Promise<number> {
  const data = await qdrantRequest<{ result?: { count?: number } }>(
    `/collections/${encodeURIComponent(collectionName)}/points/count`,
    {
      method: "POST",
      body: JSON.stringify({ exact: true, filter }),
    }
  );
  return Number(data?.result?.count ?? 0);
}

function normalizeCourtId(value: unknown): number | null {
  const courtId = Number(value);
  if (!Number.isSafeInteger(courtId) || courtId <= 0) return null;
  return courtId;
}

function normalizeExistingDecisionDate(value: unknown): string | null {
  const decisionDate = String(value ?? "").trim();
  return /^\d{4}-\d{2}-\d{2}$/.test(decisionDate) ? decisionDate : null;
}

function normalizeExistingDecisionYear(value: unknown): number | null {
  const decisionYear = Number(value);
  if (!Number.isSafeInteger(decisionYear) || decisionYear < 1000 || decisionYear > 9999) {
    return null;
  }
  return decisionYear;
}

async function fetchExistingCaseState(
  collectionName: string,
  fileName: number
): Promise<ExistingCaseState> {
  const [count, scroll] = await Promise.all([
    countPoints(collectionName, caseFilter(fileName)),
    qdrantRequest<{
      result?: { points?: Array<{ payload?: Record<string, unknown> }> };
    }>(`/collections/${encodeURIComponent(collectionName)}/points/scroll`, {
      method: "POST",
      body: JSON.stringify({
        filter: caseFilter(fileName),
        limit: 1,
        with_payload: ["courtId", "decisionDate", "decisionYear"],
        with_vector: false,
      }),
    }),
  ]);

  const existingCourtId = normalizeCourtId(
    scroll?.result?.points?.[0]?.payload?.courtId
  );
  const existingDecisionDate = normalizeExistingDecisionDate(
    scroll?.result?.points?.[0]?.payload?.decisionDate
  );
  const existingDecisionYear = normalizeExistingDecisionYear(
    scroll?.result?.points?.[0]?.payload?.decisionYear
  );

  return {
    count,
    courtId: existingCourtId,
    decisionDate: existingDecisionDate,
    decisionYear: existingDecisionYear,
  };
}

async function fetchAllCasePointIds(
  collectionName: string,
  fileName: number
): Promise<Array<number | string>> {
  const ids: Array<number | string> = [];
  let offset: QdrantOffset = null;
  const seenOffsets = new Set<string>();

  while (true) {
    const data: PointIdScrollResponse = await qdrantRequest<PointIdScrollResponse>(
      `/collections/${encodeURIComponent(collectionName)}/points/scroll`,
      {
        method: "POST",
        body: JSON.stringify({
          filter: caseFilter(fileName),
          limit: 256,
          offset,
          with_payload: false,
          with_vector: false,
        }),
      }
    );

    const points = data?.result?.points ?? [];
    for (const point of points) {
      if (point.id !== undefined) ids.push(point.id);
    }

    const nextOffset: QdrantOffset | undefined = data?.result?.next_page_offset;
    if (nextOffset === null || nextOffset === undefined || points.length === 0) break;

    const offsetKey = JSON.stringify(nextOffset);
    if (seenOffsets.has(offsetKey)) {
      throw new Error(`Qdrant returned a repeated scroll offset for case ${fileName}`);
    }
    seenOffsets.add(offsetKey);
    offset = nextOffset;
  }

  return ids;
}

async function deletePointIds(
  collectionName: string,
  pointIds: Array<number | string>
): Promise<void> {
  const batchSize = 256;
  for (let index = 0; index < pointIds.length; index += batchSize) {
    await qdrantRequest(
      `/collections/${encodeURIComponent(collectionName)}/points/delete?wait=true`,
      {
        method: "POST",
        body: JSON.stringify({ points: pointIds.slice(index, index + batchSize) }),
      }
    );
  }
}

async function verifyCollection(collectionName: string): Promise<void> {
  const data = await qdrantRequest<{
    result?: {
      config?: {
        params?: {
          vectors?: Record<string, { size?: number }>;
          sparse_vectors?: Record<string, unknown>;
        };
      };
    };
  }>(`/collections/${encodeURIComponent(collectionName)}`);

  const params = data?.result?.config?.params;
  const denseSize = Number(params?.vectors?.dense?.size);
  const hasSparse = Boolean(params?.sparse_vectors?.sparse);

  if (Number.isFinite(denseSize) && denseSize !== env.embedding.dimensions) {
    throw new Error(
      `Embedding dimension mismatch: collection dense=${denseSize}, configured=${env.embedding.dimensions}`
    );
  }
  if (!hasSparse) {
    throw new Error(`Collection ${collectionName} does not have sparse vector "sparse"`);
  }

  console.log(
    `Qdrant collection verified: ${collectionName} dense=${denseSize || "unknown"} sparse=sparse`
  );
}

async function fetchPendingFileNames(limit: number | null): Promise<number[]> {
  const pool = await getPool();
  const request = pool.request();
  const topClause = limit === null ? "" : "TOP (@limit)";

  if (limit !== null) request.input("limit", limit);

  const result = await request.query<AuditRow>(`
    SELECT DISTINCT ${topClause}
      file_name
    FROM ${AUDIT_TABLE} WITH (READPAST)
    WHERE isexportAI = 0
      AND file_name IS NOT NULL
      AND file_name > 0
    ORDER BY file_name ASC
  `);

  const unique = new Set<number>();
  for (const row of result.recordset) {
    const fileName = normalizeFileName(row.file_name);
    if (fileName === null) {
      console.warn(`Ignoring invalid pending file_name: ${String(row.file_name)}`);
      continue;
    }
    unique.add(fileName);
  }

  return [...unique];
}

async function acknowledgeAuditRows(fileName: number): Promise<number> {
  const pool = await getPool();
  const result = await pool
    .request()
    .input("fileName", fileName)
    .query(`
      UPDATE ${AUDIT_TABLE}
      SET isexportAI = 1
      WHERE file_name = @fileName
        AND isexportAI = 0
    `);

  return result.rowsAffected.reduce(
    (sum: number, count: number) => sum + count,
    0
  );
}

function inferCourtId(fileName: number): number | null {
  const courtId = Math.floor(fileName / 100000);
  return VALID_COURT_IDS.has(courtId) ? courtId : null;
}

function parseDecisionDate(value: string | null | undefined): {
  decisionDate: string | null;
  decisionYear: number | null;
} {
  const raw = String(value ?? "").trim();
  const match = raw.match(/^(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})$/);
  if (!match) return { decisionDate: null, decisionYear: null };

  const day = Number(match[1]);
  const month = MONTHS[match[2].toLowerCase()];
  const year = Number(match[3]);
  if (!month) return { decisionDate: null, decisionYear: null };

  const date = new Date(Date.UTC(year, month - 1, day));
  const valid =
    date.getUTCFullYear() === year &&
    date.getUTCMonth() === month - 1 &&
    date.getUTCDate() === day;
  if (!valid) return { decisionDate: null, decisionYear: null };

  return {
    decisionDate: `${String(year).padStart(4, "0")}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`,
    decisionYear: year,
  };
}

function enrichPoints(
  points: HybridPoint[],
  fileName: number,
  rawDecisionDate: string | null,
  existing: ExistingCaseState
): HybridPoint[] {
  const courtId = existing.courtId ?? inferCourtId(fileName);
  const parsedDecision = parseDecisionDate(rawDecisionDate);
  const decisionDate = parsedDecision.decisionDate ?? existing.decisionDate;
  const decisionYear = parsedDecision.decisionYear ?? existing.decisionYear;

  return points.map((point) => ({
    ...point,
    payload: {
      ...point.payload,
      ...(courtId !== null ? { courtId } : {}),
      ...(decisionDate !== null ? { decisionDate } : {}),
      ...(decisionYear !== null ? { decisionYear } : {}),
    },
  }));
}

function adaptiveEmbeddingBatchSize(
  chunksLength: number,
  configuredMaximum: number
): number {
  const adaptive = chunksLength > 500 ? 8 : chunksLength > 200 ? 12 : configuredMaximum;
  return Math.max(1, Math.min(configuredMaximum, adaptive));
}

async function embedAllChunksStrictly(
  texts: string[],
  batchSize: number
): Promise<number[][]> {
  const vectors = await embedTextsInBatches(texts, batchSize);
  if (vectors.length !== texts.length) {
    throw new Error(
      `Embedding count mismatch: texts=${texts.length}, vectors=${vectors.length}`
    );
  }

  for (let index = 0; index < vectors.length; index += 1) {
    const vector = vectors[index];
    if (vector.length !== env.embedding.dimensions) {
      throw new Error(
        `Embedding dimension mismatch at chunk ${index}: expected=${env.embedding.dimensions}, actual=${vector.length}`
      );
    }
    if (vector.some((value) => !Number.isFinite(value))) {
      throw new Error(`Embedding contains a non-finite value at chunk ${index}`);
    }
  }

  return vectors;
}

function validatePoints(points: HybridPoint[], fileName: number): void {
  if (!points.length) throw new Error("No Qdrant points were built");

  const ids = new Set<number>();
  for (let index = 0; index < points.length; index += 1) {
    const point = points[index];
    if (!Number.isSafeInteger(point.id)) {
      throw new Error(`Unsafe Qdrant point ID at chunk ${index}: ${point.id}`);
    }
    if (ids.has(point.id)) {
      throw new Error(`Duplicate Qdrant point ID: ${point.id}`);
    }
    ids.add(point.id);

    if (Number(point.payload.caseId) !== fileName) {
      throw new Error(`caseId mismatch at chunk ${index}`);
    }
    if (Number(point.payload.fileName) !== fileName) {
      throw new Error(`fileName mismatch at chunk ${index}`);
    }
    if (Number(point.payload.chunkIndex) !== index) {
      throw new Error(`Non-contiguous chunkIndex at position ${index}`);
    }
  }
}

async function replaceAndVerifyCase(
  collectionName: string,
  fileName: number,
  points: HybridPoint[]
): Promise<void> {
  const { upsertHybridPoints } = await import("../qdrant/hybridUpsert.js");
  await upsertHybridPoints(collectionName, points);

  const expectedIds = new Set(points.map((point) => String(point.id)));
  const allCasePointIds = await fetchAllCasePointIds(collectionName, fileName);
  const stalePointIds = allCasePointIds.filter(
    (pointId) => !expectedIds.has(String(pointId))
  );
  await deletePointIds(collectionName, stalePointIds);

  const [finalCount, finalPointIds] = await Promise.all([
    countPoints(collectionName, caseFilter(fileName)),
    fetchAllCasePointIds(collectionName, fileName),
  ]);
  const finalIds = new Set(finalPointIds.map((pointId) => String(pointId)));
  const finalIdsMatch =
    finalIds.size === expectedIds.size &&
    [...expectedIds].every((pointId) => finalIds.has(pointId));

  if (finalCount !== points.length || !finalIdsMatch) {
    throw new Error(
      `Final Qdrant verification failed: expected=${points.length}, total=${finalCount}, idsMatch=${finalIdsMatch}`
    );
  }
}

async function main(): Promise<void> {
  const options = parseArgs(process.argv.slice(2));
  const runId = new Date().toISOString().replace(/[:.]/g, "-");
  const logDir = ensureLogDir(options.logDir);
  const logFile = path.join(logDir, `sync-updated-hybrid-${runId}.jsonl`);
  const counters: RunCounters = {
    pendingCases: 0,
    successfulCases: 0,
    failedCases: 0,
    dryRunCases: 0,
    oldPoints: 0,
    newPoints: 0,
    acknowledgedAuditRows: 0,
  };

  console.log(`Mode=${options.apply ? "APPLY" : "DRY RUN"}`);
  console.log(`Audit table=${AUDIT_TABLE}`);
  console.log(`Collection=${options.collectionName}`);
  console.log(`Chunk words=${options.chunkWordTarget}, overlap paragraphs=1`);
  console.log(`Embedding model=${env.embedding.model}, dimensions=${env.embedding.dimensions}`);
  console.log(`Log=${logFile}`);

  const fileNames = await fetchPendingFileNames(options.limit);
  counters.pendingCases = fileNames.length;

  console.log(`Pending distinct cases selected=${fileNames.length}`);
  if (!fileNames.length) {
    console.log("Nothing to process.");
    return;
  }

  await verifyCollection(options.collectionName);

  let shouldStop = false;
  const requestStop = (signal: string) => {
    if (!shouldStop) {
      shouldStop = true;
      console.warn(`Received ${signal}; stopping after the current case.`);
    }
  };
  process.on("SIGINT", () => requestStop("SIGINT"));
  process.on("SIGTERM", () => requestStop("SIGTERM"));

  for (let index = 0; index < fileNames.length; index += 1) {
    if (shouldStop) break;

    const fileName = fileNames[index];
    const prefix = `[${index + 1}/${fileNames.length}] file_name=${fileName}`;
    const caseStartedAt = Date.now();

    try {
      const row = await fetchOneCase(fileName);
      if (!row) throw new Error("Case not found in dbo.jtext_data");
      if (!row.jtext?.trim()) throw new Error("Case has empty jtext");

      const raw: RawCaseRow = {
        fileName,
        ftype: row.ftype,
        flag: row.flag,
        html: row.jtext,
      };
      const parsed = parseCase(raw);
      const chunks = chunkParagraphs(
        parsed.caseId,
        parsed.paragraphs,
        options.chunkWordTarget,
        1
      );
      if (!chunks.length) throw new Error("Parser produced zero chunks");

      const existing = await fetchExistingCaseState(options.collectionName, fileName);
      counters.oldPoints += existing.count;

      console.log(
        `${prefix} title="${parsed.title ?? ""}" oldPoints=${existing.count} newChunks=${chunks.length} courtId=${existing.courtId ?? inferCourtId(fileName) ?? "null"}`
      );

      if (!options.apply) {
        counters.dryRunCases += 1;
        appendJsonLine(logFile, {
          runId,
          fileName,
          status: "dry_run_would_replace",
          oldPoints: existing.count,
          newChunks: chunks.length,
          courtId: existing.courtId ?? inferCourtId(fileName),
          title: parsed.title,
          court: parsed.court,
          dateOfDecision: parsed.dateOfDecision,
          warnings: parsed.warnings,
          durationMs: Date.now() - caseStartedAt,
          at: new Date().toISOString(),
        });
        continue;
      }

      const batchSize = adaptiveEmbeddingBatchSize(
        chunks.length,
        options.embedBatchSize
      );
      const vectors = await embedAllChunksStrictly(
        chunks.map((chunk) => chunk.text),
        batchSize
      );
      const { buildHybridPoints } = await import("../qdrant/hybridUpsert.js");
      const points = enrichPoints(
        buildHybridPoints(parsed, chunks, vectors),
        fileName,
        parsed.dateOfDecision,
        existing
      );
      validatePoints(points, fileName);

      await replaceAndVerifyCase(
        options.collectionName,
        fileName,
        points
      );

      const acknowledgedRows = await acknowledgeAuditRows(fileName);
      if (acknowledgedRows <= 0) {
        throw new Error(
          "Qdrant replacement succeeded, but no pending AuditlogForWebsite rows were acknowledged"
        );
      }

      counters.successfulCases += 1;
      counters.newPoints += points.length;
      counters.acknowledgedAuditRows += acknowledgedRows;

      console.log(
        `${prefix} success: replaced ${existing.count} with ${points.length} points; acknowledgedRows=${acknowledgedRows}`
      );
      appendJsonLine(logFile, {
        runId,
        fileName,
        status: "success",
        oldPoints: existing.count,
        newPoints: points.length,
        acknowledgedAuditRows: acknowledgedRows,
        title: parsed.title,
        warnings: parsed.warnings,
        durationMs: Date.now() - caseStartedAt,
        at: new Date().toISOString(),
      });
    } catch (error: any) {
      counters.failedCases += 1;
      const message = error?.message || String(error);
      console.error(`${prefix} failed: ${message}`);
      appendJsonLine(logFile, {
        runId,
        fileName,
        status: "failed",
        error: message,
        durationMs: Date.now() - caseStartedAt,
        at: new Date().toISOString(),
      });
    }
  }

  console.log("Done.");
  console.table(counters);
  console.log(`Detailed log=${logFile}`);

  if (counters.failedCases > 0) process.exitCode = 1;
}

main()
  .catch((error) => {
    console.error(error);
    process.exitCode = 1;
  })
  .finally(async () => {
    try {
      const pool = await getPool();
      await pool.close();
    } catch {
      // The original failure is more useful than a connection-close failure.
    }
  });
