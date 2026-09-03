import fs from "node:fs";
import path from "node:path";
import sql from "mssql";
import XLSX from "xlsx";

import { getPool, type JtextRow } from "../db/sql.js";
import { parseCase } from "../parser/parseCase.js";
import { chunkParagraphs } from "../parser/chunker.js";
import type { RawCaseRow, Chunk } from "../parser/types.js";
import { embedTextsInBatches } from "../embeddings/embed.js";
import { ensureHybridCollection } from "../qdrant/hybridCollections.js";
import {
  buildHybridPoints,
  upsertHybridPoints,
  type HybridPoint,
} from "../qdrant/hybridUpsert.js";
import { env } from "../config/env.js";

type PointId = number | string;

type CliOptions = {
  inputPath: string;
  apply: boolean;
  collectionName: string;
  sheetName: string | null;
  chunkWordTarget: number;
  sqlBatchSize: number;
  embedBatchSize: number;
  limit: number | null;
  logDir: string;
  deleteEmpty: boolean;
  deleteNoChunks: boolean;
};

type ExcelReadResult = {
  fileNames: number[];
  duplicateCount: number;
  invalidRows: Array<{ rowNumber: number; value: unknown; reason: string }>;
  sheetName: string;
  sourceColumn: string;
};

type ProcessCounters = {
  totalInput: number;
  totalUnique: number;
  foundInSql: number;
  missingSql: number;
  emptyJtext: number;
  noChunks: number;
  successCases: number;
  failedCases: number;
  deletedPoints: number;
  insertedPoints: number;
  skippedOversizedChunks: number;
};

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
  npx tsx src/scripts/reingestHybridCasesFromExcel.ts <excel-or-csv-path> [options]

Options:
  --apply                       Actually delete old Qdrant points and upsert fresh points.
  --dry-run                     Default. Reads Excel + SQL and prepares summary without writing Qdrant.
  --collection=<name>           Qdrant collection. Default: env.qdrant.hybridCollection.
  --sheet=<sheetName>           Excel sheet name. Default: first sheet.
  --chunk-words=<number>        Chunk target words. Default: 600.
  --sql-batch=<number>          SQL IN-query batch size. Default: 500.
  --embed-batch=<number>        Embedding batch size ceiling. Default: 24.
  --limit=<number>              Process only first N unique file names. Useful for testing.
  --log-dir=<path>              Log output folder. Default: logs.
  --delete-empty                If SQL row exists but jtext is empty, delete old Qdrant points.
  --delete-no-chunks            If parsing produces no chunks, delete old Qdrant points.

Excel format:
  Preferred column: file_name
  Accepted aliases: fileName, filename, file_id, fileId, File ID, caseId, case_id

Examples:
  npx tsx src/scripts/reingestHybridCasesFromExcel.ts ./updated-cases.xlsx --dry-run
  npx tsx src/scripts/reingestHybridCasesFromExcel.ts ./updated-cases.xlsx --apply
  npx tsx src/scripts/reingestHybridCasesFromExcel.ts ./updated-cases.xlsx --apply --chunk-words=600 --sql-batch=500
`);
  process.exit(1);
}

function parseArgs(argv: string[]): CliOptions {
  const inputPath = argv.find((arg) => !arg.startsWith("--"));
  if (!inputPath) usage();

  const options: CliOptions = {
    inputPath: path.resolve(inputPath),
    apply: false,
    collectionName: env.qdrant.hybridCollection,
    sheetName: null,
    chunkWordTarget: 600,
    sqlBatchSize: 500,
    embedBatchSize: 24,
    limit: null,
    logDir: "logs",
    deleteEmpty: false,
    deleteNoChunks: false,
  };

  for (const arg of argv) {
    if (!arg.startsWith("--")) continue;

    if (arg === "--apply") {
      options.apply = true;
      continue;
    }

    if (arg === "--dry-run") {
      options.apply = false;
      continue;
    }

    if (arg === "--delete-empty") {
      options.deleteEmpty = true;
      continue;
    }

    if (arg === "--delete-no-chunks") {
      options.deleteNoChunks = true;
      continue;
    }

    const [key, value] = arg.split("=", 2);

    if (key === "--collection" && value) options.collectionName = value;
    else if (key === "--sheet" && value) options.sheetName = value;
    else if (key === "--chunk-words" && value) options.chunkWordTarget = Number(value);
    else if (key === "--sql-batch" && value) options.sqlBatchSize = Number(value);
    else if (key === "--embed-batch" && value) options.embedBatchSize = Number(value);
    else if (key === "--limit" && value) options.limit = Number(value);
    else if (key === "--log-dir" && value) options.logDir = value;
    else {
      throw new Error(`Unknown option: ${arg}`);
    }
  }

  if (!fs.existsSync(options.inputPath)) {
    throw new Error(`Input file not found: ${options.inputPath}`);
  }

  if (!Number.isFinite(options.chunkWordTarget) || options.chunkWordTarget <= 0) {
    throw new Error("--chunk-words must be a positive number");
  }

  if (!Number.isFinite(options.sqlBatchSize) || options.sqlBatchSize <= 0 || options.sqlBatchSize > 2000) {
    throw new Error("--sql-batch must be a positive number up to 2000");
  }

  if (!Number.isFinite(options.embedBatchSize) || options.embedBatchSize <= 0) {
    throw new Error("--embed-batch must be a positive number");
  }

  if (options.limit !== null && (!Number.isFinite(options.limit) || options.limit <= 0)) {
    throw new Error("--limit must be a positive number");
  }

  return options;
}

function normalizeHeader(value: unknown): string {
  return String(value ?? "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");
}

function parseFileName(value: unknown): number | null {
  const raw = String(value ?? "")
    .replace(/,/g, "")
    .trim();

  if (!raw) return null;

  const normalized = raw.replace(/\.0+$/, "");
  if (!/^\d+$/.test(normalized)) return null;

  const numeric = Number(normalized);
  if (!Number.isSafeInteger(numeric) || numeric <= 0) return null;

  return numeric;
}

function readFileNamesFromExcel(inputPath: string, requestedSheetName: string | null): ExcelReadResult {
  const workbook = XLSX.readFile(inputPath, { cellDates: false });
  const sheetName = requestedSheetName ?? workbook.SheetNames[0];

  if (!sheetName || !workbook.Sheets[sheetName]) {
    throw new Error(
      `Sheet not found. Requested=${requestedSheetName ?? "first sheet"}. Available=${workbook.SheetNames.join(", ")}`
    );
  }

  const sheet = workbook.Sheets[sheetName];
  const rows = XLSX.utils.sheet_to_json<unknown[]>(sheet, {
    header: 1,
    defval: "",
    raw: false,
  });

  const nonEmptyRows = rows.filter((row) =>
    Array.isArray(row) && row.some((cell) => String(cell ?? "").trim() !== "")
  );

  if (!nonEmptyRows.length) {
    return {
      fileNames: [],
      duplicateCount: 0,
      invalidRows: [],
      sheetName,
      sourceColumn: "none",
    };
  }

  const aliases = new Set([
    "filename",
    "fileid",
    "file",
    "fileno",
    "caseno",
    "caseid",
    "id",
  ]);

  const firstRow = nonEmptyRows[0];
  const headerIndex = firstRow.findIndex((cell) => aliases.has(normalizeHeader(cell)));
  const hasHeader = headerIndex >= 0;
  const sourceColumnIndex = hasHeader ? headerIndex : 0;
  const sourceColumn = hasHeader ? String(firstRow[headerIndex] ?? "file_name") : "Column A/no header";
  const startIndex = hasHeader ? 1 : 0;

  const seen = new Set<number>();
  const fileNames: number[] = [];
  const invalidRows: Array<{ rowNumber: number; value: unknown; reason: string }> = [];
  let duplicateCount = 0;

  for (let i = startIndex; i < nonEmptyRows.length; i += 1) {
    const row = nonEmptyRows[i];
    const value = row[sourceColumnIndex];
    const rowNumber = rows.indexOf(row) + 1;

    if (String(value ?? "").trim() === "") {
      continue;
    }

    const fileName = parseFileName(value);
    if (fileName == null) {
      invalidRows.push({ rowNumber, value, reason: "Not a positive numeric file_name" });
      continue;
    }

    if (seen.has(fileName)) {
      duplicateCount += 1;
      continue;
    }

    seen.add(fileName);
    fileNames.push(fileName);
  }

  return { fileNames, duplicateCount, invalidRows, sheetName, sourceColumn };
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function normalizeQdrantUrl(url: string): string {
  return url.replace(/\/+$/, "");
}

async function qdrantRequest<T>(pathName: string, init?: RequestInit): Promise<T> {
  const baseUrl = normalizeQdrantUrl(env.qdrant.url);

  const response = await fetch(`${baseUrl}${pathName}`, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      ...(env.qdrant.apiKey ? { "api-key": env.qdrant.apiKey } : {}),
      ...(init?.headers ?? {}),
    },
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(
      `Qdrant request failed: ${response.status} ${response.statusText} :: ${text}`
    );
  }

  if (response.status === 204) return {} as T;
  return (await response.json()) as T;
}

function caseFilter(fileName: number): Record<string, unknown> {
  // Existing buildHybridPoints() always writes both caseId and fileName.
  // caseId is equal to fileName in parseCase(), so this is the safest targeted filter.
  return {
    must: [{ key: "caseId", match: { value: fileName } }],
  };
}

async function countExistingPoints(collectionName: string, fileName: number): Promise<number> {
  const data = await qdrantRequest<{ result?: { count?: number } }>(
    `/collections/${encodeURIComponent(collectionName)}/points/count`,
    {
      method: "POST",
      body: JSON.stringify({
        exact: true,
        filter: caseFilter(fileName),
      }),
    }
  );

  return data?.result?.count ?? 0;
}

async function deleteExistingPoints(collectionName: string, fileName: number): Promise<void> {
  await qdrantRequest(`/collections/${encodeURIComponent(collectionName)}/points/delete?wait=true`, {
    method: "POST",
    body: JSON.stringify({
      filter: caseFilter(fileName),
    }),
  });
}

function toNumericString(value: string | number | null | undefined): string | null {
  const raw = String(value ?? "")
    .replace(/,/g, "")
    .trim();

  if (!raw || !/^\d+$/.test(raw)) return null;
  return raw;
}

function inferCourtId(value: string | number | null | undefined): number | null {
  const raw = toNumericString(value);
  if (!raw) return null;

  const num = Number(raw);
  if (!Number.isFinite(num) || num <= 0) return null;

  const courtId = Math.floor(num / 100000);
  return VALID_COURT_IDS.has(courtId) ? courtId : null;
}

function parseDecisionDate(value: string | null | undefined): {
  decisionDate: string | null;
  decisionYear: number | null;
} {
  const raw = String(value ?? "").trim();
  if (!raw) return { decisionDate: null, decisionYear: null };

  const match = raw.match(/^(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})$/);
  if (!match) return { decisionDate: null, decisionYear: null };

  const day = Number(match[1]);
  const monthName = match[2].toLowerCase();
  const year = Number(match[3]);
  const month = MONTHS[monthName];

  if (!month) return { decisionDate: null, decisionYear: null };

  const dt = new Date(Date.UTC(year, month - 1, day));
  const valid =
    dt.getUTCFullYear() === year &&
    dt.getUTCMonth() === month - 1 &&
    dt.getUTCDate() === day;

  if (!valid) return { decisionDate: null, decisionYear: null };

  return {
    decisionDate: `${String(year).padStart(4, "0")}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`,
    decisionYear: year,
  };
}

function addDerivedPayloadFields(points: HybridPoint[], fileName: number, dateOfDecision: string | null): HybridPoint[] {
  const courtId = inferCourtId(fileName);
  const { decisionDate, decisionYear } = parseDecisionDate(dateOfDecision);
  const reingestedAt = new Date().toISOString();

  return points.map((point) => {
    const payload = {
      ...point.payload,
      reingestedAt,
      ingestionSource: "excel-sql-refresh",
    };

    if (courtId !== null) payload.courtId = courtId;
    if (decisionDate !== null) payload.decisionDate = decisionDate;
    if (decisionYear !== null) payload.decisionYear = decisionYear;

    return { ...point, payload };
  });
}

async function fetchCasesByFileNames(fileNames: number[], sqlBatchSize: number): Promise<Map<number, JtextRow>> {
  const pool = await getPool();
  const resultMap = new Map<number, JtextRow>();

  for (let i = 0; i < fileNames.length; i += sqlBatchSize) {
    const batch = fileNames.slice(i, i + sqlBatchSize);
    const request = pool.request();
    const paramNames: string[] = [];

    batch.forEach((fileName, index) => {
      const paramName = `id${index}`;
      paramNames.push(`@${paramName}`);
      request.input(paramName, sql.Int, fileName);
    });

    const query = `
      SELECT
        file_name,
        ftype,
        jtext,
        flag
      FROM dbo.jtext_data
      WHERE file_name IN (${paramNames.join(", ")})
    `;

    console.log(
      `Fetching SQL batch ${Math.floor(i / sqlBatchSize) + 1}/${Math.ceil(fileNames.length / sqlBatchSize)} with ${batch.length} file_names...`
    );

    const result = await request.query<JtextRow>(query);

    for (const row of result.recordset) {
      const fileName = Number(row.file_name);
      if (Number.isFinite(fileName)) {
        resultMap.set(fileName, row);
      }
    }
  }

  return resultMap;
}

async function embedChunksSafely(
  chunks: Chunk[],
  batchSize: number
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
    const batchTexts = batchChunks.map((chunk) => chunk.text);

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
        err?.status === 400 && message.includes("maximum context length");

      if (!isContextLengthError) {
        throw err;
      }

      console.warn(
        "Batch embedding failed due to oversized chunk(s). Falling back to per-chunk embedding for this batch..."
      );

      for (let j = 0; j < batchChunks.length; j += 1) {
        const chunk = batchChunks[j];

        try {
          const vectors = await embedTextsInBatches([chunk.text], 1);
          keptChunks.push(chunk);
          denseVectors.push(vectors[0]);

          await sleep(150);
        } catch (innerErr: any) {
          const innerMessage = innerErr?.message || "Unknown embedding error";

          if (
            innerErr?.status === 400 &&
            innerMessage.includes("maximum context length")
          ) {
            const originalIndex = i + j;
            skipped.push({ index: originalIndex, reason: "maximum context length" });
            console.warn(
              `Skipping oversized chunk at index=${originalIndex} for case=${chunk.caseId}: ${innerMessage}`
            );
            continue;
          }

          throw innerErr;
        }
      }
    }
  }

  return { keptChunks, denseVectors, skipped };
}

function adaptiveEmbeddingBatchSize(chunksLength: number, configuredMax: number): number {
  const adaptive = chunksLength > 500 ? 8 : chunksLength > 200 ? 12 : configuredMax;
  return Math.max(1, Math.min(configuredMax, adaptive));
}

function ensureLogDir(logDir: string): string {
  const resolved = path.resolve(logDir);
  fs.mkdirSync(resolved, { recursive: true });
  return resolved;
}

function appendJsonLine(filePath: string, value: Record<string, unknown>): void {
  fs.appendFileSync(filePath, `${JSON.stringify(value)}\n`, "utf8");
}

function writeCsvLine(filePath: string, values: Array<string | number | null | undefined>): void {
  const line = values
    .map((value) => {
      const raw = String(value ?? "");
      if (/[",\n]/.test(raw)) {
        return `"${raw.replace(/"/g, '""')}"`;
      }
      return raw;
    })
    .join(",");

  fs.appendFileSync(filePath, `${line}\n`, "utf8");
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  const startedAt = Date.now();
  const runId = new Date().toISOString().replace(/[:.]/g, "-");
  const logDir = ensureLogDir(options.logDir);
  const jsonlLog = path.join(logDir, `reingest-hybrid-excel-${runId}.jsonl`);
  const failedCsv = path.join(logDir, `reingest-hybrid-excel-failed-${runId}.csv`);

  writeCsvLine(failedCsv, ["file_name", "status", "reason"]);

  console.log(`Mode=${options.apply ? "APPLY" : "DRY RUN"}`);
  console.log(`Input=${options.inputPath}`);
  console.log(`Collection=${options.collectionName}`);
  console.log(`Chunk words=${options.chunkWordTarget}`);
  console.log(`SQL batch=${options.sqlBatchSize}`);
  console.log(`Embed batch ceiling=${options.embedBatchSize}`);
  console.log(`Log JSONL=${jsonlLog}`);
  console.log(`Failed CSV=${failedCsv}`);

  const excel = readFileNamesFromExcel(options.inputPath, options.sheetName);
  let fileNames = excel.fileNames;

  if (options.limit !== null) {
    fileNames = fileNames.slice(0, options.limit);
  }

  const counters: ProcessCounters = {
    totalInput: excel.fileNames.length + excel.duplicateCount + excel.invalidRows.length,
    totalUnique: fileNames.length,
    foundInSql: 0,
    missingSql: 0,
    emptyJtext: 0,
    noChunks: 0,
    successCases: 0,
    failedCases: 0,
    deletedPoints: 0,
    insertedPoints: 0,
    skippedOversizedChunks: 0,
  };

  console.log(
    `Excel parsed: sheet="${excel.sheetName}" sourceColumn="${excel.sourceColumn}" unique=${excel.fileNames.length} duplicates=${excel.duplicateCount} invalid=${excel.invalidRows.length}`
  );

  if (excel.invalidRows.length) {
    console.warn("Invalid Excel rows sample:");
    console.table(excel.invalidRows.slice(0, 20));
  }

  if (!fileNames.length) {
    console.log("No valid file_name values found. Nothing to do.");
    return;
  }

  const shouldStop = { value: false };
  const requestStop = (signal: string) => {
    if (!shouldStop.value) {
      shouldStop.value = true;
      console.warn(`Received ${signal}. Will stop after the current case finishes.`);
    }
  };

  process.on("SIGINT", () => requestStop("SIGINT"));
  process.on("SIGTERM", () => requestStop("SIGTERM"));

  await ensureHybridCollection(options.collectionName, env.embedding.dimensions);

  const sqlRows = await fetchCasesByFileNames(fileNames, options.sqlBatchSize);

  for (let index = 0; index < fileNames.length; index += 1) {
    if (shouldStop.value) break;

    const fileName = fileNames[index];
    const row = sqlRows.get(fileName) ?? null;
    const prefix = `[${index + 1}/${fileNames.length}] file_name=${fileName}`;

    try {
      if (!row) {
        counters.missingSql += 1;
        console.warn(`${prefix} missing in SQL`);
        appendJsonLine(jsonlLog, { fileName, status: "missing_sql", at: new Date().toISOString() });
        writeCsvLine(failedCsv, [fileName, "missing_sql", "No row found in dbo.jtext_data"]);
        continue;
      }

      counters.foundInSql += 1;

      const existingCount = await countExistingPoints(options.collectionName, fileName);

      if (!row.jtext) {
        counters.emptyJtext += 1;

        if (options.apply && options.deleteEmpty) {
          await deleteExistingPoints(options.collectionName, fileName);
          counters.deletedPoints += existingCount;
          console.warn(`${prefix} empty jtext. Deleted ${existingCount} old points because --delete-empty was passed.`);
          appendJsonLine(jsonlLog, {
            fileName,
            status: "deleted_empty_jtext",
            existingCount,
            at: new Date().toISOString(),
          });
        } else {
          console.warn(`${prefix} empty jtext. Skipped. Existing Qdrant points=${existingCount}.`);
          appendJsonLine(jsonlLog, {
            fileName,
            status: "empty_jtext_skipped",
            existingCount,
            at: new Date().toISOString(),
          });
          writeCsvLine(failedCsv, [fileName, "empty_jtext", "SQL row has empty jtext"]);
        }

        continue;
      }

      const raw: RawCaseRow = {
        fileName,
        ftype: row.ftype,
        flag: row.flag,
        html: row.jtext,
      };

      const parsed = parseCase(raw);
      const chunks = chunkParagraphs(parsed.caseId, parsed.paragraphs, options.chunkWordTarget, 1);

      if (!chunks.length) {
        counters.noChunks += 1;

        if (options.apply && options.deleteNoChunks) {
          await deleteExistingPoints(options.collectionName, fileName);
          counters.deletedPoints += existingCount;
          console.warn(`${prefix} parsed with no chunks. Deleted ${existingCount} old points because --delete-no-chunks was passed.`);
          appendJsonLine(jsonlLog, {
            fileName,
            status: "deleted_no_chunks",
            existingCount,
            warnings: parsed.warnings,
            at: new Date().toISOString(),
          });
        } else {
          console.warn(`${prefix} parsed with no chunks. Skipped. Existing Qdrant points=${existingCount}.`);
          appendJsonLine(jsonlLog, {
            fileName,
            status: "no_chunks_skipped",
            existingCount,
            warnings: parsed.warnings,
            at: new Date().toISOString(),
          });
          writeCsvLine(failedCsv, [fileName, "no_chunks", "Parser created zero chunks"]);
        }

        continue;
      }

      const batchSize = adaptiveEmbeddingBatchSize(chunks.length, options.embedBatchSize);

      console.log(
        `${prefix} title="${parsed.title ?? ""}" existingPoints=${existingCount} newChunks=${chunks.length} embedBatch=${batchSize}`
      );

      if (!options.apply) {
        const courtId = inferCourtId(fileName);
        const { decisionDate, decisionYear } = parseDecisionDate(parsed.dateOfDecision);

        console.log(
          `${prefix} DRY RUN: would replace ${existingCount} old points with ${chunks.length} new chunks. courtId=${courtId ?? "null"} decisionDate=${decisionDate ?? "null"} decisionYear=${decisionYear ?? "null"}`
        );

        appendJsonLine(jsonlLog, {
          fileName,
          status: "dry_run_would_replace",
          existingCount,
          newChunks: chunks.length,
          title: parsed.title,
          court: parsed.court,
          dateOfDecision: parsed.dateOfDecision,
          courtId,
          decisionDate,
          decisionYear,
          warnings: parsed.warnings,
          at: new Date().toISOString(),
        });

        continue;
      }

      const { keptChunks, denseVectors, skipped } = await embedChunksSafely(chunks, batchSize);
      counters.skippedOversizedChunks += skipped.length;

      if (!keptChunks.length) {
        counters.failedCases += 1;
        console.warn(`${prefix} all chunks failed embedding. Not deleting existing Qdrant points.`);
        appendJsonLine(jsonlLog, {
          fileName,
          status: "embedding_all_chunks_failed",
          existingCount,
          skipped,
          at: new Date().toISOString(),
        });
        writeCsvLine(failedCsv, [fileName, "embedding_failed", "All chunks failed embedding"]);
        continue;
      }

      const basePoints = buildHybridPoints(parsed, keptChunks, denseVectors);
      const points = addDerivedPayloadFields(basePoints, fileName, parsed.dateOfDecision);

      // Safety: only delete once the fresh points have been fully prepared.
      await deleteExistingPoints(options.collectionName, fileName);
      await upsertHybridPoints(options.collectionName, points);

      counters.successCases += 1;
      counters.deletedPoints += existingCount;
      counters.insertedPoints += points.length;

      console.log(
        `${prefix} replaced successfully. deletedOld=${existingCount} insertedNew=${points.length} skippedOversized=${skipped.length}`
      );

      appendJsonLine(jsonlLog, {
        fileName,
        status: "success",
        existingCount,
        insertedPoints: points.length,
        skippedOversizedChunks: skipped.length,
        title: parsed.title,
        court: parsed.court,
        dateOfDecision: parsed.dateOfDecision,
        warnings: parsed.warnings,
        at: new Date().toISOString(),
      });
    } catch (error: any) {
      counters.failedCases += 1;
      const message = error?.message || String(error);
      console.error(`${prefix} failed: ${message}`);

      appendJsonLine(jsonlLog, {
        fileName,
        status: "failed",
        error: message,
        stack: error?.stack,
        at: new Date().toISOString(),
      });

      writeCsvLine(failedCsv, [fileName, "failed", message]);
    }
  }

  const elapsedMinutes = (Date.now() - startedAt) / 1000 / 60;

  console.log("\nDone.");
  console.table({
    mode: options.apply ? "APPLY" : "DRY RUN",
    inputUniqueProcessed: counters.totalUnique,
    foundInSql: counters.foundInSql,
    missingSql: counters.missingSql,
    emptyJtext: counters.emptyJtext,
    noChunks: counters.noChunks,
    successCases: counters.successCases,
    failedCases: counters.failedCases,
    deletedPoints: counters.deletedPoints,
    insertedPoints: counters.insertedPoints,
    skippedOversizedChunks: counters.skippedOversizedChunks,
    elapsedMinutes: elapsedMinutes.toFixed(2),
  });

  console.log(`JSONL log: ${jsonlLog}`);
  console.log(`Failed CSV: ${failedCsv}`);

  if (!options.apply) {
    console.log("\nDry run only. No Qdrant points were deleted or inserted. Add --apply to execute replacement.");
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
