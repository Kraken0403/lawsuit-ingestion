import { env } from "../config/env.js";

type PointId = number | string;

type ScrollPoint = {
  id: PointId;
  payload?: {
    dateOfDecision?: string | null;
    decisionDate?: string | null;
    decisionYear?: number | string | null;
    fileName?: number | string | null;
    caseId?: number | string | null;
    [key: string]: unknown;
  };
};

type ScrollResponse = {
  result?: {
    points?: ScrollPoint[];
    next_page_offset?: PointId | null;
  };
};

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

function normalizeQdrantUrl(url: string): string {
  return url.replace(/\/+$/, "");
}

function parseDecisionDate(value: string | null | undefined): {
  decisionDate: string | null;
  decisionYear: number | null;
} {
  const raw = String(value ?? "").trim();
  if (!raw) {
    return { decisionDate: null, decisionYear: null };
  }

  // Matches: 18 January 1949
  const match = raw.match(/^(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})$/);
  if (!match) {
    return { decisionDate: null, decisionYear: null };
  }

  const day = Number(match[1]);
  const monthName = match[2].toLowerCase();
  const year = Number(match[3]);

  const month = MONTHS[monthName];
  if (!month) {
    return { decisionDate: null, decisionYear: null };
  }

  // Validate actual calendar date
  const dt = new Date(Date.UTC(year, month - 1, day));
  const valid =
    dt.getUTCFullYear() === year &&
    dt.getUTCMonth() === month - 1 &&
    dt.getUTCDate() === day;

  if (!valid) {
    return { decisionDate: null, decisionYear: null };
  }

  return {
    decisionDate: `${String(year).padStart(4, "0")}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`,
    decisionYear: year,
  };
}

function normalizeExistingYear(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && /^\d+$/.test(value.trim())) {
    return Number(value.trim());
  }
  return null;
}

async function qdrantRequest<T>(path: string, init?: RequestInit): Promise<T> {
  const baseUrl = normalizeQdrantUrl(env.qdrant.url);

  const response = await fetch(`${baseUrl}${path}`, {
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

  if (response.status === 204) {
    return {} as T;
  }

  return (await response.json()) as T;
}

async function ensureIndex(
  collectionName: string,
  fieldName: string,
  fieldSchema: "datetime" | "integer"
): Promise<void> {
  try {
    await qdrantRequest(`/collections/${collectionName}/index`, {
      method: "PUT",
      body: JSON.stringify({
        field_name: fieldName,
        field_schema: fieldSchema,
      }),
    });
    console.log(`Payload index ensured for ${collectionName}.${fieldName}`);
  } catch (error: any) {
    const message = String(error?.message || error);
    if (message.toLowerCase().includes("already exists")) {
      console.log(`Payload index already exists for ${collectionName}.${fieldName}`);
      return;
    }
    throw error;
  }
}

async function ensureIndexes(collectionName: string): Promise<void> {
  await ensureIndex(collectionName, "decisionDate", "datetime");
  await ensureIndex(collectionName, "decisionYear", "integer");
}

async function scrollPoints(
  collectionName: string,
  offset: PointId | null,
  limit: number
): Promise<{ points: ScrollPoint[]; nextOffset: PointId | null }> {
  const data = await qdrantRequest<ScrollResponse>(
    `/collections/${collectionName}/points/scroll`,
    {
      method: "POST",
      body: JSON.stringify({
        limit,
        offset,
        with_payload: [
          "fileName",
          "caseId",
          "dateOfDecision",
          "decisionDate",
          "decisionYear",
        ],
        with_vector: false,
      }),
    }
  );

  return {
    points: data?.result?.points ?? [],
    nextOffset: data?.result?.next_page_offset ?? null,
  };
}

async function setPayload(
  collectionName: string,
  pointIds: PointId[],
  payload: Record<string, unknown>
): Promise<void> {
  await qdrantRequest(`/collections/${collectionName}/points/payload`, {
    method: "POST",
    body: JSON.stringify({
      points: pointIds,
      payload,
    }),
  });
}

async function backfillDecisionFields(
  collectionName: string,
  limit = 1000,
  writeBatchSize = 500
): Promise<void> {
  let offset: PointId | null = null;
  let scanned = 0;
  let updated = 0;
  let alreadyCorrect = 0;
  let unparseable = 0;
  const unparseableSamples: Array<{
    fileName: string | number | null;
    caseId: string | number | null;
    dateOfDecision: string | null;
  }> = [];

  for (;;) {
    const { points, nextOffset } = await scrollPoints(collectionName, offset, limit);

    if (!points.length) {
      break;
    }

    scanned += points.length;

    const grouped = new Map<string, PointId[]>();

    for (const point of points) {
      const payload = point.payload ?? {};
      const rawDate = (payload.dateOfDecision ?? null) as string | null;
      const { decisionDate, decisionYear } = parseDecisionDate(rawDate);

      if (!decisionDate || !decisionYear) {
        unparseable += 1;

        if (unparseableSamples.length < 20) {
          unparseableSamples.push({
            fileName: (payload.fileName ?? null) as string | number | null,
            caseId: (payload.caseId ?? null) as string | number | null,
            dateOfDecision: rawDate,
          });
        }

        continue;
      }

      const existingDate =
        typeof payload.decisionDate === "string" ? payload.decisionDate : null;
      const existingYear = normalizeExistingYear(payload.decisionYear);

      if (existingDate === decisionDate && existingYear === decisionYear) {
        alreadyCorrect += 1;
        continue;
      }

      const bucketKey = `${decisionDate}|${decisionYear}`;
      const bucket = grouped.get(bucketKey) ?? [];
      bucket.push(point.id);
      grouped.set(bucketKey, bucket);
    }

    for (const [bucketKey, ids] of grouped.entries()) {
      const [decisionDate, decisionYearRaw] = bucketKey.split("|");
      const decisionYear = Number(decisionYearRaw);

      for (let i = 0; i < ids.length; i += writeBatchSize) {
        const batchIds = ids.slice(i, i + writeBatchSize);

        await setPayload(collectionName, batchIds, {
          decisionDate,
          decisionYear,
        });

        updated += batchIds.length;

        console.log(
          `Updated ${batchIds.length} points in ${collectionName} with decisionDate=${decisionDate} decisionYear=${decisionYear}`
        );
      }
    }

    console.log(
      `Progress: scanned=${scanned} updated=${updated} alreadyCorrect=${alreadyCorrect} unparseable=${unparseable}`
    );

    if (nextOffset == null) {
      break;
    }

    offset = nextOffset;
  }

  console.log(
    `Backfill completed for ${collectionName}. scanned=${scanned} updated=${updated} alreadyCorrect=${alreadyCorrect} unparseable=${unparseable}`
  );

  if (unparseableSamples.length) {
    console.log("Sample unparseable dateOfDecision values:");
    console.table(unparseableSamples);
  }
}

function runSelfTest(): void {
  const tests = [
    "14 December 1950",
    "18 January 1949",
    "1 March 2000",
    "31 February 1950",
    "",
    null,
  ];

  console.log("Parser self-test:");
  for (const value of tests) {
    console.log(value, "=>", parseDecisionDate(value as string | null));
  }
}

async function main() {
  const collectionName = process.argv[2] || env.qdrant.hybridCollection;
  const shouldCreateIndexes = String(process.argv[3] || "true") !== "false";
  const scrollLimit = Number(process.argv[4] || 1000);
  const writeBatchSize = Number(process.argv[5] || 500);
  const selfTestOnly = String(process.argv[6] || "false") === "true";

  if (!Number.isFinite(scrollLimit) || scrollLimit <= 0) {
    throw new Error("scrollLimit must be a positive number");
  }

  if (!Number.isFinite(writeBatchSize) || writeBatchSize <= 0) {
    throw new Error("writeBatchSize must be a positive number");
  }

  console.log(`Target collection=${collectionName}`);
  console.log(`Create payload indexes=${shouldCreateIndexes}`);
  console.log(`Scroll limit=${scrollLimit}`);
  console.log(`Write batch size=${writeBatchSize}`);
  console.log(`Self test only=${selfTestOnly}`);

  runSelfTest();

  if (selfTestOnly) {
    process.exit(0);
  }

  if (shouldCreateIndexes) {
    await ensureIndexes(collectionName);
  }

  await backfillDecisionFields(collectionName, scrollLimit, writeBatchSize);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});