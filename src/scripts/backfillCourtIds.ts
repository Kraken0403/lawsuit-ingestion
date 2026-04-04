import { env } from "../config/env.js";

type PointId = number | string;

type ScrollPoint = {
  id: PointId;
  payload?: {
    caseId?: number | string | null;
    fileName?: number | string | null;
    courtId?: number | string | null;
    [key: string]: unknown;
  };
};

type ScrollResponse = {
  result?: {
    points?: ScrollPoint[];
    next_page_offset?: PointId | null;
  };
};

const VALID_COURT_IDS = new Set<number>([
  1, 2, 3, 4, 5, 6, 7, 8, 9,
  10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
  24, 25, 26, 27, 28, 29, 30, 40,
  82, 83, 84, 91, 97, 98, 104,
]);

function normalizeQdrantUrl(url: string): string {
  return url.replace(/\/+$/, "");
}

function toNumericString(value: string | number | null | undefined): string | null {
  const raw = String(value ?? "")
    .replace(/,/g, "")
    .trim();

  if (!raw || !/^\d+$/.test(raw)) {
    return null;
  }

  return raw;
}

/**
 * Rule:
 * filename = courtId * 100000 + runningSerial
 *
 * Examples:
 * 100001   -> courtId 1
 * 200001   -> courtId 2
 * 1000001  -> courtId 10
 * 10400001 -> courtId 104
 */
function inferCourtId(value: string | number | null | undefined): number | null {
  const raw = toNumericString(value);
  if (!raw) return null;

  const num = Number(raw);
  if (!Number.isFinite(num) || num <= 0) return null;

  const courtId = Math.floor(num / 100000);

  return VALID_COURT_IDS.has(courtId) ? courtId : null;
}

function normalizeExistingCourtId(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }

  if (typeof value === "string" && /^\d+$/.test(value.trim())) {
    return Number(value.trim());
  }

  return null;
}

async function qdrantRequest<T>(
  path: string,
  init?: RequestInit
): Promise<T> {
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

async function createCourtIdIndex(collectionName: string): Promise<void> {
  try {
    await qdrantRequest(`/collections/${collectionName}/index`, {
      method: "PUT",
      body: JSON.stringify({
        field_name: "courtId",
        field_schema: "integer",
      }),
    });

    console.log(`Payload index ensured for ${collectionName}.courtId`);
  } catch (error: any) {
    const message = String(error?.message || error);
    if (message.toLowerCase().includes("already exists")) {
      console.log(`Payload index already exists for ${collectionName}.courtId`);
      return;
    }
    throw error;
  }
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
        with_payload: ["caseId", "fileName", "courtId"],
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

async function backfillCourtIds(
  collectionName: string,
  limit = 1000,
  writeBatchSize = 500
): Promise<void> {
  let offset: PointId | null = null;
  let scanned = 0;
  let updated = 0;
  let missingOrInvalid = 0;
  let alreadyCorrect = 0;

  for (;;) {
    const { points, nextOffset } = await scrollPoints(collectionName, offset, limit);

    if (!points.length) {
      break;
    }

    scanned += points.length;

    const grouped = new Map<number, PointId[]>();

    for (const point of points) {
      const payload = point.payload ?? {};
      const sourceValue = payload.fileName ?? payload.caseId ?? null;
      const inferredCourtId = inferCourtId(sourceValue);

      if (inferredCourtId == null) {
        missingOrInvalid += 1;
        continue;
      }

      const existingCourtId = normalizeExistingCourtId(payload.courtId);

      if (existingCourtId === inferredCourtId) {
        alreadyCorrect += 1;
        continue;
      }

      const bucket = grouped.get(inferredCourtId) ?? [];
      bucket.push(point.id);
      grouped.set(inferredCourtId, bucket);
    }

    for (const [courtId, ids] of grouped.entries()) {
      for (let i = 0; i < ids.length; i += writeBatchSize) {
        const batchIds = ids.slice(i, i + writeBatchSize);
        await setPayload(collectionName, batchIds, { courtId });
        updated += batchIds.length;

        console.log(
          `Updated ${batchIds.length} points in ${collectionName} with courtId=${courtId}`
        );
      }
    }

    console.log(
      `Progress: scanned=${scanned} updated=${updated} alreadyCorrect=${alreadyCorrect} missingOrInvalid=${missingOrInvalid}`
    );

    if (nextOffset == null) {
      break;
    }

    offset = nextOffset;
  }

  console.log(
    `Backfill completed for ${collectionName}. scanned=${scanned} updated=${updated} alreadyCorrect=${alreadyCorrect} missingOrInvalid=${missingOrInvalid}`
  );
}

async function main() {
  const collectionName = process.argv[2] || env.qdrant.hybridCollection;
  const shouldCreateIndex = String(process.argv[3] || "true") !== "false";
  const scrollLimit = Number(process.argv[4] || 1000);
  const writeBatchSize = Number(process.argv[5] || 500);

  if (!Number.isFinite(scrollLimit) || scrollLimit <= 0) {
    throw new Error("scrollLimit must be a positive number");
  }

  if (!Number.isFinite(writeBatchSize) || writeBatchSize <= 0) {
    throw new Error("writeBatchSize must be a positive number");
  }

  console.log(`Target collection=${collectionName}`);
  console.log(`Create payload index=${shouldCreateIndex}`);
  console.log(`Scroll limit=${scrollLimit}`);
  console.log(`Write batch size=${writeBatchSize}`);

  await backfillCourtIds(collectionName, scrollLimit, writeBatchSize);

  if (shouldCreateIndex) {
    await createCourtIdIndex(collectionName);
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});