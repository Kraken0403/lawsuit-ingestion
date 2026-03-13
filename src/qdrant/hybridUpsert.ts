import { qdrant } from "./client.js";
import type { Chunk, ParsedCase } from "../parser/types.js";

export type HybridPoint = {
  id: number;
  vector: Record<string, unknown>;
  payload: Record<string, unknown>;
};

function buildNumericPointId(caseId: number, chunkIndex: number): number {
  return caseId * 100000 + chunkIndex;
}

function compact(value: unknown): string {
  if (value == null) return "";
  if (Array.isArray(value)) {
    return value.map((v) => String(v).trim()).filter(Boolean).join(", ");
  }
  return String(value).trim();
}

function buildSparseText(parsedCase: ParsedCase, chunk: Chunk): string {
  const pieces = [
    parsedCase.title ? `title: ${parsedCase.title}` : "",
    parsedCase.citation ? `citation: ${parsedCase.citation}` : "",
    parsedCase.caseNo ? `case no: ${parsedCase.caseNo}` : "",
    parsedCase.caseType ? `case type: ${parsedCase.caseType}` : "",
    parsedCase.court ? `court: ${parsedCase.court}` : "",
    parsedCase.dateOfDecision
      ? `date of decision: ${parsedCase.dateOfDecision}`
      : "",
    parsedCase.subject ? `subject: ${parsedCase.subject}` : "",
    parsedCase.finalDecision
      ? `final decision: ${parsedCase.finalDecision}`
      : "",
    parsedCase.judges?.length ? `judges: ${compact(parsedCase.judges)}` : "",
    parsedCase.actsReferred?.length
      ? `acts referred: ${compact(parsedCase.actsReferred)}`
      : "",
    parsedCase.equivalentCitations?.length
      ? `equivalent citations: ${compact(parsedCase.equivalentCitations)}`
      : "",
    `text: ${chunk.text}`,
  ];

  return pieces.filter(Boolean).join("\n");
}

export function buildHybridPoints(
  parsedCase: ParsedCase,
  chunks: Chunk[],
  denseVectors: number[][]
): HybridPoint[] {
  if (chunks.length !== denseVectors.length) {
    throw new Error(
      `Chunks/vectors mismatch: chunks=${chunks.length} vectors=${denseVectors.length}`
    );
  }

  return chunks.map((chunk, idx) => {
    const sparseText = buildSparseText(parsedCase, chunk);

    return {
      id: buildNumericPointId(parsedCase.caseId, chunk.chunkIndex),
      vector: {
        dense: denseVectors[idx],
        sparse: {
          text: sparseText,
          model: "Qdrant/bm25",
        },
      },
      payload: {
        caseId: parsedCase.caseId,
        fileName: parsedCase.fileName,
        ftype: parsedCase.ftype,
        flag: parsedCase.flag,

        title: parsedCase.title,
        court: parsedCase.court,
        dateOfDecision: parsedCase.dateOfDecision,
        citation: parsedCase.citation,
        judges: parsedCase.judges,
        caseType: parsedCase.caseType,
        caseNo: parsedCase.caseNo,
        subject: parsedCase.subject,
        actsReferred: parsedCase.actsReferred,
        finalDecision: parsedCase.finalDecision,
        equivalentCitations: parsedCase.equivalentCitations,
        advocates: parsedCase.advocates,
        cited: parsedCase.cited,

        chunkId: chunk.chunkId,
        chunkIndex: chunk.chunkIndex,
        paragraphStart: chunk.paragraphStart,
        paragraphEnd: chunk.paragraphEnd,
        wordCount: chunk.wordCount,
        hasSubpoints: chunk.hasSubpoints,
        sourceParagraphTypes: chunk.sourceParagraphTypes,

        text: chunk.text,
      },
    };
  });
}

function estimatePointBytes(point: HybridPoint): number {
  return Buffer.byteLength(JSON.stringify(point), "utf8");
}

function splitPointsForQdrant(
  points: HybridPoint[],
  maxBatchBytes = 8 * 1024 * 1024, // 8 MB safety target
  maxPointsPerBatch = 25
): HybridPoint[][] {
  const batches: HybridPoint[][] = [];
  let currentBatch: HybridPoint[] = [];
  let currentBytes = 0;

  for (const point of points) {
    const pointBytes = estimatePointBytes(point);

    // If a single point is huge, still send it alone.
    if (pointBytes > maxBatchBytes) {
      if (currentBatch.length) {
        batches.push(currentBatch);
        currentBatch = [];
        currentBytes = 0;
      }

      batches.push([point]);
      continue;
    }

    const exceedsByteLimit = currentBytes + pointBytes > maxBatchBytes;
    const exceedsPointLimit = currentBatch.length >= maxPointsPerBatch;

    if ((exceedsByteLimit || exceedsPointLimit) && currentBatch.length) {
      batches.push(currentBatch);
      currentBatch = [];
      currentBytes = 0;
    }

    currentBatch.push(point);
    currentBytes += pointBytes;
  }

  if (currentBatch.length) {
    batches.push(currentBatch);
  }

  return batches;
}

export async function upsertHybridPoints(
  collectionName: string,
  points: HybridPoint[]
): Promise<void> {
  if (!points.length) return;

  const batches = splitPointsForQdrant(points, 8 * 1024 * 1024, 25);

  for (let i = 0; i < batches.length; i++) {
    const batch = batches[i];
    const approxBytes = batch.reduce((sum, p) => sum + estimatePointBytes(p), 0);

    console.log(
      `Qdrant upsert batch ${i + 1}/${batches.length} with ${batch.length} points (~${(approxBytes / 1024 / 1024).toFixed(2)} MB)`
    );

    await qdrant.upsert(collectionName, {
      wait: true,
      points: batch,
    });
  }
}