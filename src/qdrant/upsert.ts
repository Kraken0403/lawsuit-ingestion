import { qdrant } from "./client.js";
import type { Chunk, ParsedCase } from "../parser/types.js";

export type ChunkPoint = {
  id: number;
  vector: number[];
  payload: Record<string, unknown>;
};

function buildNumericPointId(caseId: number, chunkIndex: number): number {
  return caseId * 100000 + chunkIndex;
}

export function buildChunkPoints(
  parsedCase: ParsedCase,
  chunks: Chunk[],
  vectors: number[][]
): ChunkPoint[] {
  if (chunks.length !== vectors.length) {
    throw new Error(
      `Chunks/vectors mismatch: chunks=${chunks.length} vectors=${vectors.length}`
    );
  }

  return chunks.map((chunk, idx) => ({
    id: buildNumericPointId(parsedCase.caseId, chunk.chunkIndex),
    vector: vectors[idx],
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
  }));
}

export async function upsertChunkPoints(
  collectionName: string,
  points: ChunkPoint[]
): Promise<void> {
  if (!points.length) return;

  await qdrant.upsert(collectionName, {
    wait: true,
    points,
  });
}