import type { Chunk, JudgmentParagraph } from "./types.js";

export function chunkParagraphs(
  caseId: number,
  paragraphs: JudgmentParagraph[],
  targetWords = 900,
  overlapParagraphs = 1
): Chunk[] {
  const chunks: Chunk[] = [];
  if (!paragraphs.length) return chunks;

  let start = 0;
  let chunkIndex = 0;

  while (start < paragraphs.length) {
    let words = 0;
    let end = start;
    const selected: JudgmentParagraph[] = [];

    while (end < paragraphs.length) {
      const para = paragraphs[end];
      const paraWords = para.text.split(/\s+/).filter(Boolean).length;

      if (selected.length > 0 && words + paraWords > targetWords) {
        break;
      }

      selected.push(para);
      words += paraWords;
      end++;
    }

    const text = selected
      .map((p) => (p.paraNo !== null ? `[${p.paraNo}] ${p.text}` : p.text))
      .join("\n\n")
      .trim();

    const sourceParagraphTypes = [...new Set(selected.map((p) => p.role))];

    chunks.push({
      caseId,
      chunkId: `${caseId}_${chunkIndex}`,
      chunkIndex,
      paragraphStart: start + 1,
      paragraphEnd: end,
      text,
      wordCount: words,
      hasSubpoints: selected.some((p) => p.role === "subpoint"),
      sourceParagraphTypes,
    });

    chunkIndex++;

    if (end >= paragraphs.length) break;
    start = Math.max(end - overlapParagraphs, start + 1);
  }

  return chunks;
}