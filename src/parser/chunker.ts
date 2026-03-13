import type { Chunk, JudgmentParagraph } from "./types.js";

function splitTextByWordLimit(text: string, maxWords: number): string[] {
  const words = text.split(/\s+/).filter(Boolean);
  if (words.length <= maxWords) return [text.trim()];

  const parts: string[] = [];
  for (let i = 0; i < words.length; i += maxWords) {
    parts.push(words.slice(i, i + maxWords).join(" ").trim());
  }
  return parts;
}

export function chunkParagraphs(
  caseId: number,
  paragraphs: JudgmentParagraph[],
  targetWords = 900,
  overlapParagraphs = 1
): Chunk[] {
  const chunks: Chunk[] = [];
  if (!paragraphs.length) return chunks;

  const normalizedParagraphs: JudgmentParagraph[] = [];

  for (const para of paragraphs) {
    const paraWords = para.text.split(/\s+/).filter(Boolean).length;

    if (paraWords <= targetWords) {
      normalizedParagraphs.push(para);
      continue;
    }

    const splitParts = splitTextByWordLimit(para.text, targetWords);

    for (const part of splitParts) {
      normalizedParagraphs.push({
        ...para,
        text: part,
      });
    }
  }

  let start = 0;
  let chunkIndex = 0;

  while (start < normalizedParagraphs.length) {
    let words = 0;
    let end = start;
    const selected: JudgmentParagraph[] = [];

    while (end < normalizedParagraphs.length) {
      const para = normalizedParagraphs[end];
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

    if (end >= normalizedParagraphs.length) break;
    start = Math.max(end - overlapParagraphs, start + 1);
  }

  return chunks;
}