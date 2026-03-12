import { load } from "cheerio";
import type { RawCaseRow, ParsedCase } from "./types.js";
import { extractMetadata } from "./metadata.js";
import { extractBodyParagraphs } from "./body.js";

export function parseCase(raw: RawCaseRow): ParsedCase {
  const warnings: string[] = [];

  const $ = load(raw.html || "");

  $("script, style").remove();

  const metadata = extractMetadata($);
  const paragraphs = extractBodyParagraphs($);

  if (!metadata.title) warnings.push("Title not confidently extracted.");
  if (!metadata.court) warnings.push("Court not extracted.");
  if (!metadata.citation) warnings.push("Citation not extracted.");
  if (!paragraphs.length) {
    warnings.push("No judgment paragraphs extracted from .judgpara/.judgspara.");
  }

  const fullText = paragraphs
    .map((p) => (p.paraNo !== null ? `[${p.paraNo}] ${p.text}` : p.text))
    .join("\n\n")
    .trim();

  return {
    caseId: raw.fileName,
    fileName: raw.fileName,
    ftype: raw.ftype,
    flag: raw.flag,

    title: metadata.title,
    court: metadata.court,
    dateOfDecision: metadata.dateOfDecision,
    citation: metadata.citation,
    judges: metadata.judges,
    caseType: metadata.caseType,
    caseNo: metadata.caseNo,
    subject: metadata.subject,
    actsReferred: metadata.actsReferred,
    finalDecision: metadata.finalDecision,
    equivalentCitations: metadata.equivalentCitations,
    advocates: metadata.advocates,
    cited: metadata.cited,

    paragraphs,
    fullText,
    warnings,
  };
}