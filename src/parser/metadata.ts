import type { CheerioAPI } from "cheerio";
import { normalizeInline, splitCommaList, splitLooseList } from "./normalize.js";

export type ParsedMetadata = {
  title: string | null;
  court: string | null;
  dateOfDecision: string | null;
  citation: string | null;
  judges: string[];
  caseType: string | null;
  caseNo: string | null;
  subject: string | null;
  actsReferred: string[];
  finalDecision: string | null;
  equivalentCitations: string[];
  advocates: string[];
  cited: number | null;
};

function stripLabel(value: string, labelPattern: RegExp): string {
  return normalizeInline(value.replace(labelPattern, "").trim());
}

function extractSingleByClass(
  $: CheerioAPI,
  selector: string,
  labelPattern?: RegExp
): string | null {
  const el = $(selector).first();
  if (!el.length) return null;

  let text = normalizeInline(el.text());
  if (!text) return null;

  if (labelPattern) {
    text = stripLabel(text, labelPattern);
  }

  return text || null;
}

function extractTitle($: CheerioAPI): string | null {
  const el = $(".judgtitle").first();
  if (!el.length) return null;

  const html = el.html() || "";
  const withBreaks = html.replace(/<br\s*\/?>/gi, " ");
  const stripped = withBreaks.replace(/<[^>]+>/g, " ");
  return normalizeInline(stripped) || null;
}

function extractCourt($: CheerioAPI): string | null {
  const el = $(".highcourt").first();
  if (!el.length) return null;
  return normalizeInline(el.text()) || null;
}

function extractActs($: CheerioAPI): string[] {
  const el = $(".acts").first();
  if (!el.length) return [];

  const items: string[] = [];

  el.find("a").each((_, a) => {
    const text = normalizeInline($(a).text());
    if (text) items.push(text);
  });

  if (items.length) {
    return [...new Set(items)];
  }

  let fallback = normalizeInline(el.text());
  fallback = stripLabel(fallback, /^acts referred\s*:\s*/i);
  return splitLooseList(fallback);
}

function extractFinalDecision($: CheerioAPI): string | null {
  let found: string | null = null;

  $(".fd").each((_, el) => {
    const text = normalizeInline($(el).text());

    if (/^final decision\s*:/i.test(text)) {
      found = stripLabel(text, /^final decision\s*:\s*/i);
      return false;
    }
  });

  return found;
}

function extractSubject($: CheerioAPI): string | null {
  let found: string | null = null;

  $(".fd").each((_, el) => {
    const text = normalizeInline($(el).text());

    if (/^subject\s*:/i.test(text)) {
      found = stripLabel(text, /^subject\s*:\s*/i);
      return false;
    }
  });

  return found;
}

function extractCited($: CheerioAPI): number | null {
  const anchor = $("#aCaseCited").first();
  if (!anchor.length) return null;

  const parent = anchor.closest("p");
  if (!parent.length) return null;

  const countText = normalizeInline(parent.find("b").first().text());
  if (!countText) return null;

  const num = Number(countText);
  return Number.isFinite(num) ? num : null;
}

export function extractMetadata($: CheerioAPI): ParsedMetadata {
  return {
    title: extractTitle($),
    court: extractCourt($),
    dateOfDecision: extractSingleByClass(
      $,
      ".judgdate",
      /^date of decision\s*:\s*/i
    ),
    citation: extractSingleByClass(
      $,
      ".citations",
      /^citation\s*:\s*/i
    ),
    judges: splitCommaList(
      extractSingleByClass($, ".judges", /^hon'?ble judges\s*:\s*/i)
    ),
    caseType: extractSingleByClass(
      $,
      ".apptype",
      /^case type\s*:\s*/i
    ),
    caseNo: extractSingleByClass(
      $,
      ".appealno",
      /^case no\s*:\s*/i
    ),
    subject: extractSubject($),
    actsReferred: extractActs($),
    finalDecision: extractFinalDecision($),
    equivalentCitations: splitCommaList(
      extractSingleByClass($, ".eqcitations", /^eq\.\s*citations\s*:\s*/i)
    ),
    advocates: splitCommaList(
      extractSingleByClass($, ".advocate", /^advocates\s*:\s*/i)
    ),
    cited: extractCited($),
  };
}