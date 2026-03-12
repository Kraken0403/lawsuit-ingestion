import he from "he";
import type { CheerioAPI, Element } from "cheerio";

export function decodeAndNormalize(text: string | null | undefined): string {
  const decoded = he.decode(text || "");
  return decoded
    .replace(/\u00a0/g, " ")
    .replace(/[ \t]+/g, " ")
    .replace(/\s*\n\s*/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

export function normalizeInline(text: string | null | undefined): string {
  const decoded = he.decode(text || "");
  return decoded.replace(/\u00a0/g, " ").replace(/\s+/g, " ").trim();
}

export function cleanNodeText($: CheerioAPI, el: Element): string {
  const clone = $(el).clone();

  clone.find("a").each((_, a) => {
    const visible = normalizeInline(clone.find(a).text());
    clone.find(a).replaceWith(visible);
  });

  const text = normalizeInline(clone.text())
    .replace(/#aColspandDv/g, " ")
    .trim();

  return normalizeInline(text);
}

export function splitCommaList(value: string | null | undefined): string[] {
  const clean = normalizeInline(value);
  if (!clean) return [];

  return clean
    .split(/\s*,\s*/)
    .map((item) => item.trim())
    .filter(Boolean);
}

export function splitLooseList(value: string | null | undefined): string[] {
  const clean = normalizeInline(value);
  if (!clean) return [];

  return clean
    .split(/\s*;\s*|\s{2,}|\s*\|\s*|\s*,\s*/)
    .map((item) => item.trim().replace(/^[,;]+|[,;]+$/g, ""))
    .filter(Boolean);
}

export function parseParagraphNumber(text: string): {
  paraNo: number | null;
  text: string;
} {
  const patterns = [
    /^\[(\d+)\]\s*(.*)$/s,
    /^\((\d+)\)\s*(.*)$/s,
    /^(\d+)\.\s*(.*)$/s,
  ];

  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (match) {
      return {
        paraNo: Number(match[1]),
        text: normalizeInline(match[2]),
      };
    }
  }

  return {
    paraNo: null,
    text: normalizeInline(text),
  };
}