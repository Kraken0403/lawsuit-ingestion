import type { CheerioAPI } from "cheerio";
import type { JudgmentParagraph } from "./types.js";
import { normalizeInline, parseParagraphNumber } from "./normalize.js";

function extractOwnTextWithInlineChildren($: CheerioAPI, el: any): string {
  const clone = $(el).clone();

  clone.find("a").each((_, a) => {
    const visible = normalizeInline($(a).text());
    $(a).replaceWith(visible);
  });

  clone.find("br").replaceWith(" ");

  return normalizeInline(clone.text());
}

function looksLikeLeadLine(text: string): boolean {
  const clean = normalizeInline(text);

  return (
    /^[A-Z][A-Za-z.\s,'-]+,\s*(C\.?\s*J\.?|J\.?)\s*-\s*$/i.test(clean) ||
    /^per\s+court\s*[-:]*$/i.test(clean)
  );
}

export function extractBodyParagraphs($: CheerioAPI): JudgmentParagraph[] {
  const paragraphs: JudgmentParagraph[] = [];
  const seen = new Set<string>();

  $(".judgpara, .judgspara").each((_, el) => {
    const classAttr = ($(el).attr("class") || "").toLowerCase();
    const sourceClass =
      classAttr.includes("judgspara") ? "judgspara" : "judgpara";

    let text = extractOwnTextWithInlineChildren($, el);
    if (!text) return;

    if (/^judgement text\s*:?\s*$/i.test(text)) return;

    text = normalizeInline(text);
    if (!text) return;
    if (seen.has(text)) return;

    seen.add(text);

    const parsed = parseParagraphNumber(text);

    let role: "main" | "subpoint" | "lead" =
      sourceClass === "judgspara" ? "subpoint" : "main";

    if (looksLikeLeadLine(parsed.text)) {
      role = "lead";
    }

    paragraphs.push({
      paraNo: parsed.paraNo,
      text: parsed.text,
      sourceClass,
      role,
    });
  });

  // Merge first lead line into next paragraph if appropriate
  if (
    paragraphs.length >= 2 &&
    paragraphs[0].role === "lead" &&
    paragraphs[1].role === "main"
  ) {
    const merged: JudgmentParagraph = {
      paraNo: paragraphs[1].paraNo,
      sourceClass: paragraphs[1].sourceClass,
      role: "main",
      text: `${paragraphs[0].text} ${paragraphs[1].text}`.trim(),
    };

    return [merged, ...paragraphs.slice(2)];
  }

  return paragraphs;
}