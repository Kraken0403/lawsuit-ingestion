import { fetchOneCase } from "../db/sql.js";
import { parseCase } from "../parser/parseCase.js";
import { chunkParagraphs } from "../parser/chunker.js";
import type { RawCaseRow } from "../parser/types.js";

async function main() {
  const arg = process.argv[2];
  const caseId = arg ? Number(arg) : undefined;

  if (arg && Number.isNaN(caseId)) {
    throw new Error("If you pass a case id, it must be numeric.");
  }

  const row = await fetchOneCase(caseId);

  if (!row || !row.jtext) {
    throw new Error("No case found or jtext is empty.");
  }

  const raw: RawCaseRow = {
    fileName: row.file_name,
    ftype: row.ftype,
    flag: row.flag,
    html: row.jtext,
  };

  const parsed = parseCase(raw);
  const chunks = chunkParagraphs(parsed.caseId, parsed.paragraphs);

  console.log("=".repeat(80));
  console.log("CASE METADATA");
  console.log("=".repeat(80));
  console.log(`caseId             : ${parsed.caseId}`);
  console.log(`fileName           : ${parsed.fileName}`);
  console.log(`ftype              : ${parsed.ftype}`);
  console.log(`flag               : ${parsed.flag}`);
  console.log(`title              : ${parsed.title}`);
  console.log(`court              : ${parsed.court}`);
  console.log(`dateOfDecision     : ${parsed.dateOfDecision}`);
  console.log(`citation           : ${parsed.citation}`);
  console.log(`judges             : ${JSON.stringify(parsed.judges)}`);
  console.log(`caseType           : ${parsed.caseType}`);
  console.log(`caseNo             : ${parsed.caseNo}`);
  console.log(`subject            : ${parsed.subject}`);
  console.log(`actsReferred       : ${JSON.stringify(parsed.actsReferred)}`);
  console.log(`finalDecision      : ${parsed.finalDecision}`);
  console.log(`equivalentCitations: ${JSON.stringify(parsed.equivalentCitations)}`);
  console.log(`advocates          : ${JSON.stringify(parsed.advocates)}`);
  console.log(`cited              : ${parsed.cited}`);

  console.log("\n" + "=".repeat(80));
  console.log(`PARAGRAPHS EXTRACTED: ${parsed.paragraphs.length}`);
  console.log("=".repeat(80));

  parsed.paragraphs.slice(0, 500).forEach((p, idx) => {
    console.log(
      `\nParagraph ${idx + 1} | paraNo=${p.paraNo} | sourceClass=${p.sourceClass} | role=${p.role}`
    );
    console.log(p.text);
  });

  console.log("\n" + "=".repeat(80));
  console.log(`CHUNKS CREATED: ${chunks.length}`);
  console.log("=".repeat(80));

  chunks.slice(0, 25).forEach((chunk) => {
    console.log(
      `\nChunk ${chunk.chunkIndex} | id=${chunk.chunkId} | paragraphs ${chunk.paragraphStart} to ${chunk.paragraphEnd} | words=${chunk.wordCount} | hasSubpoints=${chunk.hasSubpoints}`
    );
    console.log(chunk.text);
  });

  if (parsed.warnings.length) {
    console.log("\n" + "=".repeat(80));
    console.log("WARNINGS");
    console.log("=".repeat(80));
    parsed.warnings.forEach((w) => console.log(`- ${w}`));
  }
}

main().catch((err) => {
  console.error("Preview failed:");
  console.error(err);
  process.exit(1);
});