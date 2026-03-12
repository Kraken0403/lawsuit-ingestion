export type RawCaseRow = {
    fileName: number;
    ftype: string | null;
    flag: number | null;
    html: string;
  };
  
  export type JudgmentParagraph = {
    paraNo: number | null;
    text: string;
    sourceClass: "judgpara" | "judgspara" | "unknown";
    role: "main" | "subpoint" | "lead";
  };
  
  export type ParsedCase = {
    caseId: number;
    fileName: number;
    ftype: string | null;
    flag: number | null;
  
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
    fullText: string;
  
    paragraphs: JudgmentParagraph[];
    warnings: string[];
  };
  
  export type Chunk = {
    caseId: number;
    chunkId: string;
    chunkIndex: number;
    paragraphStart: number;
    paragraphEnd: number;
    text: string;
    wordCount: number;
    hasSubpoints: boolean;
    sourceParagraphTypes: Array<"main" | "subpoint" | "lead">;
  };