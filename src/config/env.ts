import dotenv from "dotenv";

dotenv.config();

function required(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required env var: ${name}`);
  }
  return value;
}

export const env = {
  sql: {
    host: required("SQL_HOST"),
    port: Number(process.env.SQL_PORT || 1433),
    database: required("SQL_DATABASE"),
    user: required("SQL_USER"),
    password: required("SQL_PASSWORD"),
    encrypt: String(process.env.SQL_ENCRYPT || "true") === "true",
    trustServerCertificate:
      String(process.env.SQL_TRUST_SERVER_CERT || "true") === "true",
  },
  qdrant: {
    url: required("QDRANT_URL"),
    apiKey: process.env.QDRANT_API_KEY || "",
    hybridCollection:
    process.env.QDRANT_HYBRID_COLLECTION || "lawsuit_cases_hybrid",
  },
  embedding: {
    apiKey: required("EMBEDDING_API_KEY"),
    model: process.env.EMBEDDING_MODEL || "text-embedding-3-small",
    baseUrl: process.env.EMBEDDING_BASE_URL || "https://api.openai.com/v1",
    dimensions: Number(process.env.EMBEDDING_DIMENSIONS || 1536),
  },
};