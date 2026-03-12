import { QdrantClient } from "@qdrant/js-client-rest";
import { env } from "../config/env.js";

export const qdrant = new QdrantClient({
  url: env.qdrant.url,
  apiKey: env.qdrant.apiKey || undefined,
});