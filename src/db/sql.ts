import sql from "mssql";
import { env } from "../config/env.js";

let poolPromise: Promise<sql.ConnectionPool> | null = null;

export async function getPool(): Promise<sql.ConnectionPool> {
  if (!poolPromise) {
    poolPromise = new sql.ConnectionPool({
      server: env.sql.host,
      port: env.sql.port,
      database: env.sql.database,
      user: env.sql.user,
      password: env.sql.password,
      pool: {
        max: 10,
        min: 0,
        idleTimeoutMillis: 30000,
      },
      options: {
        encrypt: env.sql.encrypt,
        trustServerCertificate: env.sql.trustServerCertificate,
      },
      connectionTimeout: 30000,
      requestTimeout: 120000,
    }).connect();
  }

  return poolPromise;
}

export type JtextRow = {
  file_name: number;
  ftype: string | null;
  jtext: string | null;
  flag: number | null;
};

export async function fetchOneCase(caseId?: number): Promise<JtextRow | null> {
  const pool = await getPool();

  if (typeof caseId === "number") {
    const result = await pool
      .request()
      .input("caseId", sql.Int, caseId)
      .query<JtextRow>(`
        SELECT TOP (1)
          file_name,
          ftype,
          jtext,
          flag
        FROM dbo.jtext_data
        WHERE file_name = @caseId
      `);

    return result.recordset[0] ?? null;
  }

  const result = await pool.request().query<JtextRow>(`
    SELECT TOP (1)
      file_name,
      ftype,
      jtext,
      flag
    FROM dbo.jtext_data
    ORDER BY file_name
  `);

  return result.recordset[0] ?? null;
}

export async function fetchCasesBatch(afterId: number, limit: number): Promise<JtextRow[]> {
  const pool = await getPool();

  const result = await pool
    .request()
    .input("afterId", sql.Int, afterId)
    .input("limit", sql.Int, limit)
    .query<JtextRow>(`
      SELECT TOP (@limit)
        file_name,
        ftype,
        jtext,
        flag
      FROM dbo.jtext_data
      WHERE file_name > @afterId
        AND jtext IS NOT NULL
      ORDER BY file_name
    `);

  return result.recordset;
}