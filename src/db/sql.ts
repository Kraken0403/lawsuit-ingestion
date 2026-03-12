import sql from "mssql";
import { getPool } from "./pool.js";

export type JtextRow = {
  file_name: number;
  ftype: string;
  jtext: string | null;
  flag: number;
};

export async function fetchCasesBatch(
  afterId: number,
  limit: number
): Promise<JtextRow[]> {
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
      ORDER BY file_name ASC
    `);

  return result.recordset;
}

export async function fetchCasesBatchInRange(
  afterId: number,
  endId: number,
  limit: number
): Promise<JtextRow[]> {
  const pool = await getPool();

  const result = await pool
    .request()
    .input("afterId", sql.Int, afterId)
    .input("endId", sql.Int, endId)
    .input("limit", sql.Int, limit)
    .query<JtextRow>(`
      SELECT TOP (@limit)
        file_name,
        ftype,
        jtext,
        flag
      FROM dbo.jtext_data
      WHERE file_name > @afterId
        AND file_name <= @endId
        AND jtext IS NOT NULL
      ORDER BY file_name ASC
    `);

  return result.recordset;
}