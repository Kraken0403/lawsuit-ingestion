const VALID_COURT_IDS = new Set<number>([
  1, 2, 3, 4, 5, 6, 7, 8, 9,
  10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
  24, 25, 26, 27, 28, 29, 30, 40,
  82, 83, 84, 91, 97, 98, 104,
]);

function inferCourtId(value: string | number | null | undefined): number | null {
  const raw = String(value ?? "").replace(/,/g, "").trim();

  if (!/^\d+$/.test(raw)) return null;

  const num = Number(raw);
  if (!Number.isFinite(num) || num <= 0) return null;

  const courtId = Math.floor(num / 100000);

  return VALID_COURT_IDS.has(courtId) ? courtId : null;
}

console.log(inferCourtId(100001));    // 1
console.log(inferCourtId(199999));    // 1
console.log(inferCourtId(200001));    // 2
console.log(inferCourtId(1000001));   // 10
console.log(inferCourtId(10400001));  // 104