"use strict";

const path = require("path");
require("dotenv").config({ path: path.resolve(__dirname, "../../.env") });

const { google } = require("googleapis");
const { pool } = require("../../config/db");
const { withIngestionRun } = require("../lib/ingestionRun");

const SHEET_ID = process.env.SHEET_ID;
const TAB_NAME = process.env.GSHEET_TAB_CASHFLOW || "2026 Inventory & Cashflow";
const SHEET_KEY = process.env.GSHEET_KEY || "cashflow_main";
const RANGE_A1 = process.env.GSHEET_RANGE_CASHFLOW || `'${TAB_NAME}'!A1:AZ800`;

const SA_EMAIL = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL;
const SA_KEY = (process.env.GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY || "").replace(/\\n/g, "\n");

const PLAN_YEAR = Number(process.env.PLAN_YEAR || "2026");
const MODEL_VERSION = process.env.MODEL_VERSION || "sheet_v1";

const DRY_RUN = process.argv.includes("--dry-run");

if (!SHEET_ID) throw new Error("Missing SHEET_ID");
if (!SA_EMAIL) throw new Error("Missing GOOGLE_SERVICE_ACCOUNT_EMAIL");
if (!SA_KEY) throw new Error("Missing GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY");

function normalizeText(x) {
  return String(x ?? "").replace(/\u00A0/g, " ").trim();
}
function toLowerNorm(x) {
  return normalizeText(x).toLowerCase();
}
function toNumber(x) {
  const s = normalizeText(x);
  if (!s) return null;
  const parenNeg = s.match(/^\((.*)\)$/);
  const raw = parenNeg ? `-${parenNeg[1]}` : s;
  const cleaned = raw.replace(/[$,]/g, "").replace(/\s+/g, "");
  const n = Number(cleaned);
  return Number.isFinite(n) ? n : null;
}
function toIntNonNeg(x) {
  const n = toNumber(x);
  if (n == null) return null;
  const i = Math.trunc(n);
  return i < 0 ? 0 : i;
}

const MONTHS = ["jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"];
function monthDate(year, monthIdx0) {
  const m = String(monthIdx0 + 1).padStart(2, "0");
  return `${year}-${m}-01`;
}

function findMonthHeader(values) {
  for (let r = 0; r < values.length; r++) {
    const row = (values[r] || []).map((c) => toLowerNorm(c));
    let hitCount = 0;
    const monthToCol = {};
    for (const m of MONTHS) {
      const idx = row.findIndex((x) => x === m || x.startsWith(m));
      if (idx >= 0) {
        monthToCol[m] = idx;
        hitCount++;
      }
    }
    if (hitCount >= 8) return { headerRow: r, monthToCol };
  }
  return null;
}

function findRowByLabelInColA(values, label, startRow, endRow) {
  const target = label.toLowerCase();
  const end = Math.min(endRow, values.length);
  for (let r = startRow; r < end; r++) {
    const a = toLowerNorm((values[r] || [])[0]);
    if (a === target) return r;
  }
  return -1;
}
function findRowByLabelContainsInColA(values, needle, startRow, endRow) {
  const n = needle.toLowerCase();
  const end = Math.min(endRow, values.length);
  for (let r = startRow; r < end; r++) {
    const a = toLowerNorm((values[r] || [])[0]);
    if (a.includes(n)) return r;
  }
  return -1;
}
function readMonthCell(values, rowIdx, monthToCol, monthKey) {
  if (rowIdx < 0) return null;
  const col = monthToCol[monthKey];
  if (col == null) return null;
  return (values[rowIdx] || [])[col];
}
function blockWindow(anchorRow, maxSpan, valuesLen) {
  return {
    start: Math.max(0, anchorRow - 2),
    end: Math.min(valuesLen, anchorRow + maxSpan),
  };
}

async function getSheetsClient() {
  const jwt = new google.auth.JWT({
    email: SA_EMAIL,
    key: SA_KEY,
    scopes: ["https://www.googleapis.com/auth/spreadsheets.readonly"],
  });
  await jwt.authorize();
  return google.sheets({ version: "v4", auth: jwt });
}

function parseInboundForBlock(values, mh, { startInvLabel, supplyItemKey, maxSpan = 140, hasBatchRow = false }) {
  const { monthToCol } = mh;

  const rStartInv = findRowByLabelContainsInColA(values, startInvLabel, 0, values.length);
  if (rStartInv < 0) throw new Error(`Cannot find "${startInvLabel}" in col A`);

  const w = blockWindow(rStartInv, maxSpan, values.length);

  const rArriving = findRowByLabelInColA(values, "Inventory Arriving", w.start, w.end);
  if (rArriving < 0) throw new Error(`${supplyItemKey}: Cannot find "Inventory Arriving" within block window`);

  const rBatch = hasBatchRow
    ? findRowByLabelInColA(values, "Inventory Incoming Batch", w.start, w.end)
    : -1;

  const rows = [];

  for (let mi = 0; mi < MONTHS.length; mi++) {
    const mKey = MONTHS[mi];
    const arrivalMonth = monthDate(PLAN_YEAR, mi);

    const qty = toIntNonNeg(readMonthCell(values, rArriving, monthToCol, mKey)) ?? 0;

    // batch label is TEXT (THM only). It can be empty/null.
    const batchRaw = rBatch >= 0 ? readMonthCell(values, rBatch, monthToCol, mKey) : null;
    const batch = normalizeText(batchRaw) || null;

    if (qty === 0 && !batch) continue;

    const sheetRowKey = `${PLAN_YEAR}-${String(mi + 1).padStart(2, "0")}|${supplyItemKey}|${batch || "NO_BATCH"}`;

    rows.push({
      supply_item_key: supplyItemKey,
      arrival_month: arrivalMonth,
      arrival_date: null,
      quantity: qty,
      batch_name: batch,
      location_name: null,
      model_version: MODEL_VERSION,
      sheet_row_key: sheetRowKey,
    });
  }

  return { window: w, rStartInv, rArriving, rBatch, rows };
}

async function upsertSnapshot(client, { sheetKey, tabName, rangeA1, values }) {
  const q = `
    insert into ops.sheet_tab_snapshots (sheet_key, tab_name, range_a1, values)
    values ($1, $2, $3, $4::jsonb)
  `;
  await client.query(q, [sheetKey, tabName, rangeA1, JSON.stringify(values)]);
}

async function upsertInboundSupplyItem(client, row) {
  const q = `
    insert into ops.fact_inbound_shipments_supply_item
      (supply_item_key, arrival_month, arrival_date, quantity, batch_name, location_name,
       model_version, source, sheet_row_key, ingested_at)
    values
      ($1,$2,$3,$4,$5,$6,$7,'google_sheets',$8,now())
    on conflict (source, sheet_row_key) do update set
      supply_item_key = excluded.supply_item_key,
      arrival_month = excluded.arrival_month,
      arrival_date = excluded.arrival_date,
      quantity = excluded.quantity,
      batch_name = excluded.batch_name,
      location_name = excluded.location_name,
      model_version = excluded.model_version,
      ingested_at = now()
  `;
  await client.query(q, [
    row.supply_item_key,
    row.arrival_month,
    row.arrival_date,
    row.quantity,
    row.batch_name,
    row.location_name,
    row.model_version,
    row.sheet_row_key,
  ]);
}

async function main() {
  await withIngestionRun(
    {
      source: "google_sheets",
      pipeline: "sheets_inbound_shipments_2026",
      metadata: {
        sheet_id: SHEET_ID,
        tab_name: TAB_NAME,
        range_a1: RANGE_A1,
        plan_year: PLAN_YEAR,
        model_version: MODEL_VERSION,
        dry_run: DRY_RUN,
      },
    },
    async (ctx) => {
      const sheets = await getSheetsClient();
      const res = await sheets.spreadsheets.values.get({
        spreadsheetId: SHEET_ID,
        range: RANGE_A1,
        majorDimension: "ROWS",
        valueRenderOption: "UNFORMATTED_VALUE",
      });

      const values = res.data.values || [];
      const mh = findMonthHeader(values);
      if (!mh) throw new Error("Could not find month header row (Jan..Dec).");

      // Parse blocks
      const thm = parseInboundForBlock(values, mh, {
        startInvLabel: "THM Starting Inventory",
        supplyItemKey: "THM",
        hasBatchRow: true, // THM has Inventory Incoming Batch
      });

      const brush = parseInboundForBlock(values, mh, {
        startInvLabel: "Brush Starting Inventory",
        supplyItemKey: "BRUSH",
        hasBatchRow: false, // usually no batch row
      });

      const roller = parseInboundForBlock(values, mh, {
        startInvLabel: "Rollers Starting Inventory",
        supplyItemKey: "ROLLER",
        hasBatchRow: false, // usually no batch row
      });

      const allRows = [...thm.rows, ...brush.rows, ...roller.rows];
      console.log("\nSample parsed inbound rows (first 15):");
      console.table(allRows.slice(0, 15));

      if (DRY_RUN) {
        console.log("\nDry run completed (no DB writes).");
        return;
      }

      const client = await pool.connect();
      ctx.setClient(client);

      try {
        await client.query("begin");

        await upsertSnapshot(client, {
          sheetKey: SHEET_KEY,
          tabName: TAB_NAME,
          rangeA1: RANGE_A1,
          values,
        });
        ctx.incrementUpserts(1);

        for (const r of allRows) {
          await upsertInboundSupplyItem(client, r);
          ctx.incrementUpserts(1);
        }

        await client.query("commit");
      } catch (err) {
        try { await client.query("rollback"); } catch (_) {}
        throw err;
      } finally {
        ctx.setClient(null);
        client.release();
      }

      console.log("Ingestion completed");
    }
  );
}

main().catch((e) => {
  console.error("Fatal:", e.message);
  process.exitCode = 1;
});