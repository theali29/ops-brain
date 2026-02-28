"use strict";

/**
 * WRITE INGESTION — 2026 Inventory & Cashflow (Inventory/Demand blocks only)
 *
 * Writes:
 *   - ops.sheet_tab_snapshots (raw audit snapshot)
 *   - ops.fact_demand_forecast_supply_item (THM/BRUSH/ROLLER monthly plan)
 *
 * Design:
 *   - Reads Google Sheet via service account (readonly)
 *   - Parses blocks by "Starting Inventory" anchors
 *   - Uses a single DB transaction (BEGIN/COMMIT/ROLLBACK)
 *   - Logs ops.ingestion_runs via withIngestionRun
 *   - Idempotent upsert via unique constraint (source, supply_item_key, month, model_version)
 */

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

if (!SHEET_ID) throw new Error("Missing SHEET_ID");
if (!SA_EMAIL) throw new Error("Missing GOOGLE_SERVICE_ACCOUNT_EMAIL");
if (!SA_KEY) throw new Error("Missing GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY");

function nowIso() {
  return new Date().toISOString();
}
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

function readMonthInt(values, rowIdx, monthToCol, monthKey) {
  if (rowIdx < 0) return null;
  const col = monthToCol[monthKey];
  if (col == null) return null;
  return toIntNonNeg(((values[rowIdx] || [])[col]));
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

async function upsertSnapshot(client, { sheetKey, tabName, rangeA1, values }) {
  const q = `
    insert into ops.sheet_tab_snapshots (sheet_key, tab_name, range_a1, values)
    values ($1, $2, $3, $4::jsonb)
  `;
  await client.query(q, [sheetKey, tabName, rangeA1, JSON.stringify(values)]);
}

async function upsertDemandForecastSupplyItem(client, row) {
  // row: { supply_item_key, month, forecast_new_customer_units, forecast_renewal_units, forecast_rebill_3m_units, forecast_rebill_1m_units, forecast_total_units, sheet_row_key }
  const q = `
    insert into ops.fact_demand_forecast_supply_item
      (supply_item_key, month,
       forecast_new_customer_units,
       forecast_renewal_units,
       forecast_rebill_3m_units,
       forecast_rebill_1m_units,
       forecast_total_units,
       model_version, source, sheet_row_key, ingested_at)
    values
      ($1,$2,$3,$4,$5,$6,$7,$8,'google_sheets',$9,now())
    on conflict (source, supply_item_key, month, model_version) do update set
      forecast_new_customer_units = excluded.forecast_new_customer_units,
      forecast_renewal_units = excluded.forecast_renewal_units,
      forecast_rebill_3m_units = excluded.forecast_rebill_3m_units,
      forecast_rebill_1m_units = excluded.forecast_rebill_1m_units,
      forecast_total_units = excluded.forecast_total_units,
      sheet_row_key = excluded.sheet_row_key,
      ingested_at = now()
  `;
  await client.query(q, [
    row.supply_item_key,
    row.month,
    row.forecast_new_customer_units,
    row.forecast_renewal_units,
    row.forecast_rebill_3m_units,
    row.forecast_rebill_1m_units,
    row.forecast_total_units,
    MODEL_VERSION,
    row.sheet_row_key,
  ]);
}

function parseTHM(values, mh) {
  const { monthToCol } = mh;

  const rStartInv = findRowByLabelInColA(values, "THM Starting Inventory", 0, values.length);
  if (rStartInv < 0) throw new Error(`Cannot find "THM Starting Inventory" in col A`);

  const w = blockWindow(rStartInv, 140, values.length);

  const rTotalNew = findRowByLabelInColA(values, "Total new CM Units", w.start, w.end);
  const rRenew80 = findRowByLabelContainsInColA(values, "Forecasted Renewal Units", w.start, w.end);
  const r3m80 = findRowByLabelInColA(values, "3m Rebill Orders at 80%", w.start, w.end);
  const r1m80 = findRowByLabelInColA(values, "1m Rebills at 80%", w.start, w.end);
  const rTotalInv = findRowByLabelInColA(values, "Total Inventory Needed", w.start, w.end);

  if (rTotalNew < 0 || rTotalInv < 0) {
    throw new Error(`THM missing required rows: "Total new CM Units" or "Total Inventory Needed"`);
  }

  const rows = [];
  for (let mi = 0; mi < MONTHS.length; mi++) {
    const mKey = MONTHS[mi];
    const month = monthDate(PLAN_YEAR, mi);

    const newUnits = readMonthInt(values, rTotalNew, monthToCol, mKey) ?? 0;
    const totalInv = readMonthInt(values, rTotalInv, monthToCol, mKey) ?? 0;

    if (newUnits === 0 && totalInv === 0) continue;

    const renew = rRenew80 >= 0 ? (readMonthInt(values, rRenew80, monthToCol, mKey) ?? 0) : 0;
    const o3 = r3m80 >= 0 ? (readMonthInt(values, r3m80, monthToCol, mKey) ?? 0) : 0;
    const o1 = r1m80 >= 0 ? (readMonthInt(values, r1m80, monthToCol, mKey) ?? 0) : 0;

    const rebill3mUnits = o3 * 3;
    const rebill1mUnits = o1;

    const sheetRowKey = `${PLAN_YEAR}-${String(mi + 1).padStart(2, "0")}|THM`;

    rows.push({
      supply_item_key: "THM",
      month,
      forecast_new_customer_units: newUnits,
      forecast_renewal_units: renew,
      forecast_rebill_3m_units: rebill3mUnits,
      forecast_rebill_1m_units: rebill1mUnits,
      forecast_total_units: totalInv, // authoritative
      sheet_row_key: sheetRowKey,
    });
  }

  return rows;
}

function parseSimple(values, mh, { startInvLabel, supplyItemKey, maxSpan = 120 }) {
  const { monthToCol } = mh;

  const rStartInv = findRowByLabelContainsInColA(values, startInvLabel, 0, values.length);
  if (rStartInv < 0) throw new Error(`Cannot find "${startInvLabel}" in col A`);

  const w = blockWindow(rStartInv, maxSpan, values.length);

  const rNew = findRowByLabelInColA(values, "New Cms", w.start, w.end);
  const rRebills = findRowByLabelInColA(values, "Rebills", w.start, w.end);
  const rTotalInv = findRowByLabelInColA(values, "Total Inventory Needed", w.start, w.end);

  if (rNew < 0 || rRebills < 0 || rTotalInv < 0) {
    throw new Error(`${supplyItemKey} missing required rows: New Cms / Rebills / Total Inventory Needed`);
  }

  const rows = [];
  for (let mi = 0; mi < MONTHS.length; mi++) {
    const mKey = MONTHS[mi];
    const month = monthDate(PLAN_YEAR, mi);

    const newCms = readMonthInt(values, rNew, monthToCol, mKey) ?? 0;
    const rebills = readMonthInt(values, rRebills, monthToCol, mKey) ?? 0;
    const totalInv = readMonthInt(values, rTotalInv, monthToCol, mKey) ?? 0;

    if (newCms === 0 && rebills === 0 && totalInv === 0) continue;

    const sheetRowKey = `${PLAN_YEAR}-${String(mi + 1).padStart(2, "0")}|${supplyItemKey}`;

    rows.push({
      supply_item_key: supplyItemKey,
      month,
      forecast_new_customer_units: newCms,
      forecast_renewal_units: 0,
      forecast_rebill_3m_units: 0,
      forecast_rebill_1m_units: 0,
      forecast_total_units: totalInv, // authoritative from sheet
      sheet_row_key: sheetRowKey,
    });
  }

  return rows;
}

async function main() {
  await withIngestionRun(
    {
      source: "google_sheets",
      pipeline: "sheets_cashflow_inventory_2026",
      metadata: {
        sheet_id: SHEET_ID,
        tab_name: TAB_NAME,
        range_a1: RANGE_A1,
        plan_year: PLAN_YEAR,
        model_version: MODEL_VERSION,
        started_at: nowIso(),
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

      // Parse all blocks
      const thmRows = parseTHM(values, mh);
      const brushRows = parseSimple(values, mh, {
        startInvLabel: "Brush Starting Inventory",
        supplyItemKey: "BRUSH",
      });
      const rollerRows = parseSimple(values, mh, {
        startInvLabel: "Rollers Starting Inventory",
        supplyItemKey: "ROLLER",
      });

      const allRows = [...thmRows, ...brushRows, ...rollerRows];

      const client = await pool.connect();
      ctx.setClient(client);

      try {
        await client.query("begin");

        // Snapshot (audit/debug)
        await upsertSnapshot(client, {
          sheetKey: SHEET_KEY,
          tabName: TAB_NAME,
          rangeA1: RANGE_A1,
          values,
        });
        ctx.incrementUpserts(1);

        // Upsert facts
        for (const r of allRows) {
          await upsertDemandForecastSupplyItem(client, r);
          ctx.incrementUpserts(1);
        }

        await client.query("commit");
      } catch (err) {
        try {
          await client.query("rollback");
        } catch (_) {}
        throw err;
      } finally {
        ctx.setClient(null);
        client.release();
      }
    }
  );
}

main().catch((e) => {
  console.error("Fatal:", e.message);
  process.exitCode = 1;
});