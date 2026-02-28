"use strict";

const path = require("path");
require("dotenv").config({ path: path.resolve(__dirname, "../../.env") });

const { google } = require("googleapis");
const { pool } = require("../../config/db");
const { withIngestionRun } = require("../lib/ingestionRun"); 

const SHEET_ID = process.env.SHEET_ID; 
const TAB_NAME = process.env.GSHEET_TAB_UNIT_ECON || "Unit Economics Master Reference";
const SHEET_KEY = process.env.GSHEET_KEY || "cashflow_main";
const RANGE_A1 = process.env.GSHEET_RANGE_UNIT_ECON || `'${TAB_NAME}'!A1:Z400`;

const SA_EMAIL = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL;
const SA_KEY = (process.env.GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY|| "").replace(/\\n/g, "\n");

if (!SHEET_ID) throw new Error("Missing SHEET_ID in env");
if (!SA_EMAIL) throw new Error("Missing GOOGLE_SERVICE_ACCOUNT_EMAIL in env");
if (!SA_KEY) throw new Error("Missing GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY in env");

function nowIso() {
  return new Date().toISOString();
}

function normalizeText(x) {
  return String(x ?? "")
    .replace(/\u00A0/g, " ")
    .trim();
}

function canonOfferKey(name) {
  const n = normalizeText(name).toLowerCase();

  // handle tiny label drift
  if (n === "thm 1pc" || n === "thm 1 pc" || n === "thm 1-pack" || n === "thm 1 pack") return "THM_1PC";
  if (n.includes("thm") && n.includes("3") && n.includes("roller") && !n.includes("sub")) return "THM_3PC_PLUS_ROLLER";
  if (n.includes("thm") && n.includes("3") && n.includes("sub")) return "THM_3PC_SUB";
  if (n.includes("dermaroller")) return "DERMAROLLER";
  if (n === "brush" || n.includes("brush")) return "BRUSH";

  // fallback: safe deterministic key
  return normalizeText(name)
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function parseMoneyToNumber(x) {
  const s = normalizeText(x);
  if (!s) return null;
  // remove $ and commas and spaces, keep minus + dot
  const cleaned = s.replace(/[$,]/g, "").replace(/\s+/g, "");
  const n = Number(cleaned);
  return Number.isFinite(n) ? n : null;
}

function findRowIndex(values, predicate) {
  for (let i = 0; i < values.length; i++) {
    const row = values[i] || [];
    for (let j = 0; j < row.length; j++) {
      if (predicate(normalizeText(row[j]))) return i;
    }
  }
  return -1;
}


function readOfferMoneyTable(values, headerText) {
  const headerIdx = findRowIndex(values, (cell) => cell.toLowerCase() === headerText.toLowerCase());
  if (headerIdx < 0) return [];

  const out = [];
  for (let r = headerIdx + 1; r < values.length; r++) {
    const row = values[r] || [];
    const name = normalizeText(row[0]);
    const val = row[1];

    // stop when table ends
    if (!name) break;

    const money = parseMoneyToNumber(val);
    out.push({ offer_name: name, value_num: money });
  }
  return out;
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

async function upsertOfferEconomics(client, { offerKey, offerName, unitSellPrice, unitCogs }) {
  const isSub = offerKey.includes("SUB");
  const isBundle = offerKey.includes("3PC") && offerKey.includes("ROLLER");

  const q = `
    insert into ops.dim_offer_economics
      (offer_key, offer_name, unit_sell_price, unit_cogs, is_subscription, is_bundle, source, ingested_at)
    values
      ($1, $2, $3, $4, $5, $6, 'google_sheets', now())
    on conflict (offer_key) do update set
      offer_name = excluded.offer_name,
      unit_sell_price = excluded.unit_sell_price,
      unit_cogs = excluded.unit_cogs,
      is_subscription = excluded.is_subscription,
      is_bundle = excluded.is_bundle,
      source = excluded.source,
      ingested_at = now()
  `;
  await client.query(q, [offerKey, offerName, unitSellPrice, unitCogs, isSub, isBundle]);
}

async function upsertAssumption(client, { key, valueNum, valueText, unit }) {
  const q = `
    insert into ops.dim_econ_assumptions
      (key, value_num, value_text, unit, source, ingested_at)
    values
      ($1, $2, $3, $4, 'google_sheets', now())
    on conflict (key) do update set
      value_num = excluded.value_num,
      value_text = excluded.value_text,
      unit = excluded.unit,
      source = excluded.source,
      ingested_at = now()
  `;
  await client.query(q, [key, valueNum ?? null, valueText ?? null, unit ?? null]);
}


async function upsertReorderRule(client, { supplyItemKey, leadTimeMonths, safetyBufferMonths, standardOrderQty, minRunwayTriggerMonths }) {
  const reorderTriggerMonths = Number(leadTimeMonths) + Number(safetyBufferMonths);

  const q = `
    insert into ops.dim_reorder_rules
      (supply_item_key, lead_time_months, safety_buffer_months, reorder_trigger_months, standard_order_qty, min_runway_trigger_months, source, ingested_at)
    values
      ($1, $2, $3, $4, $5, $6, 'google_sheets', now())
    on conflict (supply_item_key) do update set
      lead_time_months = excluded.lead_time_months,
      safety_buffer_months = excluded.safety_buffer_months,
      reorder_trigger_months = excluded.reorder_trigger_months,
      standard_order_qty = excluded.standard_order_qty,
      min_runway_trigger_months = excluded.min_runway_trigger_months,
      source = excluded.source,
      ingested_at = now()
  `;
  await client.query(q, [
    supplyItemKey,
    leadTimeMonths,
    safetyBufferMonths,
    reorderTriggerMonths,
    standardOrderQty,
    minRunwayTriggerMonths ?? null,
  ]);
}

function parseReorderSettings(values) {
  const headerIdx = findRowIndex(values, (cell) =>
    cell.toLowerCase().includes("reorder")
  );
  if (headerIdx < 0) return [];

  let currentKey = null;
  const groups = {};

  for (let r = headerIdx + 1; r < values.length; r++) {
    const row = values[r] || [];
    const a = normalizeText(row[0]);
    const b = normalizeText(row[1]);

    if (!a && !b) continue;

    if (a.toUpperCase().includes("UNIT COST")) break;

    const supplyMatch = a.match(/^(THM|Roller|Brush)\s+Lead Time/i);
    if (supplyMatch) {
      const raw = supplyMatch[1].toLowerCase();
      currentKey =
        raw === "roller" ? "ROLLER"
        : raw === "brush" ? "BRUSH"
        : "THM";

      groups[currentKey] = groups[currentKey] || {};
      groups[currentKey].lead_time_months =
        parseMoneyToNumber(b) ??
        Number(String(b).replace(/[^0-9.-]/g, ""));

      continue;
    }

    if (!currentKey) continue;

    const field = a.toLowerCase();
    const num =
      parseMoneyToNumber(b) ??
      Number(String(b).replace(/[^0-9.-]/g, ""));

    if (field.includes("safety buffer")) {
      groups[currentKey].safety_buffer_months = num;
    } else if (field.includes("standard order")) {
      groups[currentKey].standard_order_qty = num;
    } else if (field.includes("min runway")) {
      groups[currentKey].min_runway_trigger_months = num;
    }
  }

  return Object.entries(groups).map(([supply_item_key, g]) => ({
    supply_item_key,
    lead_time_months: g.lead_time_months ?? null,
    safety_buffer_months: g.safety_buffer_months ?? null,
    standard_order_qty: g.standard_order_qty ?? null,
    min_runway_trigger_months: g.min_runway_trigger_months ?? null,
  }));
}

async function main() {
  await withIngestionRun(
    {
      source: "google_sheets",
      pipeline: "sheets_unit_econ_master_reference",
      metadata: {
        sheet_id: SHEET_ID,
        tab_name: TAB_NAME,
        range_a1: RANGE_A1,
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
      const client = await pool.connect();
      ctx.setClient(client);

      try {
        // Snapshot first (audit/debug)
        await upsertSnapshot(client, {
          sheetKey: SHEET_KEY,
          tabName: TAB_NAME,
          rangeA1: RANGE_A1,
          values,
        });
        ctx.incrementUpserts(1);

        // Parse offer tables
        const sell = readOfferMoneyTable(values, "UNIT SELL PRICE");
        const cogs = readOfferMoneyTable(values, "UNIT COG");

        // Merge by canonical key
        const map = new Map();
        for (const x of sell) {
          const k = canonOfferKey(x.offer_name);
          map.set(k, { offer_key: k, offer_name: x.offer_name, unit_sell_price: x.value_num ?? null, unit_cogs: null });
        }
        for (const x of cogs) {
          const k = canonOfferKey(x.offer_name);
          const cur = map.get(k) || { offer_key: k, offer_name: x.offer_name, unit_sell_price: null, unit_cogs: null };
          cur.unit_cogs = x.value_num ?? null;
          map.set(k, cur);
        }

        // Upsert offers
        for (const o of map.values()) {
          await upsertOfferEconomics(client, {
            offerKey: o.offer_key,
            offerName: o.offer_name,
            unitSellPrice: o.unit_sell_price,
            unitCogs: o.unit_cogs,
          });
          ctx.incrementUpserts(1);
        }
        
        const flat = values.flat().map(normalizeText);

        function findNumberAfterLabel(label) {
          const idx = flat.findIndex((x) => x.toLowerCase() === label.toLowerCase());
          if (idx < 0) return null;
          return parseMoneyToNumber(flat[idx + 1]) ?? Number(flat[idx + 1]);
        }

        const shippingCost = findNumberAfterLabel("Shipping Cost per Order");
        if (shippingCost != null && Number.isFinite(shippingCost)) {
          await upsertAssumption(client, {
            key: "shipping_cost_per_order",
            valueNum: shippingCost,
            unit: "USD",
          });
          ctx.incrementUpserts(1);
        }

        const returnRate = findNumberAfterLabel("Return Rate");
        if (returnRate != null && Number.isFinite(returnRate)) {
          // might be 2% or 0.02 depending; store as fraction if > 1 treat as percent
          const frac = returnRate > 1 ? returnRate / 100 : returnRate;
          await upsertAssumption(client, {
            key: "return_rate",
            valueNum: frac,
            unit: "fraction",
          });
          ctx.incrementUpserts(1);
        }

        const aov = findNumberAfterLabel("AOV");
        if (aov != null && Number.isFinite(aov)) {
          await upsertAssumption(client, {
            key: "aov",
            valueNum: aov,
            unit: "USD",
          });
          ctx.incrementUpserts(1);
        }

        const avgThmPerOrder = findNumberAfterLabel("Avg THM per Order");
        if (avgThmPerOrder != null && Number.isFinite(avgThmPerOrder)) {
          await upsertAssumption(client, {
            key: "avg_thm_per_order",
            valueNum: avgThmPerOrder,
            unit: "units",
          });
          ctx.incrementUpserts(1);
        }
        const rules = parseReorderSettings(values);
        for (const r of rules) {
        if (!r.supply_item_key) continue;
        if (r.lead_time_months == null || r.safety_buffer_months == null || r.standard_order_qty == null) continue;

        await upsertReorderRule(client, {
            supplyItemKey: r.supply_item_key,
            leadTimeMonths: r.lead_time_months,
            safetyBufferMonths: r.safety_buffer_months,
            standardOrderQty: r.standard_order_qty,
            minRunwayTriggerMonths: r.min_runway_trigger_months,
        });
        ctx.incrementUpserts(1);
        }

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