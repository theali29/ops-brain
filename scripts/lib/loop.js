"use strict";

const axios = require("axios");

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function nowIso() {
  return new Date().toISOString();
}

const token = process.env.LOOP_ADMIN_TOKEN;
const version = process.env.LOOP_API_VERSION || "2023-10";
const baseURL = process.env.LOOP_BASE_URL || "https://api.loopsubscriptions.com";
const timeoutMs = Number(process.env.LOOP_TIMEOUT_MS || 60000);
const rps = Number(process.env.LOOP_RPS || 3);

function createLoopClient() {
  if (!token) throw new Error("Missing Loop Admin Token");

  const api = axios.create({
    baseURL: `${baseURL}/admin/${version}`,
    headers: {
      "X-Loop-Token": token,
      accept: "application/json",
    },
    timeout: timeoutMs,
    validateStatus: () => true,
  });

  let lastRequestAt = 0;

  async function requestWithRetry(method, url, { params, data } = {}, opts = {}) {
    const maxAttempts = opts.maxAttempts ?? 8;

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      const minGapMs = Math.ceil(1000 / Math.max(1, rps));
      const elapsed = Date.now() - lastRequestAt;
      if (elapsed < minGapMs) await sleep(minGapMs - elapsed);
      lastRequestAt = Date.now();

      const res = await api.request({ method, url, params, data });

      if (res.status >= 200 && res.status < 300) return res;

      if (res.status === 429) {
        const retryAfter = Number(res.headers["retry-after"] || 1);
        const waitMs = Math.max(1000, retryAfter * 1000);
        console.warn(`Loop 429 rate-limited. Wait ${waitMs}ms (attempt ${attempt}/${maxAttempts})`);
        await sleep(waitMs);
        continue;
      }

      if (res.status >= 500 && res.status <= 599) {
        const backoff = Math.min(30000, 500 * 2 ** (attempt - 1));
        console.warn(`Loop ${res.status} server error. Backoff ${backoff}ms (attempt ${attempt}/${maxAttempts})`);
        await sleep(backoff);
        continue;
      }

      const body = typeof res.data === "string" ? res.data : JSON.stringify(res.data);
      throw new Error(`Loop HTTP ${res.status}: ${body}`);
    }

    throw new Error(`Loop request failed after ${maxAttempts} attempts`);
  }

  async function getPaged(path, opts = {}) {
    const {
      pageNo = 1,
      pageSize = 50,
      params = {},
      ...looseParams
    } = opts || {};

    const merged = { ...looseParams, ...params, pageNo, pageSize };
    Object.keys(merged).forEach((k) => merged[k] === undefined && delete merged[k]);

    const res = await requestWithRetry("GET", path, { params: merged });

    const payload = res.data || {};
    const dataArr = Array.isArray(payload.data) ? payload.data : [];
    const pageInfo = payload.pageInfo || { hasNextPage: false, hasPreviousPage: false };

    return { data: dataArr, pageInfo, raw: payload };
  }

  return { requestWithRetry, getPaged, nowIso };
}

function epochToIso(epoch) {
  if (epoch == null) return null;
  const n = Number(epoch);
  if (!Number.isFinite(n) || n <= 0) return null;
  const ms = n > 1e12 ? n : n * 1000;
  const d = new Date(ms);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString();
}

module.exports = { createLoopClient, epochToIso, nowIso };