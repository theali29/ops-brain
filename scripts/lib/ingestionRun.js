"use strict";

const { pool } = require("../../config/db");

function nowIso() {
  return new Date().toISOString();
}

async function startIngestionRun({ source, pipeline, metadata }) {
  const q = `
    insert into ops.ingestion_runs (source, pipeline, status, metadata)
    values ($1, $2, 'started', $3::jsonb)
    returning id
  `;
  const res = await pool.query(q, [
    source,
    pipeline,
    JSON.stringify(metadata || {}),
  ]);
  return res.rows[0].id;
}



async function finishIngestionRun(
  runId,
  status,
  rowsUpserted,
  rowsDeleted,
  errorMessage,
  metadataPatch
) {
  const q = `
    update ops.ingestion_runs
    set finished_at = now(),
        status = $2::ops.sync_status,
        rows_upserted = $3,
        rows_deleted = $4,
        error_message = $5,
        metadata = metadata || $6::jsonb
    where id = $1
  `;
  await pool.query(q, [
    runId,
    status,
    rowsUpserted || 0,
    rowsDeleted || 0,
    errorMessage || null,
    JSON.stringify(metadataPatch || {}),
  ]);
}

async function withIngestionRun(
  { source, pipeline, metadata },
  handler
) {
  let runId = null;
  let shuttingDown = false;
  let activeClient = null;
  let inTxn = false;
  let rowsUpserted = 0;
  let rowsDeleted = 0;

  async function gracefulShutdown(reason, err) {
    if (shuttingDown) return;
    shuttingDown = true;

    const message = err?.message
      ? `${reason}: ${err.message}`
      : reason;

    console.error(`[${pipeline}] terminating | ${message}`);

    try {
      if (activeClient && inTxn) {
        try { await activeClient.query("ROLLBACK"); } catch (_) {}
        inTxn = false;
      }

      if (runId) {
        await finishIngestionRun(
          runId,
          "failed",
          rowsUpserted,
          rowsDeleted,
          message,
          { finished_at: nowIso(), terminated: true }
        );
      }
    } catch (e) {
      console.error(`[${pipeline}] failed to mark failed: ${e.message}`);
    } finally {
      try { if (activeClient) activeClient.release(); } catch (_) {}
      try { await pool.end(); } catch (_) {}
      process.exit(1);
    }
  }

  process.on("SIGINT", () => gracefulShutdown("SIGINT"));
  process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
  process.on("uncaughtException", (e) => gracefulShutdown("uncaughtException", e));
  process.on("unhandledRejection", (e) => gracefulShutdown("unhandledRejection", e));

  try {
    runId = await startIngestionRun({ source, pipeline, metadata });

    const context = {
      runId,
      pool,
      setClient: (client) => (activeClient = client),
      beginTxn: () => (inTxn = true),
      endTxn: () => (inTxn = false),
      incrementUpserts: (n = 1) => (rowsUpserted += n),
      incrementDeletes: (n = 1) => (rowsDeleted += n),
    };

    await handler(context);

    await finishIngestionRun(
      runId,
      "succeeded",
      rowsUpserted,
      rowsDeleted,
      null,
      { finished_at: nowIso() }
    );

    console.log(`[${pipeline}] succeeded | upserts=${rowsUpserted}`);
  } catch (err) {
    await finishIngestionRun(
      runId,
      "failed",
      rowsUpserted,
      rowsDeleted,
      err.message,
      { finished_at: nowIso() }
    );

    console.error(`[${pipeline}] failed | ${err.message}`);
    process.exitCode = 1;
  } finally {
    try { await pool.end(); } catch (_) {}
  }
}

module.exports = { withIngestionRun };