const db = require("./db");

class DatabaseIdempotencyStore {
  async isProcessed(commandId) {
    if (!commandId) {
      return false;
    }

    const result = await db.query(
      `
      SELECT command_id
      FROM idempotency_keys
      WHERE command_id = $1
        AND status = 'success'
      LIMIT 1
      `,
      [commandId]
    );

    return result.rowCount > 0;
  }

  async get(commandId) {
    if (!commandId) {
      return null;
    }

    const result = await db.query(
      `
      SELECT
        command_id AS "commandId",
        status,
        processed_at AS "processedAt",
        last_error AS "lastError"
      FROM idempotency_keys
      WHERE command_id = $1
      LIMIT 1
      `,
      [commandId]
    );

    return result.rows[0] || null;
  }

  async markProcessed(commandId, payload = {}) {
    if (!commandId) {
      throw new Error("commandId is required");
    }

    const result = await db.query(
      `
      INSERT INTO idempotency_keys (command_id, status, processed_at, last_error)
      VALUES ($1, 'success', NOW(), NULL)
      ON CONFLICT (command_id)
      DO UPDATE SET
        status = 'success',
        processed_at = NOW(),
        last_error = NULL
      RETURNING
        command_id AS "commandId",
        status,
        processed_at AS "processedAt",
        last_error AS "lastError"
      `,
      [commandId]
    );

    return {
      ...result.rows[0],
      ...payload
    };
  }

  async markFailed(commandId, errorMessage) {
    if (!commandId) {
      return null;
    }

    const result = await db.query(
      `
      INSERT INTO idempotency_keys (command_id, status, processed_at, last_error)
      VALUES ($1, 'failed', NOW(), $2)
      ON CONFLICT (command_id)
      DO UPDATE SET
        status = 'failed',
        processed_at = NOW(),
        last_error = EXCLUDED.last_error
      RETURNING
        command_id AS "commandId",
        status,
        processed_at AS "processedAt",
        last_error AS "lastError"
      `,
      [commandId, errorMessage || null]
    );

    return result.rows[0];
  }
}

module.exports = {
  DatabaseIdempotencyStore
};
