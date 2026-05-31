const { Pool } = require("pg");

let pool = null;

function getConnectionString() {
  if (process.env.DATABASE_URL) {
    return process.env.DATABASE_URL;
  }

  const host = process.env.POSTGRES_HOST || "postgres";
  const port = process.env.POSTGRES_PORT || "5432";
  const database = process.env.POSTGRES_DB || "fb_api_db";
  const user = process.env.POSTGRES_USER || "fb_api_user";
  const password = process.env.POSTGRES_PASSWORD || "fb_api_password";

  return `postgresql://${encodeURIComponent(user)}:${encodeURIComponent(password)}@${host}:${port}/${database}`;
}

function getPool() {
  if (!pool) {
    pool = new Pool({
      connectionString: getConnectionString(),
      max: Number(process.env.POSTGRES_POOL_MAX || 10),
      idleTimeoutMillis: Number(process.env.POSTGRES_IDLE_TIMEOUT_MS || 30000),
      connectionTimeoutMillis: Number(process.env.POSTGRES_CONNECTION_TIMEOUT_MS || 5000)
    });

    pool.on("error", (error) => {
      console.error("[database] Unexpected PostgreSQL pool error:", error.message);
    });
  }

  return pool;
}

async function query(text, params = []) {
  return getPool().query(text, params);
}

async function checkDatabaseConnection() {
  const result = await query("SELECT NOW() AS now");
  return result.rows[0];
}

async function closeDatabase() {
  if (pool) {
    await pool.end();
    pool = null;
  }
}

module.exports = {
  getPool,
  query,
  checkDatabaseConnection,
  closeDatabase
};
