const db = require("./db");

function toInt(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function computeSpamScore(processedPayload) {
  const spamAnalysis = processedPayload && processedPayload.spamAnalysis;
  if (!spamAnalysis) return 0;

  if (Number.isFinite(Number(spamAnalysis.score))) {
    return Number(spamAnalysis.score);
  }

  const reasons = Array.isArray(spamAnalysis.reasons) ? spamAnalysis.reasons.length : 0;
  const signals = spamAnalysis.signals && typeof spamAnalysis.signals === "object"
    ? Object.values(spamAnalysis.signals).filter(Boolean).length
    : 0;

  return Math.max(reasons, signals);
}

function mapCoreStatus(processedPayload) {
  if (!processedPayload) return "processed";

  if (processedPayload.status === "action_failed") return "failed";
  if (processedPayload.action === "send_to_support") return "pending_review";
  if (processedPayload.action === "hide_comment" || processedPayload.action === "hide_and_review") return "hide_queued";
  if (processedPayload.action === "blacklist_user") return "blacklisted";
  if (processedPayload.action === "auto_reply" || processedPayload.action === "reply_comment") return "reply_queued";
  if (processedPayload.status === "ignored") return "ignored";

  return "processed";
}

class CommentStore {
  async upsertReceived(event) {
    if (!event || !event.eventId) return null;

    const result = await db.query(
      `
      INSERT INTO comments (
        event_id,
        comment_id,
        post_id,
        page_id,
        from_user_id,
        message,
        normalized_message,
        status,
        created_at,
        updated_at
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, 'received', NOW(), NOW())
      ON CONFLICT (event_id)
      DO UPDATE SET
        comment_id = COALESCE(EXCLUDED.comment_id, comments.comment_id),
        post_id = COALESCE(EXCLUDED.post_id, comments.post_id),
        page_id = COALESCE(EXCLUDED.page_id, comments.page_id),
        from_user_id = COALESCE(EXCLUDED.from_user_id, comments.from_user_id),
        message = COALESCE(EXCLUDED.message, comments.message),
        normalized_message = COALESCE(EXCLUDED.normalized_message, comments.normalized_message),
        status = CASE
          WHEN comments.status IN ('replied', 'hidden', 'failed', 'dead_letter') THEN comments.status
          ELSE 'received'
        END,
        updated_at = NOW()
      RETURNING *
      `,
      [
        event.eventId,
        event.commentId || null,
        event.postId || null,
        event.pageId || null,
        event.fromUserId || null,
        event.message || null,
        event.normalizedMessage || null
      ]
    );

    await this.log(event.eventId, "core-service", "received", "Comment event received from raw_events");
    return result.rows[0];
  }

  async upsertProcessed(processedPayload) {
    if (!processedPayload || !processedPayload.eventId) return null;

    const status = mapCoreStatus(processedPayload);
    const result = await db.query(
      `
      INSERT INTO comments (
        event_id,
        comment_id,
        post_id,
        page_id,
        from_user_id,
        message,
        normalized_message,
        intent,
        sentiment,
        spam_score,
        action,
        status,
        created_at,
        updated_at
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW(), NOW())
      ON CONFLICT (event_id)
      DO UPDATE SET
        comment_id = COALESCE(EXCLUDED.comment_id, comments.comment_id),
        post_id = COALESCE(EXCLUDED.post_id, comments.post_id),
        page_id = COALESCE(EXCLUDED.page_id, comments.page_id),
        from_user_id = COALESCE(EXCLUDED.from_user_id, comments.from_user_id),
        message = COALESCE(EXCLUDED.message, comments.message),
        normalized_message = COALESCE(EXCLUDED.normalized_message, comments.normalized_message),
        intent = EXCLUDED.intent,
        sentiment = EXCLUDED.sentiment,
        spam_score = EXCLUDED.spam_score,
        action = EXCLUDED.action,
        status = EXCLUDED.status,
        updated_at = NOW()
      RETURNING *
      `,
      [
        processedPayload.eventId,
        processedPayload.commentId || null,
        processedPayload.postId || null,
        processedPayload.pageId || null,
        processedPayload.userId || null,
        processedPayload.message || null,
        processedPayload.normalizedMessage || null,
        processedPayload.intent || null,
        processedPayload.sentiment || null,
        toInt(computeSpamScore(processedPayload), 0),
        processedPayload.action || null,
        status
      ]
    );

    await this.log(
      processedPayload.eventId,
      "core-service",
      status,
      `intent=${processedPayload.intent || 'unknown'}; action=${processedPayload.action || 'unknown'}`
    );
    return result.rows[0];
  }

  async markIgnored(event, reason) {
    if (!event || !event.eventId) return null;

    await this.upsertReceived(event);
    const result = await db.query(
      `
      UPDATE comments
      SET status = 'ignored', action = COALESCE(action, 'ignored'), updated_at = NOW()
      WHERE event_id = $1
      RETURNING *
      `,
      [event.eventId]
    );

    await this.log(event.eventId, "core-service", "ignored", reason || "Event ignored");
    return result.rows[0] || null;
  }

  async markFailed(event, error) {
    if (!event || !event.eventId) return null;

    await this.upsertReceived(event);
    const message = error && error.message ? error.message : String(error || "Unknown error");
    const result = await db.query(
      `
      UPDATE comments
      SET status = 'failed', updated_at = NOW()
      WHERE event_id = $1
      RETURNING *
      `,
      [event.eventId]
    );

    await this.log(event.eventId, "core-service", "failed", message);
    return result.rows[0] || null;
  }

  async log(eventId, serviceName, status, message) {
    await db.query(
      `
      INSERT INTO processing_logs (event_id, service_name, status, message, created_at)
      VALUES ($1, $2, $3, $4, NOW())
      `,
      [eventId || null, serviceName, status, message || null]
    );
  }
}

module.exports = {
  CommentStore
};
