const db = require("./db");

function extractOriginalEvent(command) {
  return command.original_event || command.originalEvent || command.event || null;
}

function getCommentId(command) {
  return command?.target?.commentId || command?.target?.comment_id || command?.commentId || command?.comment_id || null;
}

function getEventId(command) {
  return command?.eventId || command?.event_id || null;
}

function getPostId(command) {
  return command?.target?.postId || command?.target?.post_id || command?.postId || command?.post_id || null;
}

function getPageId(command) {
  return command?.target?.pageId || command?.target?.page_id || command?.pageId || command?.page_id || null;
}

function mapSuccessStatus(command) {
  switch (command.action) {
    case "reply":
    case "reply_comment":
    case "auto_reply":
      return "replied";
    case "hide":
    case "hide_comment":
      return "hidden";
    case "manual_review":
      return "pending_review";
    case "post":
    case "create_post":
      return "posted";
    default:
      return "processed";
  }
}

class CommentStatusStore {
  async ensureFromCommand(command) {
    const originalEvent = extractOriginalEvent(command);
    const eventId = getEventId(command) || originalEvent?.eventId;
    const commentId = getCommentId(command) || originalEvent?.commentId;

    if (!eventId && !commentId) return null;

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
        action,
        status,
        created_at,
        updated_at
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'backend_received', NOW(), NOW())
      ON CONFLICT (event_id)
      DO UPDATE SET
        comment_id = COALESCE(comments.comment_id, EXCLUDED.comment_id),
        post_id = COALESCE(comments.post_id, EXCLUDED.post_id),
        page_id = COALESCE(comments.page_id, EXCLUDED.page_id),
        from_user_id = COALESCE(comments.from_user_id, EXCLUDED.from_user_id),
        message = COALESCE(comments.message, EXCLUDED.message),
        normalized_message = COALESCE(comments.normalized_message, EXCLUDED.normalized_message),
        intent = COALESCE(comments.intent, EXCLUDED.intent),
        sentiment = COALESCE(comments.sentiment, EXCLUDED.sentiment),
        action = COALESCE(EXCLUDED.action, comments.action),
        updated_at = NOW()
      RETURNING *
      `,
      [
        eventId,
        commentId,
        getPostId(command) || originalEvent?.postId || null,
        getPageId(command) || originalEvent?.pageId || null,
        originalEvent?.fromUserId || null,
        originalEvent?.message || null,
        originalEvent?.normalizedMessage || null,
        command.intent || null,
        command.sentiment || null,
        command.action || null
      ]
    );

    await this.log(eventId, "backend-api", "backend_received", `command=${command.commandId || command.command_id || 'unknown'}`);
    return result.rows[0];
  }

  async markSucceeded(command, facebookResponse) {
    await this.ensureFromCommand(command);
    const eventId = getEventId(command) || extractOriginalEvent(command)?.eventId;
    if (!eventId) return null;

    const status = mapSuccessStatus(command);
    const result = await db.query(
      `
      UPDATE comments
      SET status = $2,
          action = COALESCE($3, action),
          updated_at = NOW()
      WHERE event_id = $1
      RETURNING *
      `,
      [eventId, status, command.action || null]
    );

    await this.log(
      eventId,
      "backend-api",
      status,
      `command=${command.commandId}; facebookResponse=${JSON.stringify(facebookResponse || {})}`
    );
    return result.rows[0] || null;
  }

  async markFailed(command, error) {
    await this.ensureFromCommand(command);
    const eventId = getEventId(command) || extractOriginalEvent(command)?.eventId;
    if (!eventId) return null;

    const message = error && error.message ? error.message : String(error || "Unknown error");
    const result = await db.query(
      `
      UPDATE comments
      SET status = 'failed',
          action = COALESCE($2, action),
          updated_at = NOW()
      WHERE event_id = $1
      RETURNING *
      `,
      [eventId, command.action || null]
    );

    await this.log(eventId, "backend-api", "failed", `command=${command.commandId}; error=${message}`);
    return result.rows[0] || null;
  }

  async markDuplicateSkipped(command, previous) {
    const eventId = getEventId(command) || extractOriginalEvent(command)?.eventId;
    if (!eventId) return null;

    await this.log(
      eventId,
      "backend-api",
      "duplicate_skipped",
      `command=${command.commandId}; previous=${JSON.stringify(previous || {})}`
    );
    return null;
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
  CommentStatusStore
};
