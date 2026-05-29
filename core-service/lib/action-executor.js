const config = require("./config");
const { nowIso, stableHash } = require("./utils");

class ActionExecutor {
  constructor({ producer, blacklistStore, replyGuardStore }) {
    this.producer = producer;
    this.blacklistStore = blacklistStore;
    this.replyGuardStore = replyGuardStore;
  }

  async execute(event, decision) {
    const results = [];
    const actions = Array.isArray(decision.actions) ? decision.actions : [];

    for (const action of actions) {
      const result = await this.executeAction(event, action, decision);
      results.push(result);
    }

    return results;
  }

  async executeAction(event, action, decision) {
    switch (action.type) {
      case "hide_comment":
      case "reply_comment":
        return this.publishReplyCommand(event, action, decision);
      case "blacklist_user":
        return this.blacklistUser(event, action);
      case "manual_review":
        return this.publishAction(event, action, {
          status: "queued",
          detail: "Waiting for human review"
        });
      default:
        return this.publishAction(event, action, {
          status: "skipped",
          detail: `Unsupported action type: ${action.type}`
        });
    }
  }

  buildCommandId(event, action) {
    const base = {
      eventId: event.eventId,
      commentId: event.commentId || null,
      action: action.type,
      message: action.message || null
    };

    return `cmd_${stableHash(base).slice(0, 24)}`;
  }

  mapBackendAction(action) {
    if (action.type === "reply_comment") {
      return "reply_comment";
    }

    if (action.type === "hide_comment") {
      return "hide_comment";
    }

    return action.type;
  }

  async publishReplyCommand(event, action, decision) {
    if ((action.type === "reply_comment" || action.type === "hide_comment") && !event.commentId) {
      return this.publishAction(event, action, {
        status: "skipped",
        detail: "No commentId available"
      });
    }

    if (action.type === "reply_comment") {
      if (!action.message) {
        return this.publishAction(event, action, {
          status: "skipped",
          detail: "Missing reply message"
        });
      }

      // Chặn bot tự trả lời lặp lại ngay tại core-service.
      // backend-api vẫn có idempotency bằng command_id để chống Kafka redeliver.
      if (this.replyGuardStore) {
        const reservation = this.replyGuardStore.reserve(event.commentId, {
          eventId: event.eventId,
          userId: event.fromUserId || null,
          message: action.message
        });

        if (reservation && !reservation.reserved) {
          return this.publishAction(event, action, {
            status: "duplicate_skipped",
            detail: {
              reason: "comment_already_auto_replied",
              previous: reservation.record
            }
          });
        }
      }
    }

    const commandId = this.buildCommandId(event, action);
    const command = {
      schema_version: 1,
      command_id: commandId,
      commandId,
      event_id: event.eventId,
      eventId: event.eventId,
      source: "core-service",
      action: this.mapBackendAction(action),
      target: {
        page_id: event.pageId || null,
        pageId: event.pageId || null,
        post_id: event.postId || null,
        postId: event.postId || null,
        comment_id: event.commentId || null,
        commentId: event.commentId || null
      },
      reply_text: action.message || null,
      replyText: action.message || null,
      intent: decision && decision.classification ? decision.classification.intent : undefined,
      sentiment: decision && decision.classification ? decision.classification.sentiment : undefined,
      reason: action.reason || null,
      retry_count: 0,
      retryCount: 0,
      created_at: nowIso(),
      createdAt: nowIso(),
      original_event: event,
      original_action: action
    };

    await this.producer.send({
      topic: config.topics.replyCommands,
      acks: -1,
      messages: [
        {
          key: commandId,
          value: JSON.stringify(command)
        }
      ]
    });

    if (action.type === "reply_comment" && this.replyGuardStore) {
      this.replyGuardStore.mark(event.commentId, {
        status: "command_published",
        commandId,
        publishedAt: nowIso()
      });
    }

    return this.publishAction(event, action, {
      status: "command_published",
      detail: {
        commandId,
        topic: config.topics.replyCommands
      }
    });
  }

  async blacklistUser(event, action) {
    const record = this.blacklistStore.add(event.fromUserId, {
      reason: action.reason || "automation_rule",
      lastEventId: event.eventId
    });

    return this.publishAction(event, action, {
      status: record ? "stored" : "skipped",
      detail: record || "No fromUserId available"
    });
  }

  async publishAction(event, action, result) {
    const payload = {
      eventId: event.eventId,
      commentId: event.commentId,
      userId: event.fromUserId,
      action,
      result,
      processedAt: nowIso()
    };

    await this.producer.send({
      topic: config.topics.moderationActions,
      messages: [
        {
          key: event.eventId,
          value: JSON.stringify(payload)
        }
      ]
    });

    return payload;
  }
}

module.exports = {
  ActionExecutor
};
