const axios = require("axios");
const config = require("./config");
const { createRetryableError, isRetryableError } = require("./utils");

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
      try {
        const result = await this.executeAction(event, action);
        results.push(result);
      } catch (error) {
        // Retryable errors such as timeout / 5xx should still go through retry-service.
        if (isRetryableError(error)) {
          throw error;
        }

        // Non-retryable Facebook errors such as HTTP 400 should not stop the whole pipeline.
        // We publish a failed moderation action, then processed_events can still record
        // intent/sentiment/action for grading/debugging.
        const details = this.extractAxiosErrorDetails(error);
        const failedPayload = await this.publishAction(event, action, {
          status: "failed",
          stage: error.stage || action.type || "action_execution",
          detail: details
        });
        results.push(failedPayload);
      }
    }

    return results;
  }

  async executeAction(event, action) {
    switch (action.type) {
      case "hide_comment":
        return this.hideComment(event, action);
      case "reply_comment":
        return this.replyToComment(event, action);
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

  async hideComment(event, action) {
    if (!event.commentId) {
      return this.publishAction(event, action, {
        status: "skipped",
        detail: "No commentId available"
      });
    }

    if (config.facebook.simulateActions) {
      return this.publishAction(event, action, {
        status: "simulated",
        detail: "SIMULATE_ACTIONS is enabled"
      });
    }

    const url = `https://graph.facebook.com/${config.facebook.graphVersion}/${event.commentId}`;

    try {
      const response = await axios.post(
        url,
        null,
        {
          params: {
            is_hidden: true,
            access_token: config.facebook.pageAccessToken
          },
          timeout: 10000,
          proxy: false
        }
      );

      return this.publishAction(event, action, {
        status: "sent",
        detail: response.data
      });
    } catch (error) {
      throw this.wrapActionError(error, "hide_comment");
    }
  }

  async replyToComment(event, action) {
    if (!event.commentId || !action.message) {
      return this.publishAction(event, action, {
        status: "skipped",
        detail: "Missing commentId or message"
      });
    }

    // Chốt chống spam quan trọng: mỗi commentId chỉ được auto reply 1 lần.
    // Khi Facebook/Kafka gửi trùng event hoặc service bị restart, record này
    // giúp Page không trả lời lặp lại cùng một bình luận.
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

    if (config.facebook.simulateActions) {
      if (this.replyGuardStore) {
        this.replyGuardStore.mark(event.commentId, {
          status: "simulated",
          simulatedAt: new Date().toISOString()
        });
      }

      return this.publishAction(event, action, {
        status: "simulated",
        detail: action.message
      });
    }

    const url = `https://graph.facebook.com/${config.facebook.graphVersion}/${event.commentId}/comments`;

    try {
      const response = await axios.post(
        url,
        null,
        {
          params: {
            message: action.message,
            access_token: config.facebook.pageAccessToken
          },
          timeout: 10000,
          proxy: false
        }
      );

      if (this.replyGuardStore) {
        this.replyGuardStore.mark(event.commentId, {
          status: "sent",
          sentAt: new Date().toISOString(),
          facebookResponse: response.data || null
        });
      }

      return this.publishAction(event, action, {
        status: "sent",
        detail: response.data
      });
    } catch (error) {
      if (this.replyGuardStore) {
        this.replyGuardStore.mark(event.commentId, {
          status: "send_failed_reserved",
          failedAt: new Date().toISOString(),
          error: error.message
        });
      }

      throw this.wrapActionError(error, "reply_comment");
    }
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

  extractAxiosErrorDetails(error) {
    if (!error) {
      return "Unknown action error";
    }

    if (error.response) {
      return {
        message: error.message,
        httpStatus: error.response.status,
        facebookResponse: error.response.data || null
      };
    }

    return {
      message: error.message,
      code: error.code || null
    };
  }

  wrapActionError(error, stage) {
    if (isRetryableError(error)) {
      return createRetryableError(`Action failed temporarily at stage ${stage}`, {
        cause: error,
        stage
      });
    }

    error.stage = stage;
    return error;
  }

  async publishAction(event, action, result) {
    const payload = {
      eventId: event.eventId,
      commentId: event.commentId,
      userId: event.fromUserId,
      action,
      result,
      processedAt: new Date().toISOString()
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
