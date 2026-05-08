const axios = require("axios");
const config = require("./config");
const { createRetryableError, isRetryableError } = require("./utils");

class ActionExecutor {
  constructor({ producer, blacklistStore }) {
    this.producer = producer;
    this.blacklistStore = blacklistStore;
  }

  async execute(event, decision) {
    const results = [];

    for (const action of decision.actions) {
      const result = await this.executeAction(event, action);
      results.push(result);
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

    if (config.facebook.simulateActions) {
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

      return this.publishAction(event, action, {
        status: "sent",
        detail: response.data
      });
    } catch (error) {
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
