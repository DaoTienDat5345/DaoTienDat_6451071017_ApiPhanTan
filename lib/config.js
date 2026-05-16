require("dotenv").config();

function readNumber(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function readBoolean(value, fallback = false) {
  if (value === undefined) {
    return fallback;
  }

  return ["1", "true", "yes", "on"].includes(String(value).toLowerCase());
}

module.exports = {
  port: readNumber(process.env.PORT, 9001),
  verifyToken: process.env.VERIFY_TOKEN,
  kafkaBroker: process.env.KAFKA_BROKER || "localhost:9092",
  kafkaClientId: process.env.KAFKA_CLIENT_ID || "webhook-service",
  topics: {
    rawEvents: process.env.KAFKA_TOPIC || "raw_events",
    processingStatus: process.env.KAFKA_STATUS_TOPIC || "processing_status",
    moderationActions: process.env.KAFKA_ACTION_TOPIC || "moderation_actions",
    sendFailed: process.env.KAFKA_SEND_FAILED_TOPIC || "send_failed",
    deadLetter: process.env.KAFKA_DEAD_LETTER_TOPIC || "dead_letter",
    processedEvents: process.env.KAFKA_PROCESSED_TOPIC || "processed_events"
  },
  consumerGroupId: process.env.KAFKA_CONSUMER_GROUP_ID || "core-service-group",
  consumerFromBeginning: readBoolean(process.env.KAFKA_FROM_BEGINNING, false),
  retryGroupId: process.env.KAFKA_RETRY_GROUP_ID || "retry-service-group",
  retry: {
    maxAttempts: readNumber(process.env.RETRY_MAX_ATTEMPTS, 3),
    baseDelayMs: readNumber(process.env.RETRY_BASE_DELAY_MS, 30000)
  },
  ai: {
    provider: process.env.AI_PROVIDER || "mock",
    apiKey: process.env.AI_API_KEY,
    model: process.env.AI_MODEL || "gpt-4o-mini",
    baseUrl: process.env.AI_BASE_URL || "https://api.openai.com/v1",
    timeoutMs: readNumber(process.env.AI_TIMEOUT_MS, 15000)
  },
  facebook: {
    pageAccessToken: process.env.PAGE_ACCESS_TOKEN,
    graphVersion: process.env.FACEBOOK_GRAPH_VERSION || "v25.0",
    simulateActions: readBoolean(process.env.SIMULATE_ACTIONS, !process.env.PAGE_ACCESS_TOKEN)
  }
};
