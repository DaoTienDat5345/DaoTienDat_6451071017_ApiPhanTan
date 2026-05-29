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
  kafka: {
    connectionTimeoutMs: readNumber(process.env.KAFKA_CONNECTION_TIMEOUT_MS, 10000),
    requestTimeoutMs: readNumber(process.env.KAFKA_REQUEST_TIMEOUT_MS, 30000),
    initialRetryTimeMs: readNumber(process.env.KAFKA_INITIAL_RETRY_TIME_MS, 300),
    retries: readNumber(process.env.KAFKA_RETRIES, 8),
    replicationFactor: readNumber(process.env.KAFKA_REPLICATION_FACTOR, 1),
    defaultTopicPartitions: readNumber(process.env.KAFKA_DEFAULT_TOPIC_PARTITIONS, 3),
    topicPartitions: {
      [process.env.KAFKA_TOPIC || "raw_events"]: readNumber(process.env.KAFKA_RAW_EVENTS_PARTITIONS, 6),
      [process.env.KAFKA_STATUS_TOPIC || "processing_status"]: readNumber(process.env.KAFKA_STATUS_PARTITIONS, 3),
      [process.env.KAFKA_ACTION_TOPIC || "moderation_actions"]: readNumber(process.env.KAFKA_ACTION_PARTITIONS, 3),
      [process.env.KAFKA_REPLY_COMMANDS_TOPIC || "reply_commands"]: readNumber(process.env.KAFKA_REPLY_COMMANDS_PARTITIONS, 3),
      [process.env.KAFKA_SEND_RETRY_TOPIC || "send_retry"]: readNumber(process.env.KAFKA_SEND_RETRY_PARTITIONS, 3),
      [process.env.KAFKA_SEND_FAILED_TOPIC || "send_failed"]: readNumber(process.env.KAFKA_SEND_FAILED_PARTITIONS, 3),
      [process.env.KAFKA_DEAD_LETTER_TOPIC || "dead_letter"]: readNumber(process.env.KAFKA_DEAD_LETTER_PARTITIONS, 3),
      [process.env.KAFKA_PROCESSED_TOPIC || "processed_events"]: readNumber(process.env.KAFKA_PROCESSED_PARTITIONS, 3)
    }
  },
  consumer: {
    partitionsConsumedConcurrently: readNumber(process.env.CONSUMER_PARTITIONS_CONCURRENTLY, 3),
    sessionTimeoutMs: readNumber(process.env.CONSUMER_SESSION_TIMEOUT_MS, 30000),
    heartbeatIntervalMs: readNumber(process.env.CONSUMER_HEARTBEAT_INTERVAL_MS, 3000),
    maxBytesPerPartition: readNumber(process.env.CONSUMER_MAX_BYTES_PER_PARTITION, 1048576),
    maxBytes: readNumber(process.env.CONSUMER_MAX_BYTES, 10485760),
    maxWaitTimeInMs: readNumber(process.env.CONSUMER_MAX_WAIT_MS, 500),
    maxBatchMessages: readNumber(process.env.CONSUMER_MAX_BATCH_MESSAGES, 100),
    commitEveryMessages: readNumber(process.env.CONSUMER_COMMIT_EVERY_MESSAGES, 10)
  },
  port: readNumber(process.env.PORT, 9001),
  backendPort: readNumber(process.env.BACKEND_PORT, 3000),
  verifyToken: process.env.VERIFY_TOKEN,
  kafkaBroker: process.env.KAFKA_BROKER || "localhost:9092",
  kafkaClientId: process.env.KAFKA_CLIENT_ID || "webhook-service",
  topics: {
    rawEvents: process.env.KAFKA_TOPIC || "raw_events",
    processingStatus: process.env.KAFKA_STATUS_TOPIC || "processing_status",
    moderationActions: process.env.KAFKA_ACTION_TOPIC || "moderation_actions",
    replyCommands: process.env.KAFKA_REPLY_COMMANDS_TOPIC || "reply_commands",
    sendRetry: process.env.KAFKA_SEND_RETRY_TOPIC || "send_retry",
    sendFailed: process.env.KAFKA_SEND_FAILED_TOPIC || "send_failed",
    deadLetter: process.env.KAFKA_DEAD_LETTER_TOPIC || "dead_letter",
    processedEvents: process.env.KAFKA_PROCESSED_TOPIC || "processed_events"
  },
  consumerGroupId: process.env.KAFKA_CONSUMER_GROUP_ID || "core-service-group",
  consumerFromBeginning: readBoolean(process.env.KAFKA_FROM_BEGINNING, false),
  retryGroupId: process.env.KAFKA_RETRY_GROUP_ID || "retry-service-group",
  backendGroupId: process.env.KAFKA_BACKEND_GROUP_ID || "backend-api-group",
  retry: {
    maxAttempts: readNumber(process.env.RETRY_MAX_ATTEMPTS, 3),
    baseDelayMs: readNumber(process.env.RETRY_BASE_DELAY_MS, 1000)
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
    appSecret: process.env.FACEBOOK_APP_SECRET,
    graphVersion: process.env.FACEBOOK_GRAPH_VERSION || "v25.0",
    simulateActions: readBoolean(process.env.SIMULATE_ACTIONS, !process.env.PAGE_ACCESS_TOKEN),
    pageId: process.env.PAGE_ID,
    timeoutMs: readNumber(process.env.FACEBOOK_TIMEOUT_MS, 10000)
  },
  circuitBreaker: {
    failureThreshold: readNumber(process.env.CIRCUIT_BREAKER_FAILURE_THRESHOLD, 5),
    openTimeoutMs: readNumber(process.env.CIRCUIT_BREAKER_OPEN_TIMEOUT_MS, 30000)
  }
};
