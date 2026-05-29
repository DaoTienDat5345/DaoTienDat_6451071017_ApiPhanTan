require("dotenv").config();

const crypto = require("crypto");
const express = require("express");
const config = require("./lib/config");
const { normalizeFacebookPayload } = require("./lib/facebook-normalizer");
const { createKafka, ensureTopics, createReliableProducer } = require("./lib/kafka");

const app = express();

app.use(
  express.json({
    limit: "2mb",
    verify: (req, res, buf) => {
      req.rawBody = buf;
    }
  })
);

const kafka = createKafka("webhook-service");
const producer = createReliableProducer(kafka);

app.use((req, res, next) => {
  res.setHeader("ngrok-skip-browser-warning", "true");
  console.log(`[${new Date().toISOString()}] ${req.method} ${req.originalUrl}`);
  next();
});

function verifyFacebookSignature(req, res, next) {
  const appSecret = config.facebook.appSecret;

  // Cho phép test local khi chưa cấu hình App Secret.
  // Khi demo/nộp bài thật, hãy cấu hình FACEBOOK_APP_SECRET để bật kiểm tra HMAC.
  if (!appSecret) {
    console.warn(
      "FACEBOOK_APP_SECRET is empty. Skipping X-Hub-Signature-256 verification."
    );
    return next();
  }

  const signatureHeader = req.get("x-hub-signature-256");

  if (!signatureHeader) {
    console.error("Missing X-Hub-Signature-256 header");
    return res.status(403).json({
      success: false,
      message: "Missing X-Hub-Signature-256 header"
    });
  }

  if (!signatureHeader.startsWith("sha256=")) {
    console.error("Invalid X-Hub-Signature-256 format");
    return res.status(403).json({
      success: false,
      message: "Invalid X-Hub-Signature-256 format"
    });
  }

  if (!req.rawBody) {
    console.error("Raw body is missing. Cannot verify Facebook signature.");
    return res.status(500).json({
      success: false,
      message: "Raw body is missing"
    });
  }

  const expectedSignature =
    "sha256=" +
    crypto
      .createHmac("sha256", appSecret)
      .update(req.rawBody)
      .digest("hex");

  const receivedBuffer = Buffer.from(signatureHeader, "utf8");
  const expectedBuffer = Buffer.from(expectedSignature, "utf8");

  const isValid =
    receivedBuffer.length === expectedBuffer.length &&
    crypto.timingSafeEqual(receivedBuffer, expectedBuffer);

  if (!isValid) {
    console.error("Invalid Facebook webhook signature");
    return res.status(403).json({
      success: false,
      message: "Invalid Facebook webhook signature"
    });
  }

  return next();
}


function webhookHandler(req, res) {
  const mode = req.query["hub.mode"];
  const token = req.query["hub.verify_token"];
  const challenge = req.query["hub.challenge"];

  if (mode === "subscribe" && token === config.verifyToken) {
    console.log("Webhook verified");
    return res.status(200).send(challenge);
  }

  console.error("Webhook verify failed");
  return res.sendStatus(403);
}

async function webhookEventHandler(req, res) {
  try {
    const payload = req.body;
    const normalizedEvents = normalizeFacebookPayload(payload);

    console.log("--- RECEIVED WEBHOOK EVENT ---");
    console.log("Headers:", req.headers);
    console.log("Incoming entries:", normalizedEvents.length);

    await producer.send({
      topic: config.topics.rawEvents,
      acks: -1,
      messages: normalizedEvents.map((event) => ({
        key: event.eventId,
        value: JSON.stringify(event)
      }))
    });

    console.log(
      `Published ${normalizedEvents.length} event(s) to Kafka topic ${config.topics.rawEvents}`
    );

    return res.status(200).json({
      success: true,
      message: "Event received and queued",
      eventCount: normalizedEvents.length
    });
  } catch (error) {
    console.error("Webhook error:", error);
    return res.status(500).json({
      success: false,
      message: error.message
    });
  }
}

app.get("/webhook", webhookHandler);
app.get("/webhooks", webhookHandler);
app.post("/webhook", verifyFacebookSignature, webhookEventHandler);
app.post("/webhooks", verifyFacebookSignature, webhookEventHandler);

app.get("/", (req, res) => {
  res.json({
    success: true,
    message: "webhook-service is running",
    webhookPath: "/webhook",
    rawEventsTopic: config.topics.rawEvents
  });
});

app.post("/", (req, res) => {
  console.warn("Received POST / instead of POST /webhook");
  console.warn("Root payload:", JSON.stringify(req.body, null, 2));

  res.status(200).json({
    success: false,
    message: "Use POST /webhook as the Meta callback URL",
    expectedPath: "/webhook"
  });
});

async function start() {
  await ensureTopics(kafka, Object.values(config.topics));
  await producer.connect();

  app.listen(config.port, () => {
    console.log(`Webhook service running at http://localhost:${config.port}`);
  });
}

async function shutdown(signal) {
  console.log(`Received ${signal}, disconnecting Kafka producer...`);

  try {
    await producer.disconnect();
  } finally {
    process.exit(0);
  }
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));

start().catch((error) => {
  console.error("Failed to start webhook service:", error);
  process.exit(1);
});
