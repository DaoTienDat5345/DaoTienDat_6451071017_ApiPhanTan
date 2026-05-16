const { normalizeText } = require("./utils");

const LINK_PATTERN = /(https?:\/\/|www\.|bit\.ly|tinyurl\.com|t\.co|wa\.me|fb\.me|zalo\.me|telegram\.me|t\.me)/i;
const PHONE_PATTERN = /(\+?84|0)(\s|\.|-)?(3|5|7|8|9)(\d|\s|\.|-){8,}/;
const REPEATED_CHARS_PATTERN = /(.)\1{7,}/i;
const SCAM_KEYWORDS = [
  "telegram",
  "zalo",
  "momo",
  "coc coc",
  "click link",
  "bam link",
  "vao link",
  "ib ngay",
  "quet ma",
  "chuyen khoan truoc",
  "nhan qua",
  "trung thuong",
  "vay tien",
  "kiem tien online",
  "khuyen mai soc",
  "mien phi 100",
  "tai app",
  "dang nhap de nhan"
];

function uniqueCount(events) {
  const ids = new Set();

  for (const event of events || []) {
    ids.add(event.eventId || `${event.commentId || ""}:${event.createdAt || event.updatedAt || ""}`);
  }

  return ids.size;
}

function detectSpam(event, context = {}) {
  const message = String(event.message || "");
  const normalizedMessage = normalizeText(message);
  const recentSameUserEvents = context.recentSameUserEvents || [];
  const recentSameMessageEvents = context.recentSameMessageEvents || [];
  const recentSamePageMessageEvents = context.recentSamePageMessageEvents || [];
  const isBlacklisted = Boolean(context.isBlacklisted);
  const reasons = [];
  let score = 0;

  const hasLink = LINK_PATTERN.test(message);
  const hasPhone = PHONE_PATTERN.test(message);
  const hasScamKeyword = SCAM_KEYWORDS.some((keyword) => normalizedMessage.includes(keyword));
  const hasRepeatedChars = REPEATED_CHARS_PATTERN.test(normalizedMessage);
  const duplicateCount24h = uniqueCount(recentSameMessageEvents) + (normalizedMessage ? 1 : 0);
  const pageDuplicateCount24h = uniqueCount(recentSamePageMessageEvents) + (normalizedMessage ? 1 : 0);
  const userCount24h = uniqueCount(recentSameUserEvents) + (event.fromUserId ? 1 : 0);

  if (isBlacklisted) {
    reasons.push("user_blacklisted");
    score = Math.max(score, 0.99);
  }

  if (hasLink) {
    reasons.push("contains_link");
    score += 0.45;
  }

  if (hasPhone) {
    reasons.push("contains_phone_number");
    score += 0.15;
  }

  if (hasScamKeyword) {
    reasons.push("contains_scam_keyword");
    score += 0.35;
  }

  if (duplicateCount24h >= 3) {
    reasons.push("same_user_repeated_content_24h");
    score += 0.45;
  }

  if (pageDuplicateCount24h >= 8) {
    reasons.push("same_content_spammed_on_page_24h");
    score += 0.3;
  }

  if (userCount24h >= 6) {
    reasons.push("high_frequency_user_24h");
    score += 0.25;
  }

  if (hasRepeatedChars) {
    reasons.push("repeated_characters");
    score += 0.15;
  }

  if (normalizedMessage && normalizedMessage.length <= 2) {
    reasons.push("too_short");
    score += 0.1;
  }

  score = Math.min(Number(score.toFixed(2)), 0.99);

  let severity = "clean";
  let spamCategory = "clean";

  if (isBlacklisted || (hasLink && hasScamKeyword)) {
    severity = "high";
    spamCategory = "malicious_or_blacklisted";
  } else if (duplicateCount24h >= 3 || userCount24h >= 6) {
    severity = "medium";
    spamCategory = "repeated_comment";
  } else if (score >= 0.45) {
    severity = "low";
    spamCategory = "promotional_or_suspicious";
  }

  return {
    isSpam: score >= 0.45 || isBlacklisted,
    severity,
    spamCategory,
    spamScore: score,
    reasons,
    signals: {
      hasLink,
      hasPhone,
      hasScamKeyword,
      hasRepeatedChars,
      duplicateCount24h,
      pageDuplicateCount24h,
      userCount24h
    }
  };
}

module.exports = {
  detectSpam
};
