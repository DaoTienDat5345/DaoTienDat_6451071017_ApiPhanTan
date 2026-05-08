const { normalizeText } = require("./utils");

const LINK_PATTERN = /(https?:\/\/|www\.|bit\.ly|tinyurl\.com|t\.co|wa\.me|fb\.me)/i;
const SCAM_KEYWORDS = [
  "telegram",
  "zalo",
  "momo",
  "coc coc",
  "click link",
  "ib ngay",
  "quet ma",
  "chuyen khoan truoc"
];

function detectSpam(event, context = {}) {
  const message = String(event.message || "");
  const normalizedMessage = normalizeText(message);
  const recentSameUserEvents = context.recentSameUserEvents || [];
  const recentSameMessageEvents = context.recentSameMessageEvents || [];
  const isBlacklisted = Boolean(context.isBlacklisted);
  const reasons = [];
  let score = 0;

  const hasLink = LINK_PATTERN.test(message);
  const hasScamKeyword = SCAM_KEYWORDS.some((keyword) => normalizedMessage.includes(keyword));
  const duplicateCount24h = recentSameMessageEvents.length + (normalizedMessage ? 1 : 0);
  const userCount24h = recentSameUserEvents.length + (event.fromUserId ? 1 : 0);

  if (isBlacklisted) {
    reasons.push("user_blacklisted");
    score = Math.max(score, 0.99);
  }

  if (hasLink) {
    reasons.push("contains_link");
    score += 0.45;
  }

  if (hasScamKeyword) {
    reasons.push("contains_scam_keyword");
    score += 0.35;
  }

  if (duplicateCount24h >= 3) {
    reasons.push("duplicate_content_24h");
    score += 0.4;
  }

  if (userCount24h >= 3) {
    reasons.push("high_frequency_user_24h");
    score += 0.25;
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
  } else if (duplicateCount24h >= 3) {
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
      hasScamKeyword,
      duplicateCount24h,
      userCount24h
    }
  };
}

module.exports = {
  detectSpam
};
