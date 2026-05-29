function buildReplyMessage(classification) {
  switch (classification.intent) {
    case "ask_price":
      return "Shop da nhan duoc cau hoi ve gia. Ban de lai ten san pham hoac mau can tu van, shop se bao gia nhanh cho ban nhe.";
    case "ask_shipping":
      return "Shop co ho tro giao hang toan quoc. Ban inbox giup shop dia chi hoac khu vuc de shop bao phi ship chinh xac nhe.";
    case "purchase_intent":
      return "Cam on ban quan tam. Ban inbox thong tin san pham va so dien thoai, shop se ho tro len don ngay.";
    case "praise":
      return "Cam on ban rat nhieu. Phan hoi cua ban la dong luc de shop phuc vu tot hon nua.";
    case "complaint":
      return "Shop rat tiec vi trai nghiem cua ban chua tot. Ban inbox ma don hoac so dien thoai de shop kiem tra va ho tro ngay nhe.";
    default:
      return null;
  }
}

function buildDecision(event, spamAnalysis, classification) {
  const actions = [];
  let route = "no_action";

  if (spamAnalysis.isSpam) {
    actions.push({
      type: "hide_comment",
      reason: spamAnalysis.spamCategory
    });
    route = "spam_handling";

    if (
      spamAnalysis.severity === "high" ||
      spamAnalysis.signals.duplicateCount24h >= 3 ||
      spamAnalysis.reasons.includes("user_blacklisted")
    ) {
      actions.push({
        type: "blacklist_user",
        reason: spamAnalysis.reasons.join(",")
      });
    }

    if (spamAnalysis.severity === "high") {
      actions.push({
        type: "manual_review",
        reason: "high_risk_spam"
      });
    }

    return {
      route,
      actions,
      autoReplySuppressed: true,
      summary: "Spam detected"
    };
  }

  if (classification.intent === "complaint" && classification.sentiment === "negative") {
    actions.push({
      type: "manual_review",
      reason: "negative_complaint"
    });

    const replyMessage = buildReplyMessage(classification);
    if (replyMessage && event.commentId) {
      actions.push({
        type: "reply_comment",
        message: replyMessage,
        reason: "complaint_acknowledgement"
      });
    }

    return {
      route: "manual_review",
      actions,
      autoReplySuppressed: false,
      summary: "Complaint routed to manual review"
    };
  }

  const replyMessage = buildReplyMessage(classification);

  if (replyMessage && event.commentId) {
    actions.push({
      type: "reply_comment",
      message: replyMessage,
      reason: classification.intent
    });

    return {
      route: "auto_reply",
      actions,
      autoReplySuppressed: false,
      summary: `Auto reply for ${classification.intent}`
    };
  }

  return {
    route: "observe_only",
    actions,
    autoReplySuppressed: true,
    summary: "No automation rule matched"
  };
}

module.exports = {
  buildDecision
};
