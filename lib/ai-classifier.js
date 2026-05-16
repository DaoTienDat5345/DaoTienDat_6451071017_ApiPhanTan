const axios = require("axios");
const config = require("./config");
const { createRetryableError, extractJsonObject, normalizeText } = require("./utils");

function heuristicClassify(message, spamAnalysis) {
  const normalized = normalizeText(message);

  if (spamAnalysis && spamAnalysis.isSpam) {
    return {
      intent: "spam",
      sentiment: "negative",
      confidence: 0.88,
      source: "heuristic"
    };
  }

  if (!normalized) {
    return {
      intent: "unknown",
      sentiment: "neutral",
      confidence: 0.5,
      source: "heuristic"
    };
  }

  if (/(^|\b)(gia|bao nhieu|price|bao gia|gia bao nhieu|bn)($|\b)/.test(normalized)) {
    return {
      intent: "ask_price",
      sentiment: "neutral",
      confidence: 0.85,
      source: "heuristic"
    };
  }

  if (/ship|giao hang|cod|phi ship/.test(normalized)) {
    return {
      intent: "ask_shipping",
      sentiment: "neutral",
      confidence: 0.82,
      source: "heuristic"
    };
  }

  if (/khong nhan|chua nhan|chua duoc|kiem tra giup|ho tro|don hang|hang dau|te qua|that vong|loi|khieu nai|phan nan/.test(normalized)) {
    return {
      intent: "complaint",
      sentiment: "negative",
      confidence: 0.84,
      source: "heuristic"
    };
  }

  if (/hay qua|bai viet hay|dep qua|rat dep|san pham.*dep|tot qua|uy tin|rat ung|xinh qua|xin qua|tuyet voi|hai long/.test(normalized)) {
    return {
      intent: "praise",
      sentiment: "positive",
      confidence: 0.83,
      source: "heuristic"
    };
  }

  if (/mua|chot|chốt|inbox|dat hang|đặt hàng/.test(normalized)) {
    return {
      intent: "purchase_intent",
      sentiment: "positive",
      confidence: 0.78,
      source: "heuristic"
    };
  }

  return {
    intent: "unknown",
    sentiment: "neutral",
    confidence: 0.55,
    source: "heuristic"
  };
}

function buildClassificationPrompt(message) {
  return [
    "You classify social media comments.",
    "Return JSON only with keys: intent, sentiment, confidence.",
    'Allowed intent values: ask_price, ask_shipping, complaint, praise, purchase_intent, spam, unknown.',
    'Allowed sentiment values: positive, neutral, negative.',
    `Comment: ${JSON.stringify(message || "")}`
  ].join("\n");
}

function normalizeClassification(parsed) {
  return {
    intent: parsed.intent || "unknown",
    sentiment: parsed.sentiment || "neutral",
    confidence: Number(parsed.confidence) || 0.6,
    source: "ai"
  };
}

function logProviderFallback(provider, error) {
  const status = error.response && error.response.status;
  const details = error.response && error.response.data ? error.response.data : error.message;

  console.warn(`[ai-classifier] ${provider} fallback to heuristic`, {
    status: status || null,
    details
  });
}

async function classifyWithOpenAiCompatible(message, spamAnalysis) {
  if (!config.ai.apiKey) {
    return heuristicClassify(message, spamAnalysis);
  }

  const prompt = buildClassificationPrompt(message);

  try {
    const response = await axios.post(
      `${config.ai.baseUrl.replace(/\/$/, "")}/chat/completions`,
      {
        model: config.ai.model,
        temperature: 0,
        messages: [
          {
            role: "system",
            content: "Return valid compact JSON only."
          },
          {
            role: "user",
            content: prompt
          }
        ]
      },
      {
        headers: {
          Authorization: `Bearer ${config.ai.apiKey}`,
          "Content-Type": "application/json"
        },
        timeout: config.ai.timeoutMs
      }
    );

    const content =
      response.data &&
      response.data.choices &&
      response.data.choices[0] &&
      response.data.choices[0].message &&
      response.data.choices[0].message.content;

    const parsed = extractJsonObject(content);

    return normalizeClassification(parsed);
  } catch (error) {
    if (error.code === "ECONNABORTED" || !error.response || error.response.status >= 500) {
      throw createRetryableError("AI classification failed temporarily", {
        cause: error,
        stage: "ai_classification"
      });
    }

    logProviderFallback("openai-compatible", error);
    return heuristicClassify(message, spamAnalysis);
  }
}

async function classifyWithGemini(message, spamAnalysis) {
  if (!config.ai.apiKey) {
    return heuristicClassify(message, spamAnalysis);
  }

  const prompt = buildClassificationPrompt(message);
  const baseUrl = config.ai.baseUrl.replace(/\/$/, "");
  const url = `${baseUrl}/models/${config.ai.model}:generateContent`;

  try {
    const response = await axios.post(
      url,
      {
        contents: [
          {
            role: "user",
            parts: [
              {
                text: `${prompt}\nReturn valid compact JSON only.`
              }
            ]
          }
        ],
        generationConfig: {
          temperature: 0,
          responseMimeType: "application/json"
        }
      },
      {
        params: {
          key: config.ai.apiKey
        },
        headers: {
          "Content-Type": "application/json"
        },
        timeout: config.ai.timeoutMs
      }
    );

    const candidates = (response.data && response.data.candidates) || [];
    const parts =
      candidates[0] &&
      candidates[0].content &&
      Array.isArray(candidates[0].content.parts)
        ? candidates[0].content.parts
        : [];
    const content = parts
      .map((part) => part.text || "")
      .join("\n")
      .trim();

    const parsed = extractJsonObject(content);

    return normalizeClassification(parsed);
  } catch (error) {
    if (error.code === "ECONNABORTED" || !error.response || error.response.status >= 500) {
      throw createRetryableError("AI classification failed temporarily", {
        cause: error,
        stage: "ai_classification"
      });
    }

    logProviderFallback("gemini", error);
    return heuristicClassify(message, spamAnalysis);
  }
}

async function classifyMessage(message, spamAnalysis) {
  const provider = String(config.ai.provider || "mock").toLowerCase();

  if (provider === "mock" || provider === "heuristic") {
    return heuristicClassify(message, spamAnalysis);
  }

  if (provider === "google" || provider === "gemini") {
    return classifyWithGemini(message, spamAnalysis);
  }

  return classifyWithOpenAiCompatible(message, spamAnalysis);
}

module.exports = {
  classifyMessage
};
