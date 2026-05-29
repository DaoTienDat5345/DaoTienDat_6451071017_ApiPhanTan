const axios = require("axios");
const config = require("./config");

class FacebookClient {
  constructor({ accessToken, graphVersion, simulateActions }) {
    this.accessToken = accessToken;
    this.graphVersion = graphVersion;
    this.simulateActions = simulateActions;
    this.baseUrl = `https://graph.facebook.com/${graphVersion}`;
  }

  ensureToken() {
    if (!this.accessToken && !this.simulateActions) {
      const error = new Error("Missing PAGE_ACCESS_TOKEN");
      error.code = "MISSING_PAGE_ACCESS_TOKEN";
      throw error;
    }
  }

  async replyComment(commentId, message) {
    if (!commentId || !message) {
      const error = new Error("Missing commentId or message for reply_comment");
      error.code = "INVALID_COMMAND";
      throw error;
    }

    if (this.simulateActions) {
      return {
        simulated: true,
        action: "reply_comment",
        commentId,
        message
      };
    }

    this.ensureToken();
    const response = await axios.post(
      `${this.baseUrl}/${commentId}/comments`,
      null,
      {
        params: {
          message,
          access_token: this.accessToken
        },
        timeout: config.facebook.timeoutMs,
        proxy: false
      }
    );

    return response.data;
  }

  async hideComment(commentId) {
    if (!commentId) {
      const error = new Error("Missing commentId for hide_comment");
      error.code = "INVALID_COMMAND";
      throw error;
    }

    if (this.simulateActions) {
      return {
        simulated: true,
        action: "hide_comment",
        commentId
      };
    }

    this.ensureToken();
    const response = await axios.post(
      `${this.baseUrl}/${commentId}`,
      null,
      {
        params: {
          is_hidden: true,
          access_token: this.accessToken
        },
        timeout: config.facebook.timeoutMs,
        proxy: false
      }
    );

    return response.data;
  }

  async createPost(pageId, message) {
    if (!pageId || !message) {
      const error = new Error("Missing pageId or message for create_post");
      error.code = "INVALID_COMMAND";
      throw error;
    }

    if (this.simulateActions) {
      return {
        simulated: true,
        action: "create_post",
        pageId,
        message
      };
    }

    this.ensureToken();
    const response = await axios.post(
      `${this.baseUrl}/${pageId}/feed`,
      null,
      {
        params: {
          message,
          access_token: this.accessToken
        },
        timeout: config.facebook.timeoutMs,
        proxy: false
      }
    );

    return response.data;
  }

  async getPosts(pageId, limit = 20) {
    this.ensureToken();
    const response = await axios.get(`${this.baseUrl}/${pageId}/posts`, {
      params: {
        fields: "id,message,created_time,permalink_url",
        limit,
        access_token: this.accessToken
      },
      timeout: config.facebook.timeoutMs,
      proxy: false
    });

    return response.data;
  }

  async getComments(postId, limit = 50) {
    this.ensureToken();
    const response = await axios.get(`${this.baseUrl}/${postId}/comments`, {
      params: {
        fields: "id,message,from,created_time,comment_count,like_count",
        limit,
        access_token: this.accessToken
      },
      timeout: config.facebook.timeoutMs,
      proxy: false
    });

    return response.data;
  }
}

function createFacebookClient() {
  return new FacebookClient({
    accessToken: config.facebook.pageAccessToken,
    graphVersion: config.facebook.graphVersion,
    simulateActions: config.facebook.simulateActions
  });
}

module.exports = {
  FacebookClient,
  createFacebookClient
};
