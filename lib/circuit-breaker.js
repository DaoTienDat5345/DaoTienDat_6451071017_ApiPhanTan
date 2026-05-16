class CircuitBreaker {
  constructor({ failureThreshold = 5, openTimeoutMs = 30000, halfOpenMaxRequests = 1 } = {}) {
    this.failureThreshold = failureThreshold;
    this.openTimeoutMs = openTimeoutMs;
    this.halfOpenMaxRequests = halfOpenMaxRequests;
    this.state = "CLOSED";
    this.failureCount = 0;
    this.openedAt = null;
    this.halfOpenRequests = 0;
  }

  getState() {
    if (this.state === "OPEN" && this.openedAt) {
      const elapsed = Date.now() - this.openedAt;
      if (elapsed >= this.openTimeoutMs) {
        this.state = "HALF_OPEN";
        this.halfOpenRequests = 0;
      }
    }

    return {
      state: this.state,
      failureCount: this.failureCount,
      openedAt: this.openedAt,
      openTimeoutMs: this.openTimeoutMs
    };
  }

  canRequest() {
    const current = this.getState();

    if (current.state === "CLOSED") {
      return true;
    }

    if (current.state === "HALF_OPEN") {
      if (this.halfOpenRequests < this.halfOpenMaxRequests) {
        this.halfOpenRequests += 1;
        return true;
      }
      return false;
    }

    return false;
  }

  recordSuccess() {
    this.state = "CLOSED";
    this.failureCount = 0;
    this.openedAt = null;
    this.halfOpenRequests = 0;
  }

  recordFailure() {
    if (this.state === "HALF_OPEN") {
      this.open();
      return;
    }

    this.failureCount += 1;

    if (this.failureCount >= this.failureThreshold) {
      this.open();
    }
  }

  open() {
    this.state = "OPEN";
    this.openedAt = Date.now();
    this.halfOpenRequests = 0;
  }
}

module.exports = {
  CircuitBreaker
};
