const WebSocket = require("ws");
const debugLogger = require("./debugLogger");

const SAMPLE_RATE = 16000;
const WEBSOCKET_TIMEOUT_MS = 30000;
const TERMINATION_TIMEOUT_MS = 5000;
const TOKEN_REFRESH_BUFFER_MS = 30000;
const TOKEN_EXPIRY_MS = 300000;

class AssemblyAiStreaming {
  constructor() {
    this.ws = null;
    this.sessionId = null;
    this.isConnected = false;
    this.onPartialTranscript = null;
    this.onFinalTranscript = null;
    this.onError = null;
    this.onSessionEnd = null;
    this.pendingResolve = null;
    this.pendingReject = null;
    this.connectionTimeout = null;
    this.accumulatedText = "";
    this.lastTurnText = "";
    this.turns = [];
    this.terminationResolve = null;
    this.cachedToken = null;
    this.tokenFetchedAt = null;
    this.isDisconnecting = false;
  }

  buildWebSocketUrl(options) {
    const sampleRate = options.sampleRate || SAMPLE_RATE;
    const params = new URLSearchParams({
      sample_rate: String(sampleRate),
      encoding: "pcm_s16le",
      format_turns: "true",
      token: options.token,
    });
    if (options.language && options.language !== "auto") {
      params.set("speech_model", "universal-streaming-multilingual");
    }
    return `wss://streaming.assemblyai.com/v3/ws?${params.toString()}`;
  }

  cacheToken(token) {
    this.cachedToken = token;
    this.tokenFetchedAt = Date.now();
    debugLogger.debug("AssemblyAI token cached", { expiresIn: TOKEN_EXPIRY_MS });
  }

  isTokenValid() {
    if (!this.cachedToken || !this.tokenFetchedAt) return false;
    const age = Date.now() - this.tokenFetchedAt;
    return age < TOKEN_EXPIRY_MS - TOKEN_REFRESH_BUFFER_MS;
  }

  getCachedToken() {
    return this.isTokenValid() ? this.cachedToken : null;
  }

  async connect(options = {}) {
    const { token } = options;
    if (!token) {
      throw new Error("Streaming token is required");
    }

    if (this.isConnected) {
      debugLogger.debug("AssemblyAI streaming already connected");
      return;
    }

    // Reset accumulated text for new session
    this.accumulatedText = "";
    this.lastTurnText = "";
    this.turns = [];

    const url = this.buildWebSocketUrl(options);
    debugLogger.debug("AssemblyAI streaming connecting");

    return new Promise((resolve, reject) => {
      this.pendingResolve = resolve;
      this.pendingReject = reject;

      this.connectionTimeout = setTimeout(() => {
        this.cleanup();
        reject(new Error("AssemblyAI WebSocket connection timeout"));
      }, WEBSOCKET_TIMEOUT_MS);

      this.ws = new WebSocket(url);

      this.ws.on("open", () => {
        debugLogger.debug("AssemblyAI WebSocket connected");
      });

      this.ws.on("message", (data) => {
        this.handleMessage(data);
      });

      this.ws.on("error", (error) => {
        debugLogger.error("AssemblyAI WebSocket error", { error: error.message });
        this.cleanup();
        if (this.pendingReject) {
          this.pendingReject(error);
          this.pendingReject = null;
          this.pendingResolve = null;
        }
        this.onError?.(error);
      });

      this.ws.on("close", (code, reason) => {
        const wasActive = this.isConnected;
        debugLogger.debug("AssemblyAI WebSocket closed", {
          code,
          reason: reason?.toString(),
          wasActive,
        });
        this.cleanup();
        if (wasActive && !this.isDisconnecting) {
          this.onError?.(new Error(`Connection lost (code: ${code})`));
        }
      });
    });
  }

  handleMessage(data) {
    try {
      const message = JSON.parse(data.toString());

      switch (message.type) {
        case "Begin":
          this.sessionId = message.id;
          this.isConnected = true;
          clearTimeout(this.connectionTimeout);
          debugLogger.debug("AssemblyAI session started", { sessionId: this.sessionId });
          if (this.pendingResolve) {
            this.pendingResolve();
            this.pendingResolve = null;
            this.pendingReject = null;
          }
          break;

        case "Turn":
          if (message.transcript) {
            if (message.end_of_turn) {
              // Turn has ended - append once, then replace with formatted variant if needed
              const trimmedTranscript = message.transcript.trim();
              const normalizedTranscript = this.normalizeTurnText(trimmedTranscript);
              const previousTurn = this.turns[this.turns.length - 1];

              if (!trimmedTranscript || !normalizedTranscript) {
                break;
              }

              if (previousTurn && previousTurn.normalized === normalizedTranscript) {
                // AssemblyAI can emit the same turn twice (raw then formatted). Replace previous
                // turn only when this variant is formatted, otherwise ignore duplicate.
                if (message.turn_is_formatted && previousTurn.text !== trimmedTranscript) {
                  previousTurn.text = trimmedTranscript;
                  this.lastTurnText = trimmedTranscript;
                  this.accumulatedText = this.turns.map((turn) => turn.text).join(" ");
                  this.onFinalTranscript?.(this.accumulatedText);
                  debugLogger.debug("AssemblyAI formatted turn update applied", {
                    text: trimmedTranscript.slice(0, 100),
                    totalAccumulated: this.accumulatedText.length,
                  });
                } else {
                  debugLogger.debug("AssemblyAI duplicate turn ignored", {
                    text: trimmedTranscript.slice(0, 100),
                  });
                }
                break;
              }

              this.turns.push({
                text: trimmedTranscript,
                normalized: normalizedTranscript,
              });
              this.lastTurnText = trimmedTranscript;
              this.accumulatedText = this.turns.map((turn) => turn.text).join(" ");
              this.onFinalTranscript?.(this.accumulatedText);
              debugLogger.debug("AssemblyAI final transcript (end_of_turn)", {
                text: message.transcript.slice(0, 100),
                totalAccumulated: this.accumulatedText.length,
              });
            } else if (message.turn_is_formatted) {
              // Formatted but turn not ended yet - show as preview without accumulating
              this.onPartialTranscript?.(message.transcript);
            } else {
              // Partial transcript - show real-time updates (current turn only)
              this.onPartialTranscript?.(message.transcript);
            }
          }
          break;

        case "Termination":
          debugLogger.debug("AssemblyAI session terminated", {
            audioDuration: message.audio_duration_seconds,
          });
          // Resolve any pending termination wait
          if (this.terminationResolve) {
            this.terminationResolve({
              audioDuration: message.audio_duration_seconds,
              text: this.accumulatedText,
            });
            this.terminationResolve = null;
          }
          this.onSessionEnd?.({
            audioDuration: message.audio_duration_seconds,
            text: this.accumulatedText,
          });
          this.cleanup();
          break;

        case "Error":
          debugLogger.error("AssemblyAI streaming error", { error: message.error });
          this.onError?.(new Error(message.error));
          break;

        default:
          debugLogger.debug("AssemblyAI unknown message type", { type: message.type });
      }
    } catch (err) {
      debugLogger.error("AssemblyAI message parse error", { error: err.message });
    }
  }

  normalizeTurnText(text) {
    return text
      .toLowerCase()
      .replace(/[^\p{L}\p{N}\s]/gu, " ")
      .replace(/\s+/g, " ")
      .trim();
  }

  sendAudio(pcmBuffer) {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      return false;
    }

    this.ws.send(pcmBuffer);
    return true;
  }

  forceEndpoint() {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      return false;
    }

    this.ws.send(JSON.stringify({ type: "ForceEndpoint" }));
    debugLogger.debug("AssemblyAI ForceEndpoint sent");
    return true;
  }

  async disconnect(terminate = true) {
    if (!this.ws) return { text: this.accumulatedText };

    this.isDisconnecting = true;

    if (terminate && this.ws.readyState === WebSocket.OPEN) {
      try {
        this.ws.send(JSON.stringify({ type: "Terminate" }));

        let timeoutId;
        const result = await Promise.race([
          new Promise((resolve) => {
            this.terminationResolve = resolve;
          }),
          new Promise((resolve) => {
            timeoutId = setTimeout(() => {
              debugLogger.debug("AssemblyAI termination timeout, using accumulated text");
              resolve({ text: this.accumulatedText });
            }, TERMINATION_TIMEOUT_MS);
          }),
        ]);
        clearTimeout(timeoutId);

        this.terminationResolve = null;
        this.cleanup();
        this.isDisconnecting = false;
        return result;
      } catch (err) {
        debugLogger.debug("AssemblyAI terminate send failed", { error: err.message });
      }
    }

    const result = { text: this.accumulatedText };
    this.cleanup();
    this.isDisconnecting = false;
    return result;
  }

  cleanup() {
    clearTimeout(this.connectionTimeout);
    this.connectionTimeout = null;

    if (this.ws) {
      try {
        this.ws.close();
      } catch (err) {
        // Ignore close errors
      }
      this.ws = null;
    }

    this.isConnected = false;
    this.sessionId = null;
    this.terminationResolve = null;
  }

  cleanupAll() {
    this.cleanup();
    this.cachedToken = null;
    this.tokenFetchedAt = null;
    this.turns = [];
  }

  getStatus() {
    return {
      isConnected: this.isConnected,
      sessionId: this.sessionId,
      hasValidToken: this.isTokenValid(),
    };
  }
}

module.exports = AssemblyAiStreaming;
