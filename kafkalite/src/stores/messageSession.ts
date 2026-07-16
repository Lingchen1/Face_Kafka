import { defineStore } from "pinia";
import { computed, ref } from "vue";
import * as messageApi from "../api/message";
import type { ConsumeRequest, MessageDto, SessionEvent } from "../types";

const MAX_DISPLAY = 5000;

export type TimeSort = "asc" | "desc";

export const useMessageSessionStore = defineStore("messageSession", () => {
  const messages = ref<MessageDto[]>([]);
  const sessionId = ref<string | null>(null);
  const running = ref(false);
  const progress = ref(0);
  const finishReason = ref<string | null>(null);
  const error = ref<string | null>(null);
  const selectedIndex = ref<number | null>(null);
  const filterText = ref("");
  const timeSort = ref<TimeSort>("desc");

  const filteredMessages = computed(() => {
    const keyword = filterText.value.trim().toLowerCase();
    let list = messages.value;
    if (keyword) {
      list = list.filter((message) => {
        const haystack = [
          message.key ?? "",
          message.valuePreview,
          ...message.headers.map((h) => `${h.key}:${h.value ?? ""}`),
        ]
          .join(" ")
          .toLowerCase();
        return haystack.includes(keyword);
      });
    }

    return [...list].sort((a, b) => {
      const ta = a.timestamp ?? 0;
      const tb = b.timestamp ?? 0;
      return timeSort.value === "asc" ? ta - tb : tb - ta;
    });
  });

  const selectedMessage = computed(() => {
    if (selectedIndex.value === null) {
      return null;
    }
    return filteredMessages.value[selectedIndex.value] ?? null;
  });

  function clear() {
    messages.value = [];
    sessionId.value = null;
    running.value = false;
    progress.value = 0;
    finishReason.value = null;
    error.value = null;
    selectedIndex.value = null;
  }

  function handleEvent(event: SessionEvent) {
    switch (event.type) {
      case "started":
        sessionId.value = event.sessionId;
        running.value = true;
        break;
      case "batch":
        messages.value.push(...event.messages);
        if (messages.value.length > MAX_DISPLAY) {
          messages.value.splice(0, messages.value.length - MAX_DISPLAY);
        }
        break;
      case "progress":
        progress.value = event.count;
        break;
      case "finished":
        running.value = false;
        finishReason.value = event.reason;
        break;
      case "error":
        running.value = false;
        error.value = event.error.message;
        break;
    }
  }

  async function start(request: ConsumeRequest) {
    if (running.value && sessionId.value) {
      await stop();
    }
    messages.value = [];
    progress.value = 0;
    finishReason.value = null;
    error.value = null;
    selectedIndex.value = null;
    running.value = true;
    try {
      sessionId.value = await messageApi.startConsume(request, handleEvent);
    } catch (err) {
      running.value = false;
      error.value = err instanceof Error ? err.message : String(err);
      throw err;
    }
  }

  async function stop() {
    if (!sessionId.value) {
      running.value = false;
      return;
    }
    try {
      await messageApi.stopConsume(sessionId.value);
    } catch (err) {
      error.value = err instanceof Error ? err.message : String(err);
    }
  }

  function select(index: number) {
    selectedIndex.value = index;
  }

  function exportJsonLines(): string {
    return filteredMessages.value
      .map((message) =>
        JSON.stringify({
          topic: message.topic,
          partition: message.partition,
          offset: message.offset,
          timestamp: message.timestamp,
          key: message.key,
          value: message.valuePreview,
          valueEncoding: message.valueEncoding,
          valueSize: message.valueSize,
          truncated: message.truncated,
          headers: message.headers,
        }),
      )
      .join("\n");
  }

  return {
    messages,
    sessionId,
    running,
    progress,
    finishReason,
    error,
    selectedIndex,
    selectedMessage,
    filterText,
    timeSort,
    filteredMessages,
    clear,
    start,
    stop,
    select,
    exportJsonLines,
  };
});
