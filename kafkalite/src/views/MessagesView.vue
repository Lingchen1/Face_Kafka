<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, reactive, ref, watch } from "vue";
import { storeToRefs } from "pinia";
import { useConnectionStore } from "../stores/connection";
import { useMessageSessionStore } from "../stores/messageSession";
import { useTopicStore } from "../stores/topic";
import * as messageApi from "../api/message";

const connectionStore = useConnectionStore();
const topicStore = useTopicStore();
const messageStore = useMessageSessionStore();
const { timeSort, selectedIndex } = storeToRefs(messageStore);

const form = reactive({
  topic: "",
  partitionMode: "all" as "all" | "single",
  partition: 0,
  startType: "earliest" as "earliest" | "latest" | "offset" | "timestamp",
  startValue: 0,
  limit: 500,
  filter: "",
});

const showTombstone = ref(false);
const tombstoneKey = ref("");
const tombstonePartition = ref<number | undefined>(undefined);
const tombstoneInfo = ref<string | null>(null);
const previewMode = ref<"raw" | "json">("raw");
const rowHeight = 36;
const scrollTop = ref(0);

const activeId = computed(() => connectionStore.activeConnection?.id);
const activeName = computed(() => connectionStore.activeConnection?.name ?? "");
const topicOptions = computed(() => topicStore.topics.map((item) => item.name));

const finishReasonText = computed(() => {
  const reason = messageStore.finishReason;
  if (!reason) {
    return "";
  }
  const map: Record<string, string> = {
    idle: "已读完或超时无新消息",
    limit: "已达到条数上限",
    cancelled: "用户已停止",
  };
  return map[reason] ?? reason;
});

const timeSortLabel = computed(() =>
  timeSort.value === "desc" ? "时间 ↓ 降序" : "时间 ↑ 升序",
);

function onToggleTimeSort() {
  timeSort.value = timeSort.value === "desc" ? "asc" : "desc";
  selectedIndex.value = null;
}

const visibleWindow = computed(() => {
  const list = messageStore.filteredMessages;
  const viewportHeight = 420;
  const start = Math.max(0, Math.floor(scrollTop.value / rowHeight) - 5);
  const visibleCount = Math.ceil(viewportHeight / rowHeight) + 10;
  const end = Math.min(list.length, start + visibleCount);
  return {
    offsetY: start * rowHeight,
    totalHeight: list.length * rowHeight,
    items: list.slice(start, end).map((message, index) => ({
      message,
      index: start + index,
    })),
  };
});

function formatTime(timestamp?: number): string {
  if (timestamp === undefined || timestamp === null || Number.isNaN(timestamp)) {
    return "-";
  }
  const date = new Date(timestamp);
  if (Number.isNaN(date.getTime())) {
    return "-";
  }
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
}

const formattedPreview = computed(() => {
  const message = messageStore.selectedMessage;
  if (!message) {
    return "";
  }
  if (previewMode.value === "json") {
    try {
      return JSON.stringify(JSON.parse(message.valuePreview), null, 2);
    } catch {
      return message.valuePreview;
    }
  }
  return message.valuePreview;
});

onMounted(async () => {
  if (!connectionStore.activeConnection) {
    await connectionStore.refresh();
  }
  if (activeId.value && !topicStore.topics.length) {
    await topicStore.refresh(activeId.value);
  }
  form.topic = topicStore.selectedTopic ?? topicOptions.value[0] ?? "";
  messageStore.filterText = "";
});

watch(
  () => topicStore.selectedTopic,
  (value) => {
    if (value) {
      form.topic = value;
    }
  },
);

watch(
  () => form.filter,
  (value) => {
    messageStore.filterText = value;
  },
);

onBeforeUnmount(async () => {
  if (messageStore.running) {
    await messageStore.stop();
  }
});

async function startQuery() {
  if (!activeId.value || !form.topic) {
    return;
  }
  topicStore.selectedTopic = form.topic;
  await messageStore.start({
    connectionId: activeId.value,
    topic: form.topic,
    partitions: form.partitionMode === "all" ? "all" : [form.partition],
    start: {
      type: form.startType,
      value:
        form.startType === "offset" || form.startType === "timestamp"
          ? form.startValue
          : undefined,
    },
    limit: form.limit,
    batchSize: 200,
    idleTimeoutMs: 4000,
  });
}

function onScroll(event: Event) {
  const target = event.target as HTMLElement;
  scrollTop.value = target.scrollTop;
}

function exportResults() {
  const content = messageStore.exportJsonLines();
  const blob = new Blob([content], { type: "application/x-ndjson" });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = `${form.topic || "messages"}.jsonl`;
  anchor.click();
  URL.revokeObjectURL(url);
}

async function sendTombstone() {
  if (!activeId.value || !form.topic || !tombstoneKey.value) {
    return;
  }
  const result = await messageApi.produceTombstone({
    connectionId: activeId.value,
    topic: form.topic,
    key: tombstoneKey.value,
    partition: tombstonePartition.value,
  });
  tombstoneInfo.value = `已发送 Tombstone：分区 ${result.partition}，Offset ${result.offset}`;
  tombstoneKey.value = "";
  showTombstone.value = false;
}
</script>

<template>
  <section class="page page-fill">
    <header class="page-header">
      <div>
        <h1>消息查询</h1>
        <p>按最早 / 最新 / Offset / 时间戳拉取，支持停止与导出。</p>
      </div>
      <div class="header-right">
        <span class="conn-badge" :class="{ on: !!activeId }">
          {{ activeId ? `已连接 · ${activeName}` : "未连接" }}
        </span>
        <button
          type="button"
          class="ghost"
          :disabled="!activeId || !form.topic"
          @click="showTombstone = true"
        >
          发送 Tombstone
        </button>
      </div>
    </header>

    <div v-if="!activeId" class="panel empty-state">请先连接 Kafka 集群。</div>

    <template v-else>
      <div class="panel query-panel">
        <div class="form-inline">
          <label>
            Topic
            <select v-model="form.topic">
              <option v-for="name in topicOptions" :key="name" :value="name">
                {{ name }}
              </option>
            </select>
          </label>
          <label>
            分区
            <select v-model="form.partitionMode">
              <option value="all">全部</option>
              <option value="single">单个</option>
            </select>
          </label>
          <label v-if="form.partitionMode === 'single'">
            分区 ID
            <input v-model.number="form.partition" type="number" min="0" />
          </label>
          <label>
            起点
            <select v-model="form.startType">
              <option value="earliest">最早（从头读）</option>
              <option value="latest">最新（只读新消息）</option>
              <option value="offset">指定 Offset</option>
              <option value="timestamp">指定时间戳</option>
            </select>
          </label>
          <label
            v-if="form.startType === 'offset' || form.startType === 'timestamp'"
          >
            {{ form.startType === "timestamp" ? "时间戳(ms)" : "Offset" }}
            <input v-model.number="form.startValue" type="number" />
          </label>
          <label>
            条数
            <select v-model.number="form.limit">
              <option :value="50">50</option>
              <option :value="100">100</option>
              <option :value="500">500</option>
              <option :value="1000">1000</option>
              <option :value="5000">5000</option>
              <option :value="10000">10000</option>
            </select>
          </label>
        </div>
        <div class="actions">
          <button
            type="button"
            :disabled="messageStore.running || !form.topic"
            @click="startQuery"
          >
            开始查询
          </button>
          <button
            type="button"
            class="secondary"
            :disabled="!messageStore.running"
            @click="messageStore.stop()"
          >
            停止
          </button>
          <button
            type="button"
            class="secondary"
            :disabled="!messageStore.filteredMessages.length"
            @click="exportResults"
          >
            导出 JSONL
          </button>
          <button type="button" class="secondary" @click="messageStore.clear()">
            清空
          </button>
        </div>
        <p class="muted">
          已拉取 {{ messageStore.progress }} 条
          <span v-if="finishReasonText"> · 结束原因：{{ finishReasonText }}</span>
          <span v-if="messageStore.running"> · 查询中...</span>
        </p>
        <p v-if="tombstoneInfo" class="success">{{ tombstoneInfo }}</p>
        <p v-if="messageStore.error" class="error">{{ messageStore.error }}</p>
      </div>

      <div class="page-grid message-grid">
        <div class="panel panel-scroll">
          <div class="toolbar">
            <input
              v-model="form.filter"
              placeholder="本地筛选 Key / Value / Header..."
            />
          </div>
          <div class="message-list-wrap">
            <div class="message-table-header">
              <span>分区</span>
              <span>Offset</span>
              <span>Key</span>
              <span>Value</span>
              <button
                type="button"
                class="sort-btn"
                title="点击切换时间升序/降序"
                @click.stop.prevent="onToggleTimeSort"
              >
                {{ timeSortLabel }}
              </button>
            </div>
            <div class="message-table-body" @scroll="onScroll">
              <div
                class="virtual-spacer"
                :style="{ height: `${visibleWindow.totalHeight}px` }"
              >
                <div
                  class="virtual-window"
                  :style="{
                    transform: `translateY(${visibleWindow.offsetY}px)`,
                  }"
                >
                  <button
                    v-for="item in visibleWindow.items"
                    :key="`${item.message.partition}-${item.message.offset}-${item.index}`"
                    type="button"
                    class="message-row"
                    :class="{
                      selected: messageStore.selectedIndex === item.index,
                    }"
                    @click="messageStore.select(item.index)"
                  >
                    <span>{{ item.message.partition }}</span>
                    <span>{{ item.message.offset }}</span>
                    <span class="ellipsis" :title="item.message.key || ''">{{
                      item.message.key || "(null)"
                    }}</span>
                    <span
                      class="ellipsis"
                      :title="item.message.valuePreview"
                    >
                      {{ item.message.valuePreview }}
                    </span>
                    <span class="ellipsis">{{
                      formatTime(item.message.timestamp)
                    }}</span>
                  </button>
                </div>
              </div>
              <p
                v-if="!messageStore.filteredMessages.length"
                class="muted"
                style="padding: 12px"
              >
                暂无消息。
              </p>
            </div>
          </div>
        </div>

        <div class="panel panel-scroll">
          <div class="panel-body">
            <h2>消息详情</h2>
            <template v-if="messageStore.selectedMessage">
              <p>
                分区 {{ messageStore.selectedMessage.partition }} · Offset
                {{ messageStore.selectedMessage.offset }}
              </p>
              <p class="muted">
                时间 {{ formatTime(messageStore.selectedMessage.timestamp) }} ·
                大小 {{ messageStore.selectedMessage.valueSize }} ·
                {{ messageStore.selectedMessage.valueEncoding }}
                <span v-if="messageStore.selectedMessage.truncated">
                  · 已截断
                </span>
              </p>
              <div class="actions">
                <button
                  type="button"
                  class="secondary"
                  @click="previewMode = 'raw'"
                >
                  原文
                </button>
                <button
                  type="button"
                  class="secondary"
                  @click="previewMode = 'json'"
                >
                  JSON
                </button>
              </div>
              <pre class="preview">{{ formattedPreview }}</pre>
              <h3>Headers</h3>
              <ul>
                <li
                  v-for="header in messageStore.selectedMessage.headers"
                  :key="header.key"
                >
                  {{ header.key }} = {{ header.value ?? "(null)" }}
                </li>
              </ul>
            </template>
            <p v-else class="muted">点击左侧消息查看详情。</p>
          </div>
        </div>
      </div>
    </template>

    <div
      v-if="showTombstone"
      class="modal-mask"
      @click.self="showTombstone = false"
    >
      <form class="panel modal" @submit.prevent="sendTombstone">
        <h2>发送 Tombstone</h2>
        <p class="modal-hint">
          Tombstone 是一条「相同 Key、Value 为空」的消息，用于
          compact（压缩）类型 Topic 按 Key 做逻辑删除。普通 delete 策略 Topic
          一般用不到。
        </p>
        <label>
          Key（必填）
          <input
            v-model="tombstoneKey"
            required
            placeholder="要逻辑删除的消息 Key"
          />
        </label>
        <label>
          分区（可选，不填则由 Kafka 决定）
          <input v-model.number="tombstonePartition" type="number" min="0" />
        </label>
        <div class="actions">
          <button
            type="button"
            class="secondary"
            @click="showTombstone = false"
          >
            取消
          </button>
          <button type="submit" :disabled="!tombstoneKey">发送</button>
        </div>
      </form>
    </div>
  </section>
</template>
