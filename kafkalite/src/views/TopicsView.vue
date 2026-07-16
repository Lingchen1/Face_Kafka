<script setup lang="ts">
import { computed, onMounted, reactive, ref, watch } from "vue";
import { useRouter } from "vue-router";
import { useConnectionStore } from "../stores/connection";
import { useTopicStore } from "../stores/topic";

const router = useRouter();
const connectionStore = useConnectionStore();
const topicStore = useTopicStore();

const showCreate = ref(false);
const deleteConfirmName = ref("");
const deletingTopic = ref<string | null>(null);

const createForm = reactive({
  name: "",
  partitions: 3,
  replicationFactor: 1,
  cleanupPolicy: "delete",
  retentionMs: undefined as number | undefined,
});

const deleteRecordsForm = reactive({
  partition: 0,
  beforeOffset: 0,
  confirmationText: "",
});

const activeId = computed(() => connectionStore.activeConnection?.id);
const activeName = computed(() => connectionStore.activeConnection?.name ?? "");

const expectedDeleteRecordsConfirm = computed(() => {
  if (!topicStore.selectedTopic || !activeName.value) {
    return "";
  }
  return `${activeName.value}/${topicStore.selectedTopic}/${deleteRecordsForm.partition}:${deleteRecordsForm.beforeOffset}`;
});

async function refreshTopics() {
  if (!activeId.value) {
    return;
  }
  await topicStore.refresh(activeId.value);
}

onMounted(async () => {
  if (!connectionStore.activeConnection) {
    await connectionStore.refresh();
  }
  await refreshTopics();
});

watch(
  () => connectionStore.activeConnection?.id,
  () => {
    refreshTopics();
  },
);

async function submitCreate() {
  if (!activeId.value) {
    return;
  }
  await topicStore.create({
    connectionId: activeId.value,
    name: createForm.name.trim(),
    partitions: createForm.partitions,
    replicationFactor: createForm.replicationFactor,
    cleanupPolicy: createForm.cleanupPolicy || undefined,
    retentionMs: createForm.retentionMs,
  });
  showCreate.value = false;
  createForm.name = "";
}

async function confirmDelete() {
  if (!activeId.value || !deletingTopic.value) {
    return;
  }
  await topicStore.remove(
    activeId.value,
    deletingTopic.value,
    deleteConfirmName.value,
  );
  deletingTopic.value = null;
  deleteConfirmName.value = "";
}

async function openDetail(name: string) {
  if (!activeId.value) {
    return;
  }
  await topicStore.select(activeId.value, name);
  if (topicStore.detail?.partitions.length) {
    deleteRecordsForm.partition = topicStore.detail.partitions[0].id;
    deleteRecordsForm.beforeOffset =
      topicStore.detail.partitions[0].beginningOffset !== undefined
        ? (topicStore.detail.partitions[0].beginningOffset ?? 0) + 1
        : 0;
  }
}

function goMessages(name?: string) {
  if (name) {
    topicStore.selectedTopic = name;
  }
  if (topicStore.selectedTopic) {
    router.push("/messages");
  }
}

async function submitDeleteRecords() {
  if (!activeId.value || !topicStore.selectedTopic) {
    return;
  }
  await topicStore.deleteRecords({
    connectionId: activeId.value,
    topic: topicStore.selectedTopic,
    targets: [
      {
        partition: deleteRecordsForm.partition,
        beforeOffset: deleteRecordsForm.beforeOffset,
      },
    ],
    confirmationText: deleteRecordsForm.confirmationText,
  });
  deleteRecordsForm.confirmationText = "";
}
</script>

<template>
  <section class="page page-fill">
    <header class="page-header">
      <div>
        <h1>Topic 管理</h1>
        <p>浏览、创建、删除 Topic，并支持删除 Offset 之前的记录。</p>
      </div>
      <div class="header-right">
        <span class="conn-badge" :class="{ on: !!activeId }">
          {{ activeId ? `已连接 · ${activeName}` : "未连接" }}
        </span>
        <button
          type="button"
          class="secondary"
          :disabled="!activeId"
          @click="refreshTopics"
        >
          刷新
        </button>
        <button type="button" :disabled="!activeId" @click="showCreate = true">
          新建 Topic
        </button>
      </div>
    </header>

    <div v-if="!activeId" class="panel empty-state">
      请先在「连接」页面激活一个 Kafka 集群。
    </div>

    <template v-else>
      <div v-if="topicStore.info || topicStore.error" class="page-alerts">
        <p v-if="topicStore.info" class="success">{{ topicStore.info }}</p>
        <p v-if="topicStore.error" class="error">{{ topicStore.error }}</p>
      </div>

      <div class="page-grid topic-grid">
        <div class="panel panel-scroll">
          <div class="toolbar">
            <input v-model="topicStore.search" placeholder="搜索 Topic..." />
            <label class="checkbox">
              <input v-model="topicStore.hideInternal" type="checkbox" />
              隐藏内部 Topic
            </label>
          </div>
          <div class="panel-body">
            <table class="data-table topic-table">
              <thead>
                <tr>
                  <th>Topic</th>
                  <th>分区</th>
                  <th>操作</th>
                </tr>
              </thead>
              <tbody>
                <tr
                  v-for="topic in topicStore.filteredTopics"
                  :key="topic.name"
                  :class="{ selected: topicStore.selectedTopic === topic.name }"
                >
                  <td :title="topic.name">{{ topic.name }}</td>
                  <td>{{ topic.partitionCount }}</td>
                  <td>
                    <div class="row-actions">
                      <button
                        type="button"
                        class="secondary"
                        @click="openDetail(topic.name)"
                      >
                        详情
                      </button>
                      <button
                        type="button"
                        class="secondary"
                        @click="goMessages(topic.name)"
                      >
                        消息
                      </button>
                      <button
                        type="button"
                        class="danger"
                        @click="deletingTopic = topic.name"
                      >
                        删除
                      </button>
                    </div>
                  </td>
                </tr>
              </tbody>
            </table>
            <p v-if="topicStore.loading" class="muted">加载中...</p>
            <p v-else-if="!topicStore.filteredTopics.length" class="muted">
              没有匹配的 Topic。
            </p>
          </div>
        </div>

        <div class="panel panel-scroll">
          <div class="panel-body">
            <template v-if="topicStore.detail">
              <h2>{{ topicStore.detail.name }}</h2>
              <p class="muted">
                分区 {{ topicStore.detail.partitionCount }} ·
                {{ topicStore.detail.internal ? "内部" : "用户" }}
              </p>
              <table class="data-table partition-table">
                <thead>
                  <tr>
                    <th>分区</th>
                    <th>Leader</th>
                    <th>副本</th>
                    <th>ISR</th>
                    <th>起始</th>
                    <th>末尾</th>
                  </tr>
                </thead>
                <tbody>
                  <tr
                    v-for="partition in topicStore.detail.partitions"
                    :key="partition.id"
                  >
                    <td>{{ partition.id }}</td>
                    <td>{{ partition.leader }}</td>
                    <td>{{ partition.replicas.join(",") }}</td>
                    <td>{{ partition.isr.join(",") }}</td>
                    <td>{{ partition.beginningOffset ?? "-" }}</td>
                    <td>{{ partition.endOffset ?? "-" }}</td>
                  </tr>
                </tbody>
              </table>

              <h3>删除 Offset 之前的消息</h3>
              <p class="muted">
                将日志起始 Offset 推进到目标值，删除该 Offset
                之前的记录（不是随机删单条）。
              </p>
              <div class="form-inline">
                <label>
                  分区
                  <select v-model.number="deleteRecordsForm.partition">
                    <option
                      v-for="partition in topicStore.detail.partitions"
                      :key="partition.id"
                      :value="partition.id"
                    >
                      {{ partition.id }}
                    </option>
                  </select>
                </label>
                <label>
                  目标 Offset 之前
                  <input
                    v-model.number="deleteRecordsForm.beforeOffset"
                    type="number"
                    min="0"
                  />
                </label>
              </div>
              <label>
                确认文本（须完全一致）
                <input
                  v-model="deleteRecordsForm.confirmationText"
                  :placeholder="expectedDeleteRecordsConfirm"
                />
              </label>
              <p class="muted">期望：{{ expectedDeleteRecordsConfirm }}</p>
              <button type="button" class="danger" @click="submitDeleteRecords">
                删除 Offset 之前的消息
              </button>
            </template>
            <p v-else class="muted">在左侧选择 Topic 查看详情。</p>
          </div>
        </div>
      </div>
    </template>

    <div v-if="showCreate" class="modal-mask" @click.self="showCreate = false">
      <form class="panel modal" @submit.prevent="submitCreate">
        <h2>新建 Topic</h2>
        <label>
          名称
          <input v-model="createForm.name" required />
        </label>
        <label>
          分区数
          <input
            v-model.number="createForm.partitions"
            type="number"
            min="1"
            required
          />
        </label>
        <label>
          副本因子
          <input
            v-model.number="createForm.replicationFactor"
            type="number"
            min="1"
            required
          />
        </label>
        <label>
          清理策略
          <select v-model="createForm.cleanupPolicy">
            <option value="delete">delete</option>
            <option value="compact">compact</option>
            <option value="compact,delete">compact,delete</option>
          </select>
        </label>
        <label>
          retention.ms（可选）
          <input v-model.number="createForm.retentionMs" type="number" />
        </label>
        <div class="actions">
          <button type="button" class="secondary" @click="showCreate = false">
            取消
          </button>
          <button type="submit">创建</button>
        </div>
      </form>
    </div>

    <div
      v-if="deletingTopic"
      class="modal-mask"
      @click.self="deletingTopic = null"
    >
      <form class="panel modal" @submit.prevent="confirmDelete">
        <h2>删除 Topic</h2>
        <p>
          危险操作：请输入 Topic 名称
          <strong>{{ deletingTopic }}</strong>
          以确认删除。
        </p>
        <label>
          确认名称
          <input v-model="deleteConfirmName" required />
        </label>
        <div class="actions">
          <button
            type="button"
            class="secondary"
            @click="
              deletingTopic = null;
              deleteConfirmName = '';
            "
          >
            取消
          </button>
          <button type="submit" class="danger">确认删除</button>
        </div>
      </form>
    </div>
  </section>
</template>
