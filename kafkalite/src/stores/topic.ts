import { defineStore } from "pinia";
import { computed, ref } from "vue";
import * as topicApi from "../api/topic";
import type {
  CreateTopicRequest,
  DeleteRecordsRequest,
  TopicDetail,
  TopicSummary,
} from "../types";

export const useTopicStore = defineStore("topic", () => {
  const topics = ref<TopicSummary[]>([]);
  const selectedTopic = ref<string | null>(null);
  const detail = ref<TopicDetail | null>(null);
  const search = ref("");
  const hideInternal = ref(true);
  const loading = ref(false);
  const error = ref<string | null>(null);
  const info = ref<string | null>(null);

  const filteredTopics = computed(() => {
    const keyword = search.value.trim().toLowerCase();
    return topics.value.filter((topic) => {
      if (hideInternal.value && topic.internal) {
        return false;
      }
      if (!keyword) {
        return true;
      }
      return topic.name.toLowerCase().includes(keyword);
    });
  });

  async function refresh(connectionId: string) {
    loading.value = true;
    error.value = null;
    try {
      topics.value = await topicApi.listTopics(connectionId);
      if (
        selectedTopic.value &&
        !topics.value.some((item) => item.name === selectedTopic.value)
      ) {
        selectedTopic.value = null;
        detail.value = null;
      }
    } catch (err) {
      error.value = err instanceof Error ? err.message : String(err);
      topics.value = [];
    } finally {
      loading.value = false;
    }
  }

  async function select(connectionId: string, name: string) {
    selectedTopic.value = name;
    loading.value = true;
    error.value = null;
    try {
      detail.value = await topicApi.describeTopic(connectionId, name);
    } catch (err) {
      error.value = err instanceof Error ? err.message : String(err);
      detail.value = null;
    } finally {
      loading.value = false;
    }
  }

  async function create(request: CreateTopicRequest) {
    loading.value = true;
    error.value = null;
    info.value = null;
    try {
      const created = await topicApi.createTopic(request);
      info.value = `Topic 已创建：${created.name}`;
      await refresh(request.connectionId);
      return created;
    } catch (err) {
      error.value = err instanceof Error ? err.message : String(err);
      throw err;
    } finally {
      loading.value = false;
    }
  }

  async function remove(
    connectionId: string,
    topicName: string,
    confirmationText: string,
  ) {
    loading.value = true;
    error.value = null;
    info.value = null;
    try {
      await topicApi.deleteTopic(connectionId, topicName, confirmationText);
      info.value = `Topic 已删除：${topicName}`;
      if (selectedTopic.value === topicName) {
        selectedTopic.value = null;
        detail.value = null;
      }
      await refresh(connectionId);
    } catch (err) {
      error.value = err instanceof Error ? err.message : String(err);
      throw err;
    } finally {
      loading.value = false;
    }
  }

  async function deleteRecords(request: DeleteRecordsRequest) {
    loading.value = true;
    error.value = null;
    info.value = null;
    try {
      const results = await topicApi.deleteRecordsBefore(request);
      const okCount = results.filter((item) => item.success).length;
      info.value = `删除 Offset 之前记录完成：成功 ${okCount}/${results.length}`;
      if (selectedTopic.value === request.topic) {
        detail.value = await topicApi.describeTopic(
          request.connectionId,
          request.topic,
        );
      }
      return results;
    } catch (err) {
      error.value = err instanceof Error ? err.message : String(err);
      throw err;
    } finally {
      loading.value = false;
    }
  }

  return {
    topics,
    selectedTopic,
    detail,
    search,
    hideInternal,
    loading,
    error,
    info,
    filteredTopics,
    refresh,
    select,
    create,
    remove,
    deleteRecords,
  };
});
