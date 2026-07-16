<script setup lang="ts">
import { computed } from "vue";
import { useConnectionStore } from "../stores/connection";

const connectionStore = useConnectionStore();

const statusText = computed(() => {
  const active = connectionStore.activeConnection;
  if (!active) {
    return "未连接";
  }
  return `当前集群：${active.name}`;
});
</script>

<template>
  <footer class="status-bar">
    <span>{{ statusText }}</span>
    <span v-if="connectionStore.loading">处理中...</span>
    <span v-if="connectionStore.error" class="error">{{
      connectionStore.error
    }}</span>
  </footer>
</template>
