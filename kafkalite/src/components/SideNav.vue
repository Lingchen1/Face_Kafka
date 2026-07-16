<script setup lang="ts">
import { RouterLink } from "vue-router";
</script>

<template>
  <nav class="side-nav">
    <div class="brand">
      <span class="brand-mark">K</span>
      <div>
        <strong>KafkaLite</strong>
        <small>Desktop Client</small>
      </div>
    </div>
    <RouterLink to="/connections" class="nav-item">连接</RouterLink>
    <RouterLink to="/topics" class="nav-item">Topic</RouterLink>
    <RouterLink to="/messages" class="nav-item">消息</RouterLink>
  </nav>
</template>
