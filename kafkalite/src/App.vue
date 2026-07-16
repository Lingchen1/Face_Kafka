<script setup lang="ts">
import { RouterView } from "vue-router";
import SideNav from "./components/SideNav.vue";
import StatusBar from "./components/StatusBar.vue";
</script>

<template>
  <div class="app-shell">
    <SideNav />
    <div class="main-area">
      <main class="content-host">
        <RouterView />
      </main>
      <StatusBar />
    </div>
  </div>
</template>

<style src="./styles/app.css"></style>
