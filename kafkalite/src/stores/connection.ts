import { defineStore } from "pinia";
import { ref } from "vue";
import * as connectionApi from "../api/connection";
import type { ConnectionProfile, SaveConnectionRequest } from "../types";

export const useConnectionStore = defineStore("connection", () => {
  const connections = ref<ConnectionProfile[]>([]);
  const activeConnection = ref<ConnectionProfile | null>(null);
  const loading = ref(false);
  const error = ref<string | null>(null);
  const testResult = ref<string | null>(null);

  async function refresh() {
    loading.value = true;
    error.value = null;
    try {
      connections.value = await connectionApi.listConnections();
      activeConnection.value = await connectionApi.getActiveConnection();
    } catch (err) {
      error.value = err instanceof Error ? err.message : String(err);
    } finally {
      loading.value = false;
    }
  }

  async function save(request: SaveConnectionRequest) {
    loading.value = true;
    error.value = null;
    try {
      const saved = await connectionApi.saveConnection(request);
      await refresh();
      return saved;
    } catch (err) {
      error.value = err instanceof Error ? err.message : String(err);
      throw err;
    } finally {
      loading.value = false;
    }
  }

  async function remove(id: string) {
    loading.value = true;
    error.value = null;
    try {
      await connectionApi.deleteConnection(id);
      if (activeConnection.value?.id === id) {
        activeConnection.value = null;
      }
      await refresh();
    } catch (err) {
      error.value = err instanceof Error ? err.message : String(err);
    } finally {
      loading.value = false;
    }
  }

  async function test(profileId?: string, request?: SaveConnectionRequest) {
    loading.value = true;
    error.value = null;
    testResult.value = null;
    try {
      const result = await connectionApi.testConnection({ profileId, request });
      testResult.value = `成功 · ${result.latencyMs}ms · ${result.brokerCount} brokers${
        result.clusterId ? ` · ${result.clusterId}` : ""
      }`;
      return result;
    } catch (err) {
      error.value = err instanceof Error ? err.message : String(err);
      throw err;
    } finally {
      loading.value = false;
    }
  }

  async function connect(profileId: string) {
    loading.value = true;
    error.value = null;
    try {
      activeConnection.value = await connectionApi.connectCluster(profileId);
    } catch (err) {
      error.value = err instanceof Error ? err.message : String(err);
    } finally {
      loading.value = false;
    }
  }

  async function disconnect(profileId: string) {
    loading.value = true;
    error.value = null;
    try {
      await connectionApi.disconnectCluster(profileId);
      activeConnection.value = null;
    } catch (err) {
      error.value = err instanceof Error ? err.message : String(err);
    } finally {
      loading.value = false;
    }
  }

  return {
    connections,
    activeConnection,
    loading,
    error,
    testResult,
    refresh,
    save,
    remove,
    test,
    connect,
    disconnect,
  };
});
