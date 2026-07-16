<script setup lang="ts">
import { onMounted, reactive, ref } from "vue";
import { useConnectionStore } from "../stores/connection";
import type { ConnectionProfile, SaveConnectionRequest, SecurityProtocol } from "../types";

const connectionStore = useConnectionStore();

const showForm = ref(false);
const editingId = ref<string | null>(null);
const form = reactive<SaveConnectionRequest>({
  name: "",
  bootstrapServers: "localhost:9092",
  securityProtocol: "PLAINTEXT",
  clientId: "kafkalite",
  requestTimeoutMs: 10000,
});

onMounted(() => {
  connectionStore.refresh();
});

function openCreate() {
  resetForm();
  showForm.value = true;
}

function closeForm() {
  showForm.value = false;
  resetForm();
}

function resetForm() {
  editingId.value = null;
  form.id = undefined;
  form.name = "";
  form.bootstrapServers = "localhost:9092";
  form.securityProtocol = "PLAINTEXT";
  form.saslMechanism = undefined;
  form.username = undefined;
  form.password = undefined;
  form.sslCaPath = undefined;
  form.sslCertificatePath = undefined;
  form.sslKeyPath = undefined;
  form.clientId = "kafkalite";
  form.requestTimeoutMs = 10000;
}

function edit(profileId: string) {
  const profile = connectionStore.connections.find(
    (item: ConnectionProfile) => item.id === profileId,
  );
  if (!profile) {
    return;
  }
  editingId.value = profile.id;
  form.id = profile.id;
  form.name = profile.name;
  form.bootstrapServers = profile.bootstrapServers.join(",");
  form.securityProtocol = profile.securityProtocol as SecurityProtocol;
  form.saslMechanism = profile.saslMechanism;
  form.username = profile.username;
  form.password = undefined;
  form.sslCaPath = profile.sslCaPath;
  form.sslCertificatePath = profile.sslCertificatePath;
  form.sslKeyPath = profile.sslKeyPath;
  form.clientId = profile.clientId;
  form.requestTimeoutMs = profile.requestTimeoutMs;
  showForm.value = true;
}

async function submit() {
  await connectionStore.save({ ...form });
  closeForm();
}

async function testCurrent() {
  if (editingId.value) {
    await connectionStore.test(editingId.value);
  } else {
    await connectionStore.test(undefined, { ...form });
  }
}
</script>

<template>
  <section class="page">
    <header class="page-header">
      <div>
        <h1>连接管理</h1>
        <p>保存集群配置、测试连通性并激活连接。</p>
      </div>
      <div class="header-right">
        <span
          class="conn-badge"
          :class="{ on: !!connectionStore.activeConnection }"
        >
          {{
            connectionStore.activeConnection
              ? `已连接 · ${connectionStore.activeConnection.name}`
              : "未连接"
          }}
        </span>
        <button type="button" @click="openCreate">新建连接</button>
      </div>
    </header>

    <div class="panel list-panel-full">
      <h2>已保存连接</h2>
      <ul class="connection-list">
        <li v-for="item in connectionStore.connections" :key="item.id">
          <div>
            <strong>{{ item.name }}</strong>
            <small>{{ item.bootstrapServers.join(", ") }}</small>
            <small>{{ item.securityProtocol }}</small>
          </div>
          <div class="row-actions">
            <button type="button" class="secondary" @click="edit(item.id)">
              编辑
            </button>
            <button
              type="button"
              class="secondary"
              @click="connectionStore.test(item.id)"
            >
              测试
            </button>
            <button
              v-if="connectionStore.activeConnection?.id !== item.id"
              type="button"
              @click="connectionStore.connect(item.id)"
            >
              连接
            </button>
            <button
              v-else
              type="button"
              class="danger"
              @click="connectionStore.disconnect(item.id)"
            >
              断开
            </button>
            <button
              type="button"
              class="danger"
              @click="connectionStore.remove(item.id)"
            >
              删除
            </button>
          </div>
        </li>
      </ul>
      <p v-if="!connectionStore.connections.length" class="muted">
        暂无连接，点击右上角「新建连接」。
      </p>
      <p v-if="connectionStore.testResult" class="success">
        {{ connectionStore.testResult }}
      </p>
      <p v-if="connectionStore.error" class="error">
        {{ connectionStore.error }}
      </p>
    </div>

    <div v-if="showForm" class="modal-mask" @click.self="closeForm">
      <form class="panel modal modal-wide form-panel" @submit.prevent="submit">
        <h2>{{ editingId ? "编辑连接" : "新建连接" }}</h2>
        <label>
          名称
          <input v-model="form.name" required placeholder="dev-cluster" />
        </label>
        <label>
          Bootstrap Servers
          <input
            v-model="form.bootstrapServers"
            required
            placeholder="localhost:9092,localhost:9093"
          />
        </label>
        <label>
          安全协议
          <select v-model="form.securityProtocol">
            <option value="PLAINTEXT">PLAINTEXT</option>
            <option value="SSL">SSL</option>
            <option value="SASL_PLAINTEXT">SASL_PLAINTEXT</option>
            <option value="SASL_SSL">SASL_SSL</option>
          </select>
        </label>
        <label v-if="form.securityProtocol.includes('SASL')">
          SASL 机制
          <select v-model="form.saslMechanism">
            <option value="PLAIN">PLAIN</option>
            <option value="SCRAM-SHA-256">SCRAM-SHA-256</option>
            <option value="SCRAM-SHA-512">SCRAM-SHA-512</option>
          </select>
        </label>
        <label v-if="form.securityProtocol.includes('SASL')">
          用户名
          <input v-model="form.username" />
        </label>
        <label v-if="form.securityProtocol.includes('SASL')">
          密码
          <input v-model="form.password" type="password" />
        </label>
        <label v-if="form.securityProtocol.includes('SSL')">
          SSL CA Path
          <input v-model="form.sslCaPath" placeholder="C:\\certs\\ca.pem" />
        </label>
        <label v-if="form.securityProtocol.includes('SSL')">
          SSL Certificate Path
          <input v-model="form.sslCertificatePath" />
        </label>
        <label v-if="form.securityProtocol.includes('SSL')">
          SSL Key Path
          <input v-model="form.sslKeyPath" />
        </label>
        <label>
          Client ID
          <input v-model="form.clientId" />
        </label>
        <label>
          超时 (ms)
          <input
            v-model.number="form.requestTimeoutMs"
            type="number"
            min="1000"
          />
        </label>
        <div class="actions">
          <button type="button" class="secondary" @click="closeForm">
            取消
          </button>
          <button type="button" class="secondary" @click="testCurrent">
            测试连接
          </button>
          <button type="submit" :disabled="connectionStore.loading">保存</button>
        </div>
        <p v-if="connectionStore.testResult" class="success">
          {{ connectionStore.testResult }}
        </p>
        <p v-if="connectionStore.error" class="error">
          {{ connectionStore.error }}
        </p>
      </form>
    </div>
  </section>
</template>
