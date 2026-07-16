# KafkaLite

轻量级 Kafka 桌面客户端，基于 **Tauri 2 + Vue 3 + Rust + rust-rdkafka**。

使用说明见：[使用文档.md](./使用文档.md)

便携版 exe：`release/KafkaLite.exe`

## 功能（MVP）

- 连接管理：PLAINTEXT / SSL / SASL，保存、测试、激活、断开
- Topic：列表、详情、创建、删除（二次确认）
- 消息查询：earliest / latest / offset / timestamp，批量 Channel 推送，可停止
- 虚拟列表预览、JSON/Raw 详情、本地筛选、导出 JSONL
- 删除 Offset 之前的记录（DeleteRecords）
- Tombstone 发送（compact Topic）
- SQLite 本地配置 + 审计日志

## 开发环境

- Node.js LTS + pnpm
- Rust stable
- CMake + Microsoft C++ Build Tools（编译 librdkafka）
- WebView2 Runtime

## 启动

```powershell
$env:Path = "C:\Program Files\CMake\bin;$env:USERPROFILE\.cargo\bin;D:\app\ClaudeCode;" + $env:Path
cd d:\projects\Face_KafkaConnect\kafkalite
pnpm install
pnpm tauri dev
```

## 本地 Kafka（可选）

```powershell
docker compose up -d
```

默认 PLAINTEXT：`localhost:9092`。

## 打包

```powershell
pnpm tauri build
```

产物在 `src-tauri/target/release/bundle/`。

## SSL / SASL 说明

连接表单已支持 SSL/SASL 配置。当前 Windows 默认构建未开启 `ssl-vendored`（需安装 Perl 才能编译 OpenSSL）。若需要生产 SSL/SASL：

1. 安装 [Strawberry Perl](https://strawberryperl.com/)
2. 在 `src-tauri/Cargo.toml` 为 `rdkafka` 增加 feature：`ssl-vendored`
3. 重新 `cargo build` / `pnpm tauri build`

## 删除语义说明

Kafka 不支持随机物理删除单条消息。本工具中的「删除消息」实现为：

1. 删除整个 Topic
2. 删除某分区指定 Offset **之前**的记录（DeleteRecords）
3. 对 compact Topic 发送 Tombstone（按 Key 逻辑删除）
