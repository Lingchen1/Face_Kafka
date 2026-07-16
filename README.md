# Face_Kafka / KafkaLite

一款轻量、高性能的 Kafka 桌面连接工具。

技术栈：**Tauri 2 + Vue 3 + TypeScript + Rust + rust-rdkafka**

## 快速开始

### 使用便携版（Windows）

直接运行：

```text
kafkalite/release/KafkaLite.exe
```

详细说明见：[kafkalite/使用文档.md](./kafkalite/使用文档.md)

### 开发

```powershell
cd kafkalite
pnpm install
pnpm tauri dev
```

打包：

```powershell
cd kafkalite
pnpm tauri build
```

## 主要功能

- 连接管理（PLAINTEXT / SSL / SASL）
- Topic 浏览、创建、删除
- 消息查询（最早 / 最新 / Offset / 时间戳）
- 删除 Offset 之前的消息（DeleteRecords）
- Tombstone 发送（compact Topic）

## 目录

```text
Face_Kafka/
├── kafkalite/                 # 应用源码与便携 exe
│   ├── release/KafkaLite.exe
│   ├── 使用文档.md
│   ├── src/                   # Vue 前端
│   └── src-tauri/             # Rust 后端
├── KafkaLite_Tauri2_Vue3_Rust_开发方案.docx
└── README.md
```
