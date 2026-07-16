export type SecurityProtocol =
  | "PLAINTEXT"
  | "SSL"
  | "SASL_PLAINTEXT"
  | "SASL_SSL";

export type SaslMechanism = "PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512";

export interface ConnectionProfile {
  id: string;
  name: string;
  bootstrapServers: string[];
  securityProtocol: SecurityProtocol;
  saslMechanism?: SaslMechanism;
  username?: string;
  credentialRef?: string;
  sslCaPath?: string;
  sslCertificatePath?: string;
  sslKeyPath?: string;
  clientId: string;
  requestTimeoutMs: number;
}

export interface SaveConnectionRequest {
  id?: string;
  name: string;
  bootstrapServers: string;
  securityProtocol: SecurityProtocol;
  saslMechanism?: SaslMechanism;
  username?: string;
  password?: string;
  sslCaPath?: string;
  sslCertificatePath?: string;
  sslKeyPath?: string;
  clientId?: string;
  requestTimeoutMs?: number;
}

export interface TestConnectionResult {
  success: boolean;
  latencyMs: number;
  brokerCount: number;
  clusterId?: string;
  warnings: string[];
}

export interface TopicSummary {
  name: string;
  partitionCount: number;
  internal: boolean;
}

export interface PartitionInfo {
  id: number;
  leader: number;
  replicas: number[];
  isr: number[];
  beginningOffset?: number;
  endOffset?: number;
}

export interface TopicDetail {
  name: string;
  partitionCount: number;
  internal: boolean;
  partitions: PartitionInfo[];
}

export interface CreateTopicRequest {
  connectionId: string;
  name: string;
  partitions: number;
  replicationFactor: number;
  cleanupPolicy?: string;
  retentionMs?: number;
  retentionBytes?: number;
}

export interface DeleteRecordsTarget {
  partition: number;
  beforeOffset: number;
}

export interface DeleteRecordsRequest {
  connectionId: string;
  topic: string;
  targets: DeleteRecordsTarget[];
  confirmationText: string;
}

export interface DeleteRecordsResultItem {
  partition: number;
  beforeOffset: number;
  newBeginningOffset?: number;
  success: boolean;
  error?: string;
}

export interface ApiError {
  code: string;
  message: string;
  detail?: string;
  retryable: boolean;
  operationId: string;
}

export interface ApiResult<T> {
  ok: boolean;
  data?: T;
  error?: ApiError;
}

export interface MessageDto {
  topic: string;
  partition: number;
  offset: number;
  timestamp?: number;
  key?: string;
  valuePreview: string;
  valueEncoding: "utf8" | "hex" | "base64" | string;
  valueSize: number;
  truncated: boolean;
  headers: Array<{ key: string; value?: string }>;
}

export interface ConsumeRequest {
  connectionId: string;
  topic: string;
  partitions: number[] | "all";
  start: {
    type: "earliest" | "latest" | "offset" | "timestamp";
    value?: number;
  };
  limit: number;
  batchSize?: number;
  idleTimeoutMs?: number;
  maxPreviewBytes?: number;
}

export type SessionEvent =
  | { type: "started"; sessionId: string }
  | { type: "batch"; sessionId: string; messages: MessageDto[] }
  | { type: "progress"; sessionId: string; count: number }
  | {
      type: "finished";
      sessionId: string;
      reason: "limit" | "idle" | "cancelled" | string;
    }
  | { type: "error"; sessionId: string; error: ApiError };
