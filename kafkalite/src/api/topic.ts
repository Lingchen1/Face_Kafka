import { invoke } from "@tauri-apps/api/core";
import type {
  ApiResult,
  CreateTopicRequest,
  DeleteRecordsRequest,
  DeleteRecordsResultItem,
  TopicDetail,
  TopicSummary,
} from "../types";

function unwrap<T>(result: ApiResult<T>): T {
  if (result.ok && result.data !== undefined) {
    return result.data;
  }
  throw new Error(result.error?.message ?? "Unknown error");
}

export async function listTopics(connectionId: string): Promise<TopicSummary[]> {
  return unwrap(
    await invoke<ApiResult<TopicSummary[]>>("list_topics", { connectionId }),
  );
}

export async function describeTopic(
  connectionId: string,
  topicName: string,
): Promise<TopicDetail> {
  return unwrap(
    await invoke<ApiResult<TopicDetail>>("describe_topic", {
      connectionId,
      topicName,
    }),
  );
}

export async function createTopic(
  request: CreateTopicRequest,
): Promise<TopicSummary> {
  return unwrap(
    await invoke<ApiResult<TopicSummary>>("create_topic", { request }),
  );
}

export async function deleteTopic(
  connectionId: string,
  topicName: string,
  confirmationText: string,
): Promise<void> {
  return unwrap(
    await invoke<ApiResult<void>>("delete_topic", {
      connectionId,
      topicName,
      confirmationText,
    }),
  );
}

export async function deleteRecordsBefore(
  request: DeleteRecordsRequest,
): Promise<DeleteRecordsResultItem[]> {
  return unwrap(
    await invoke<ApiResult<DeleteRecordsResultItem[]>>("delete_records_before", {
      request,
    }),
  );
}
