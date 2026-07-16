import { Channel, invoke } from "@tauri-apps/api/core";
import type {
  ApiResult,
  ConsumeRequest,
  SessionEvent,
} from "../types";

function unwrap<T>(result: ApiResult<T>): T {
  if (result.ok && result.data !== undefined) {
    return result.data;
  }
  throw new Error(result.error?.message ?? "Unknown error");
}

export async function startConsume(
  request: ConsumeRequest,
  onEvent: (event: SessionEvent) => void,
): Promise<string> {
  const channel = new Channel<SessionEvent>();
  channel.onmessage = onEvent;
  const result = unwrap(
    await invoke<ApiResult<{ sessionId: string }>>("start_consume", {
      request,
      onEvent: channel,
    }),
  );
  return result.sessionId;
}

export async function stopConsume(sessionId: string): Promise<void> {
  return unwrap(
    await invoke<ApiResult<void>>("stop_consume", { sessionId }),
  );
}

export async function produceTombstone(request: {
  connectionId: string;
  topic: string;
  key: string;
  partition?: number;
}): Promise<{ topic: string; partition: number; offset: number }> {
  return unwrap(
    await invoke<ApiResult<{ topic: string; partition: number; offset: number }>>(
      "produce_tombstone",
      { request },
    ),
  );
}
