import { invoke } from "@tauri-apps/api/core";
import type {
  ApiResult,
  ConnectionProfile,
  SaveConnectionRequest,
  TestConnectionResult,
} from "../types";

function unwrap<T>(result: ApiResult<T>): T {
  if (result.ok && result.data !== undefined) {
    return result.data;
  }
  const error = result.error;
  throw new Error(error?.message ?? "Unknown error");
}

export async function listConnections(): Promise<ConnectionProfile[]> {
  return unwrap(await invoke<ApiResult<ConnectionProfile[]>>("list_connections"));
}

export async function saveConnection(
  request: SaveConnectionRequest,
): Promise<ConnectionProfile> {
  return unwrap(
    await invoke<ApiResult<ConnectionProfile>>("save_connection", { request }),
  );
}

export async function deleteConnection(id: string): Promise<void> {
  return unwrap(await invoke<ApiResult<void>>("delete_connection", { id }));
}

export async function testConnection(options: {
  profileId?: string;
  request?: SaveConnectionRequest;
}): Promise<TestConnectionResult> {
  return unwrap(
    await invoke<ApiResult<TestConnectionResult>>("test_connection", {
      profileId: options.profileId ?? null,
      request: options.request ?? null,
    }),
  );
}

export async function connectCluster(
  profileId: string,
): Promise<ConnectionProfile> {
  return unwrap(
    await invoke<ApiResult<ConnectionProfile>>("connect_cluster", {
      profileId,
    }),
  );
}

export async function disconnectCluster(profileId: string): Promise<void> {
  return unwrap(
    await invoke<ApiResult<void>>("disconnect_cluster", { profileId }),
  );
}

export async function getActiveConnection(): Promise<ConnectionProfile | null> {
  return unwrap(
    await invoke<ApiResult<ConnectionProfile | null>>("get_active_connection"),
  );
}
