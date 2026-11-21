import { assert, it } from "@effect/vitest";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  SecretProviderId,
  ServerSettingsError,
  ThreadId,
  type OrchestrationV2ProviderThread,
} from "@t3tools/contracts";
import { BoxApi } from "@t3tools/provider-upstash-box/server";
import * as SecretResolver from "@t3tools/secret-provider-core/server/SecretResolver";
import * as ConfigProvider from "effect/ConfigProvider";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { vi } from "vite-plus/test";

import * as ServerSettings from "../serverSettings.ts";
import * as BoxThreadCleanup from "./BoxThreadCleanup.ts";

const scenarios: ReadonlyArray<{
  driver: string;
  fallback?: boolean;
  invalidRef?: boolean;
  noIdentity?: boolean;
  key?:
    | "global"
    | "legacy"
    | "instance"
    | "secret"
    | "secret_missing"
    | "missing"
    | "settings_failure";
  status?: number;
  failsAt?: "get" | "delete";
  reason?: BoxThreadCleanup.BoxThreadCleanupError["reason"];
}> = [
  { driver: "upstashBox" },
  { driver: "boxCommandCode" },
  { driver: "boxClaudeCode" },
  { driver: "boxCodex" },
  { driver: "upstashBox", fallback: true },
  { driver: "upstashBox", key: "global" },
  { driver: "upstashBox", key: "legacy" },
  { driver: "boxCodex", key: "instance" },
  { driver: "upstashBox", key: "secret" },
  { driver: "upstashBox", key: "secret_missing", reason: "credentials_unavailable" },
  { driver: "upstashBox", key: "missing", reason: "credentials_unavailable" },
  { driver: "upstashBox", key: "settings_failure", reason: "credentials_unavailable" },
  { driver: "upstashBox", invalidRef: true, reason: "invalid_reference" },
  { driver: "upstashBox", fallback: true, noIdentity: true, reason: "invalid_reference" },
  { driver: "upstashBox", status: 404, failsAt: "get" },
  { driver: "upstashBox", status: 404, failsAt: "delete" },
  { driver: "upstashBox", status: 503, failsAt: "get", reason: "delete_failed" },
  { driver: "upstashBox", status: 503, failsAt: "delete", reason: "delete_failed" },
];

it.effect.each(scenarios)(
  "deletes historical Box references without an active adapter: %j",
  (scenario) => {
    const threadId = ThreadId.make("deleted-thread");
    const instanceId = ProviderInstanceId.make("removed-instance");
    const driver = ProviderDriverKind.make(scenario.driver);
    const now = DateTime.makeUnsafe(0);
    const thread: OrchestrationV2ProviderThread = {
      id: ProviderThreadId.make("historical-provider-thread"),
      driver,
      providerInstanceId: instanceId,
      providerSessionId: null,
      appThreadId: scenario.noIdentity ? null : threadId,
      ownerNodeId: null,
      nativeThreadRef: scenario.fallback
        ? null
        : {
            driver: scenario.invalidRef ? ProviderDriverKind.make("other") : driver,
            nativeId: "persisted-box",
            strength: "strong",
          },
      nativeConversationHeadRef: null,
      status: "closed",
      firstRunOrdinal: null,
      lastRunOrdinal: null,
      handoffIds: [],
      forkedFrom: null,
      pendingBackgroundTasks: [],
      contextUsage: null,
      nativeMetadata: null,
      goal: null,
      createdAt: now,
      updatedAt: now,
    };
    const calls: string[] = [];
    const upstreamError = Object.assign(new Error("Box deletion refused for private-key"), {
      statusCode: scenario.status,
      headers: { Authorization: "private upstream details" },
      apiKey: "private-key",
      cause: { request: { headers: { Authorization: "private upstream details" } } },
    });
    const unexpected = async (): Promise<never> => {
      throw new Error("Unexpected Box operation");
    };
    const box: BoxApi.BoxHandle = {
      id: "persisted-box",
      agent: { stream: unexpected },
      exec: { command: unexpected },
      files: { write: unexpected },
      resume: unexpected,
      getStatus: unexpected,
      delete: async () => {
        calls.push("delete");
        if (scenario.failsAt === "delete") throw upstreamError;
      },
    };
    const key = scenario.key;
    const settings =
      key === "settings_failure"
        ? Layer.mock(ServerSettings.ServerSettingsService)({
            getSettings: Effect.fail(
              new ServerSettingsError({
                settingsPath: "test-settings.json",
                operation: "read-file",
                cause: upstreamError,
              }),
            ),
          })
        : ServerSettings.ServerSettingsService.layerTest({
            upstashBox: {
              apiKey:
                key === "global"
                  ? "private-key"
                  : key === "secret" || key === "secret_missing"
                    ? { provider: SecretProviderId.make("test"), reference: "box-key" }
                    : "",
            },
            providerInstances:
              key === "legacy" || key === "instance"
                ? {
                    [instanceId]: {
                      driver,
                      config: { apiKey: "private-key" },
                      environment:
                        key === "instance"
                          ? [{ name: "UPSTASH_BOX_API_KEY", value: "private-key", sensitive: true }]
                          : [],
                    },
                  }
                : {},
          });
    return Effect.gen(function* () {
      using _missingBox = vi
        .spyOn(BoxApi, "isMissingBox")
        .mockImplementation((error) => error === upstreamError && scenario.status === 404);
      const cleanup = yield* BoxThreadCleanup.BoxThreadCleanup;
      const result = yield* cleanup
        .remove({ threadId, providerThread: thread })
        .pipe(Effect.result);
      if (scenario.reason) {
        assert.equal(result._tag, "Failure");
        if (result._tag === "Failure") {
          assert.equal(result.failure.reason, scenario.reason);
          assert.equal(result.failure.providerThreadId, thread.id);
          assert.equal(
            result.failure.message,
            `Could not delete the thread Box (${scenario.reason}).${scenario.reason === "delete_failed" ? " Box deletion refused for [REDACTED]" : ""}`,
          );
          assert.notInclude(JSON.stringify(result.failure), "private upstream details");
          assert.notInclude(JSON.stringify(result.failure), "private-key");
          assert.notInclude(JSON.stringify(result.failure), "Authorization");
          if (scenario.reason === "delete_failed") {
            const diagnostic = {
              message: "Box deletion refused for [REDACTED]",
              statusCode: scenario.status,
              boxId: "persisted-box",
            };
            assert.deepEqual(result.failure.boxError, diagnostic);
            assert.isUndefined(result.failure.cause);
          } else if (key === "settings_failure") {
            assert.deepEqual(result.failure.cause, {
              message: "Server settings read-file failed at test-settings.json.",
            });
          } else {
            assert.isUndefined(result.failure.cause);
          }
        }
      } else assert.equal(result._tag, "Success");
      assert.deepEqual(
        calls,
        scenario.reason === "invalid_reference" ||
          key === "missing" ||
          key === "secret_missing" ||
          key === "settings_failure"
          ? []
          : scenario.failsAt === "get"
            ? ["get"]
            : ["get", "delete"],
      );
    }).pipe(
      Effect.provide(
        BoxThreadCleanup.layer.pipe(
          Layer.provideMerge(
            Layer.mergeAll(
              settings,
              Layer.mock(SecretResolver.SecretResolver)({
                read: () => Effect.succeed(key === "secret_missing" ? [] : ["private-key"]),
              }),
              Layer.succeed(BoxApi.BoxApi, {
                get: async (id, apiKey) => {
                  calls.push("get");
                  assert.equal(
                    id,
                    scenario.fallback
                      ? `t3-${Buffer.from(threadId).toString("hex")}`
                      : "persisted-box",
                  );
                  assert.equal(apiKey, "private-key");
                  if (scenario.failsAt === "get") throw upstreamError;
                  return box;
                },
                create: unexpected,
                list: unexpected,
                checkModelKey: unexpected,
              }),
              ConfigProvider.layer(
                ConfigProvider.fromUnknown({ UPSTASH_BOX_API_KEY: key ? "" : "private-key" }),
              ),
            ),
          ),
        ),
      ),
    );
  },
);
