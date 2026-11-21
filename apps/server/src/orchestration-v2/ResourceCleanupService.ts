import { isUpstashBoxDriver } from "@t3tools/provider-upstash-box/harnesses";
import { BoxApi } from "@t3tools/provider-upstash-box/server";
import {
  ThreadId,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ThreadShell,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";

import { resolveAttachmentRelativePath } from "../attachmentPaths.ts";
import { parseAttachmentFileExtension, resolveAttachmentPathById } from "../attachmentStore.ts";
import * as ServerConfig from "../config.ts";
import * as DeviceService from "../device/DeviceService.ts";
import * as PreviewManager from "../preview/Manager.ts";
import * as ScheduledTaskService from "../scheduledTasks/ScheduledTaskService.ts";
import { forkParked } from "../serverActivation.ts";
import * as TerminalManager from "../terminal/Manager.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as BoxThreadCleanup from "./BoxThreadCleanup.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ThreadCommandExecutor from "./ThreadCommandExecutor.ts";
import { threadHasQueuedTurnStart } from "./ThreadSettlementService.ts";

export const cleanupBoxThreads = Effect.fn("ResourceCleanupService.cleanupBoxThreads")(
  function* (input: {
    readonly threadId: ThreadId;
    readonly deleted: boolean;
    readonly providerThreads: ReadonlyArray<OrchestrationV2ProviderThread>;
  }) {
    if (!input.deleted) return;
    const cleanup = yield* BoxThreadCleanup.BoxThreadCleanup;
    const [, failures] = yield* Effect.partition(
      input.providerThreads.filter(
        (thread) => isUpstashBoxDriver(thread.driver) || thread.driver === "upstashBox",
      ),
      (providerThread) => cleanup.remove({ threadId: input.threadId, providerThread }),
      { concurrency: 1 },
    );
    if (failures[0]) return yield* failures[0];
  },
);

class BoxCleanup extends Context.Reference<
  (threadId: ThreadId) => Effect.Effect<void, BoxThreadCleanup.BoxThreadCleanupError>
>("t3/orchestration-v2/BoxCleanup", { defaultValue: () => () => Effect.void }) {}

export const layerBoxCleanup = Layer.effect(
  BoxCleanup,
  Effect.gen(function* () {
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const boxes = yield* BoxThreadCleanup.BoxThreadCleanup;
    const cleanup = Effect.fn("ResourceCleanupService.cleanupBoxes")(function* (
      threadId: ThreadId,
    ) {
      const records = yield* projections.getThreadRecords(threadId, ["providerThreads"]).pipe(
        Effect.mapError(
          (cause) =>
            new BoxThreadCleanup.BoxThreadCleanupError({
              threadId,
              reason: "load_failed",
              cause: BoxApi.boxErrorDetails(cause),
            }),
        ),
      );
      yield* cleanupBoxThreads({
        threadId,
        deleted: records.thread.deletedAt !== null,
        providerThreads: records.providerThreads,
      }).pipe(Effect.provideService(BoxThreadCleanup.BoxThreadCleanup, boxes));
    });
    return cleanup;
  }),
);

// A recreated Box starts from a fresh clone without agent memory, so a settled thread keeps its
// Box while a follow-up is still likely.
const SETTLED_BOX_RETENTION = Duration.days(3);

/** When a settled, idle thread went quiet; null while it is unsettled or still has work running. */
function settledBoxQuietSince(thread: OrchestrationV2ThreadShell, now: number): number | null {
  if (
    thread.settledAt === null ||
    thread.activeRunId !== null ||
    thread.pendingRuntimeRequest !== null ||
    (thread.pendingBackgroundTasks?.length ?? 0) > 0 ||
    threadHasQueuedTurnStart(thread, now)
  )
    return null;
  return Math.max(
    ...[
      thread.settledAt,
      thread.createdAt,
      thread.latestUserMessageAt,
      thread.latestRunRequestedAt,
      thread.latestRunStartedAt,
      thread.latestRunCompletedAt,
    ].flatMap((value) => (value == null ? [] : [DateTime.toEpochMillis(value)])),
  );
}

/**
 * Deletes the Boxes of threads that stayed settled and quiet for the retention period, unless the
 * Box holds uncommitted or unpushed work. `released` remembers handled Boxes per quiet period.
 */
export const cleanupSettledBoxes = Effect.fn("ResourceCleanupService.cleanupSettledBoxes")(
  function* (released: Set<string>) {
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const adapters = yield* ProviderAdapterRegistry.ProviderAdapterRegistryV2;
    const dispatch = yield* ThreadCommandExecutor.ThreadCommandExecutor;
    const now = DateTime.toEpochMillis(yield* DateTime.now);
    const snapshot = yield* projections.getShellSnapshot({ location: "active" });
    for (const shell of snapshot.threads) {
      const quietSince = settledBoxQuietSince(shell, now);
      if (quietSince === null || now - quietSince < Duration.toMillis(SETTLED_BOX_RETENTION))
        continue;
      const records = yield* projections.getThreadRecords(shell.id, ["providerThreads"]);
      for (const thread of records.providerThreads) {
        const key = `${thread.id}@${quietSince}`;
        if (!isUpstashBoxDriver(thread.driver) || released.has(key)) continue;
        yield* dispatch
          .withLock(
            shell.id,
            Effect.gen(function* () {
              const adapter = yield* adapters.get(thread.providerInstanceId);
              const current = yield* projections.getThreadShell(shell.id);
              // Intake commits run requests under this same lock before their effects can start.
              if (current === null || settledBoxQuietSince(current, now) !== quietSince) return;
              if (!adapter.deleteIdleThread) return;
              if (yield* adapter.deleteIdleThread(thread)) released.add(key);
              else
                yield* Effect.logInfo("Kept a settled thread Box with unsaved work.", {
                  threadId: shell.id,
                });
            }),
          )
          .pipe(
            Effect.catchCause(() =>
              Effect.logWarning("Could not delete a settled thread Box.", { threadId: shell.id }),
            ),
          );
      }
    }
  },
);

export const layerSettledBoxCleanup = Layer.effectDiscard(
  Effect.gen(function* () {
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const adapters = yield* ProviderAdapterRegistry.ProviderAdapterRegistryV2;
    const dispatch = yield* ThreadCommandExecutor.ThreadCommandExecutor;
    const released = new Set<string>();
    yield* forkParked(
      cleanupSettledBoxes(released).pipe(
        Effect.provideService(ProjectionStore.ProjectionStoreV2, projections),
        Effect.provideService(ProviderAdapterRegistry.ProviderAdapterRegistryV2, adapters),
        Effect.provideService(ThreadCommandExecutor.ThreadCommandExecutor, dispatch),
        Effect.catchCause(() => Effect.logWarning("Could not sweep settled thread Boxes.")),
        Effect.repeat(Schedule.spaced(Duration.hours(1))),
      ),
    );
  }),
);

export class ResourceCleanupError extends Schema.TaggedError<ResourceCleanupError>()(
  "ResourceCleanupError",
  {
    operation: Schema.Literals([
      "terminal",
      "preview",
      "device",
      "attachment",
      "scheduled-tasks",
      "box",
    ]),
    threadId: Schema.optional(Schema.String),
    attachmentId: Schema.optional(Schema.String),
    cause: Schema.Defect(),
  },
) {}

export class ResourceCleanupService extends Context.Reference<{
  readonly cleanupScheduledTasks: (threadId: ThreadId) => Effect.Effect<void, ResourceCleanupError>;
  readonly cleanupTerminals: (threadId: string) => Effect.Effect<void, ResourceCleanupError>;
  readonly cleanupPreviews: (threadId: string) => Effect.Effect<void, ResourceCleanupError>;
  /** Releases the thread's device claims, as `device_close` does; the devices stay booted. */
  readonly cleanupDevices: (threadId: ThreadId) => Effect.Effect<void, ResourceCleanupError>;
  readonly cleanupAttachments: (
    attachmentIds: ReadonlyArray<string>,
  ) => Effect.Effect<void, ResourceCleanupError>;
}>("t3/orchestration-v2/ResourceCleanupService", {
  defaultValue: () => ({
    cleanupScheduledTasks: () => Effect.void,
    cleanupTerminals: () => Effect.void,
    cleanupPreviews: () => Effect.void,
    cleanupDevices: () => Effect.void,
    cleanupAttachments: () => Effect.void,
  }),
}) {}

export const layer = Layer.effect(
  ResourceCleanupService,
  Effect.gen(function* () {
    const terminals = yield* TerminalManager.TerminalManager;
    const previews = yield* PreviewManager.PreviewManager;
    const devices = yield* DeviceService.DeviceService;
    const fileSystem = yield* FileSystem.FileSystem;
    const config = yield* ServerConfig.ServerConfig;
    const scheduledTasks = yield* ScheduledTaskService.ScheduledTaskService;
    const cleanupBoxes = yield* BoxCleanup;
    return {
      cleanupScheduledTasks: Effect.fn("ResourceCleanupService.cleanupScheduledTasks")(function* (
        threadId: ThreadId,
      ) {
        const tasks = yield* Effect.exit(scheduledTasks.deleteForThread(threadId));
        yield* cleanupBoxes(threadId).pipe(
          Effect.mapError(
            (cause) => new ResourceCleanupError({ operation: "box", threadId, cause }),
          ),
        );
        if (Exit.isFailure(tasks))
          return yield* new ResourceCleanupError({
            operation: "scheduled-tasks",
            threadId,
            cause: tasks.cause,
          });
      }),
      cleanupTerminals: Effect.fn("ResourceCleanupService.cleanupTerminals")(function (
        threadId: string,
      ) {
        return terminals
          .close({ threadId, deleteHistory: true })
          .pipe(
            Effect.mapError(
              (cause) => new ResourceCleanupError({ operation: "terminal", threadId, cause }),
            ),
          );
      }),
      cleanupPreviews: Effect.fn("ResourceCleanupService.cleanupPreviews")(function (
        threadId: string,
      ) {
        return previews
          .close({ threadId: ThreadId.make(threadId) })
          .pipe(
            Effect.mapError(
              (cause) => new ResourceCleanupError({ operation: "preview", threadId, cause }),
            ),
          );
      }),
      cleanupDevices: Effect.fn("ResourceCleanupService.cleanupDevices")(function* (
        threadId: ThreadId,
      ) {
        // One close per session, so a reconnecting host does not keep the other hosts' claims.
        const sessions = yield* devices.sessionsForThread(threadId);
        const [, failures] = yield* Effect.partition(
          sessions,
          (session) =>
            devices.close({ threadId, hostId: session.hostId, deviceId: session.deviceId }),
          { concurrency: 1 },
        );
        const [failure] = failures;
        if (failure !== undefined)
          return yield* new ResourceCleanupError({ operation: "device", threadId, cause: failure });
      }),
      cleanupAttachments: Effect.fn("ResourceCleanupService.cleanupAttachments")(function (
        attachmentIds: ReadonlyArray<string>,
      ) {
        return Effect.forEach(
          attachmentIds,
          (attachmentId) => {
            const extension = parseAttachmentFileExtension(attachmentId);
            const path =
              resolveAttachmentPathById({
                attachmentsDir: config.attachmentsDir,
                attachmentId,
              }) ??
              (extension
                ? resolveAttachmentRelativePath({
                    attachmentsDir: config.attachmentsDir,
                    relativePath: `${attachmentId}.${extension}`,
                  })
                : null);
            return path === null
              ? Effect.void
              : fileSystem.remove(path, { force: true }).pipe(
                  Effect.andThen(fileSystem.remove(`${path}.txt`, { force: true })),
                  Effect.mapError(
                    (cause) =>
                      new ResourceCleanupError({ operation: "attachment", attachmentId, cause }),
                  ),
                );
          },
          { discard: true, concurrency: 4 },
        );
      }),
    };
  }),
);
