/**
 * Serverless worker mode.
 *
 * For hosts without an always-on process (e.g. only Vercel), set
 * SERVERLESS_WORKER=true. The DM queue is then drained:
 *   - right after each Instagram webhook (instant DMs), and
 *   - by /api/cron/drain, called every few minutes by an external scheduler
 *     (GitHub Actions workflow in .github/workflows/drain.yml) for delayed
 *     jobs, retries and the missed-comment polling sweep.
 */
import os from "node:os";
import { createDMWorker } from "@/lib/queue/dm-worker";
import { getDMQueue } from "@/lib/queue/client";
import { recordWorkerHeartbeat } from "@/lib/ops/worker-health";

export function isServerlessWorker(): boolean {
  return process.env.SERVERLESS_WORKER === "true";
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function pendingJobs(): Promise<number> {
  const queue = getDMQueue();
  const counts = await queue.getJobCounts("waiting", "active", "prioritized");
  let pending =
    (counts.waiting ?? 0) + (counts.active ?? 0) + (counts.prioritized ?? 0);
  // Delayed jobs whose time has already come count as pending too.
  const delayed = await queue.getDelayed(0, 50);
  const now = Date.now();
  pending += delayed.filter((j) => j.timestamp + (j.delay ?? 0) <= now).length;
  return pending;
}

export async function drainDMQueue({
  budgetMs = 45_000,
}: { budgetMs?: number } = {}): Promise<{ ranMs: number; leftPending: number }> {
  const started = Date.now();
  await recordWorkerHeartbeat({
    pid: process.pid,
    hostname: `serverless:${os.hostname()}`,
    startedAt: new Date(started).toISOString(),
  }).catch(() => {});

  const worker = createDMWorker(); // starts consuming immediately

  // Give the worker a moment to pick up the first jobs.
  await sleep(1500);
  let left = await pendingJobs().catch(() => 0);
  while (left > 0 && Date.now() - started < budgetMs) {
    await sleep(1000);
    left = await pendingJobs().catch(() => 0);
  }

  await worker.close(); // waits for in-flight jobs to finish
  return { ranMs: Date.now() - started, leftPending: left };
}
