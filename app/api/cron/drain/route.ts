import { NextRequest, NextResponse } from "next/server";
import { drainDMQueue } from "@/lib/queue/serverless-drain";
import { reconcileComments } from "@/lib/polling/comment-reconciler";
import { attachPendingNextReels } from "@/lib/automation/attach-next-reel";

export const runtime = "nodejs";
export const maxDuration = 60;
export const dynamic = "force-dynamic";

/**
 * Serverless replacement for the always-on worker loop.
 * Call every 5–15 minutes with `Authorization: Bearer <CRON_SECRET>`.
 * Sends delayed/retried DMs and runs the missed-comment polling sweep.
 */
export async function GET(request: NextRequest) {
  const authHeader = request.headers.get("authorization");
  const cronSecret = process.env.CRON_SECRET || process.env.NEXTAUTH_SECRET;
  if (!cronSecret || authHeader !== `Bearer ${cronSecret}`) {
    return NextResponse.json(
      { success: false, error: "Unauthorized" },
      { status: 401 }
    );
  }

  const errors: string[] = [];
  try {
    await attachPendingNextReels();
  } catch (e) {
    errors.push(`attach: ${e instanceof Error ? e.message : String(e)}`);
  }
  try {
    await reconcileComments();
  } catch (e) {
    errors.push(`reconcile: ${e instanceof Error ? e.message : String(e)}`);
  }
  const drain = await drainDMQueue({ budgetMs: 40_000 });

  return NextResponse.json({ success: errors.length === 0, drain, errors });
}
