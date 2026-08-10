/**
 * Polls the Go engine, applies the escalation policy, and exposes the verdict.
 *
 *   ENGINE=http://localhost:8080 PORT=8081 npm start
 *
 * Zero runtime dependencies: Node's built-in fetch and http server are enough,
 * and a component whose job is to notice outages should not have a dependency
 * tree of its own.
 */
import { createServer } from "node:http";
import {
  Window,
  evaluate,
  worst,
  defaultPolicy,
  type Alert,
  type EngineState,
} from "./watcher.js";

const ENGINE = process.env["ENGINE"] ?? "http://127.0.0.1:8080";
const PORT = Number(process.env["PORT"] ?? 8081);
const INTERVAL = Number(process.env["INTERVAL_MS"] ?? 1500);

const window = new Window(defaultPolicy.window);
let current: Alert[] = [];
let lastError: string | undefined;
let polls = 0;

async function poll(): Promise<void> {
  try {
    const res = await fetch(`${ENGINE}/api/state`);
    if (!res.ok) throw new Error(`engine returned ${res.status}`);
    const body = (await res.json()) as EngineState;
    window.push(body);
    lastError = undefined;
    polls++;
  } catch (err) {
    // An unreachable engine is itself a page-worthy condition; it must not look
    // like healthy silence.
    lastError = err instanceof Error ? err.message : String(err);
    current = [
      { severity: "page", code: "engine_unreachable", message: `cannot reach ${ENGINE}: ${lastError}`, samples: 0 },
    ];
    return;
  }
  const next = evaluate(window);
  const changed =
    next.length !== current.length ||
    next.some((a, i) => a.code !== current[i]?.code || a.severity !== current[i]?.severity);
  current = next;
  if (changed) {
    for (const a of next) {
      const line = `[${a.severity.toUpperCase()}] ${a.code}: ${a.message} (${a.samples} polls)`;
      if (a.severity === "page") console.error(line);
      else console.log(line);
    }
  }
}

createServer((req, res) => {
  const severity = worst(current);
  const code = severity === "page" ? 503 : 200;
  res.writeHead(code, { "content-type": "application/json" });
  res.end(
    JSON.stringify(
      { engine: ENGINE, polls, severity, alerts: current, error: lastError ?? null },
      null,
      2,
    ),
  );
  void req;
}).listen(PORT, () => {
  console.log(`watching ${ENGINE}, status on http://localhost:${PORT}`);
});

void poll();
setInterval(() => void poll(), INTERVAL);
