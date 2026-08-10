/**
 * The escalation policy: when does a reconciliation difference become a page?
 *
 * This is deliberately a pure function over a rolling window rather than a
 * threshold on the latest sample. Every interesting property of this system is
 * about time:
 *
 *   - a residue of zero with large in-flight terms is a healthy busy system
 *   - a residue that appears and clears within a poll or two is a race, not a bug
 *   - a residue that persists is a bug, and its *sign* decides the urgency
 *   - a shortfall is never "wait and see", because the engine has already
 *     halted withdrawals and somebody needs to know why
 *
 * Alerting on `Math.abs(residue) > 0` produces noise that gets muted, and muting
 * is how a real shortfall goes unnoticed. So the policy is stated once, here,
 * with tests, instead of being spread across a dashboard and a runbook.
 */

/** Micro-units: 1 USDC = 1_000_000. Never a float. */
export type Micro = number;

export interface Terms {
  withdrawn_not_recognised: Micro;
  deposits_not_ingested: Micro;
  provisional_credits: Micro;
}

export type ReconciliationStatus =
  | "ok"
  | "explained"
  | "unsafe_shortfall"
  | "unsafe_surplus";

export interface Reconciliation {
  at: string;
  status: ReconciliationStatus;
  ledger_internal: Micro;
  chain_finalized: Micro;
  raw_delta: Micro;
  explained: Micro;
  unexplained: Micro;
  terms: Terms;
  in_flight_intents: number;
  healed_from_chain_state: number;
  note?: string;
}

export interface Violation {
  id: string;
  name: string;
  detail: string;
}

export interface EngineState {
  reconciliation: Reconciliation;
  violations: Violation[];
  freezes: Record<string, string>;
  escrow?: { unexplained: number; explained: number; stuck?: number };
}

export type Severity = "ok" | "info" | "warn" | "page";

export interface Alert {
  severity: Severity;
  code: string;
  message: string;
  /** How many consecutive samples this condition has held. */
  samples: number;
}

export interface PolicyConfig {
  /** Samples a non-zero residue may persist before it stops being a race. */
  residueGrace: number;
  /** Samples an in-flight term may sit unchanged before it looks wedged. */
  stallGrace: number;
  /** Rolling window size. */
  window: number;
}

export const defaultPolicy: PolicyConfig = {
  residueGrace: 2,
  stallGrace: 10,
  window: 32,
};

const usd = (v: Micro): string =>
  (v / 1_000_000).toLocaleString("en-US", { maximumFractionDigits: 6 });

/** A bounded rolling window of samples, oldest first. */
export class Window {
  private readonly samples: EngineState[] = [];

  constructor(private readonly limit: number) {}

  push(s: EngineState): void {
    this.samples.push(s);
    while (this.samples.length > this.limit) this.samples.shift();
  }

  get size(): number {
    return this.samples.length;
  }

  latest(): EngineState | undefined {
    return this.samples[this.samples.length - 1];
  }

  /**
   * How many consecutive samples, counting back from the newest, satisfy the
   * predicate. Counting backwards is what makes "has been true for N polls"
   * cheap and makes a recovery reset the counter immediately.
   */
  streak(pred: (s: EngineState) => boolean): number {
    let n = 0;
    for (let i = this.samples.length - 1; i >= 0; i--) {
      const s = this.samples[i];
      if (s === undefined || !pred(s)) break;
      n++;
    }
    return n;
  }

  /** True when a value has not moved at all across the last `n` samples. */
  frozen(n: number, pick: (s: EngineState) => number): boolean {
    if (this.samples.length < n || n < 2) return false;
    const slice = this.samples.slice(-n);
    const first = slice[0];
    if (first === undefined) return false;
    const v = pick(first);
    return slice.every((s) => pick(s) === v);
  }
}

/**
 * Evaluate the window and return every alert that currently applies, most severe
 * first. An empty-ish result is represented by a single `ok` alert rather than an
 * empty array, so a caller cannot accidentally treat "no data" as "healthy".
 */
export function evaluate(w: Window, cfg: PolicyConfig = defaultPolicy): Alert[] {
  const latest = w.latest();
  if (latest === undefined) {
    return [{ severity: "info", code: "no_data", message: "no samples yet", samples: 0 }];
  }

  const r = latest.reconciliation;
  const alerts: Alert[] = [];

  // A halt is always a page. The engine stopped paying people out; that is not
  // something to observe for a while and see if it improves.
  const halt = latest.freezes["*"];
  if (halt !== undefined) {
    alerts.push({
      severity: "page",
      code: "withdrawals_halted",
      message: `withdrawals halted: ${halt}`,
      samples: w.streak((s) => s.freezes["*"] !== undefined),
    });
  }

  // A shortfall means the ledger believes in money the vault does not hold. No
  // grace period: the sign is the whole point.
  if (r.status === "unsafe_shortfall") {
    alerts.push({
      severity: "page",
      code: "shortfall",
      message: `ledger exceeds custody by ${usd(r.unexplained)} beyond what in-flight work explains`,
      samples: w.streak((s) => s.reconciliation.status === "unsafe_shortfall"),
    });
  }

  // A surplus is safe — the chain holds more than we can attribute — but it is
  // still a bug, so it gets a grace period and then a warning rather than a page.
  if (r.status === "unsafe_surplus") {
    const streak = w.streak((s) => s.reconciliation.status === "unsafe_surplus");
    if (streak > cfg.residueGrace) {
      alerts.push({
        severity: "warn",
        code: "surplus",
        message: `custody exceeds the ledger by ${usd(-r.unexplained)}, unattributed for ${streak} polls`,
        samples: streak,
      });
    }
  }

  // Invariant failures other than I2 are not about drift at all; they mean the
  // ledger itself is inconsistent.
  const structural = latest.violations.filter((v) => v.id !== "I2");
  if (structural.length > 0) {
    const first = structural[0];
    alerts.push({
      severity: "page",
      code: "invariant_broken",
      message: `${structural.length} invariant(s) failing, e.g. ${first?.id}: ${first?.detail}`,
      samples: w.streak((s) => s.violations.some((v) => v.id !== "I2")),
    });
  }

  // Work that is in flight but not moving. Healthy in-flight terms churn; a term
  // that is bit-for-bit identical for a long stretch means something is wedged
  // and nobody is retrying it.
  const inFlightTotal = (s: EngineState): number =>
    s.reconciliation.terms.withdrawn_not_recognised +
    s.reconciliation.terms.deposits_not_ingested +
    s.reconciliation.terms.provisional_credits;

  if (inFlightTotal(latest) !== 0 && w.frozen(cfg.stallGrace, inFlightTotal)) {
    alerts.push({
      severity: "warn",
      code: "in_flight_stalled",
      message: `in-flight total has not moved for ${cfg.stallGrace} polls (${usd(inFlightTotal(latest))})`,
      samples: cfg.stallGrace,
    });
  }

  const escrow = latest.escrow;
  if (escrow !== undefined && (escrow.stuck ?? 0) > 0) {
    alerts.push({
      severity: "warn",
      code: "escrow_stalled",
      message: `${escrow.stuck} market(s) whose escrow gap has stopped closing`,
      samples: w.streak((s) => (s.escrow?.stuck ?? 0) > 0),
    });
  }

  if (alerts.length === 0) {
    const busy = r.explained !== 0 || r.in_flight_intents > 0;
    return [
      {
        severity: "ok",
        code: busy ? "explained" : "settled",
        message: busy
          ? `${r.in_flight_intents} intent(s) in flight, ${usd(r.explained)} explained, residue zero`
          : "ledger and custody agree exactly",
        samples: w.size,
      },
    ];
  }

  const rank: Record<Severity, number> = { page: 0, warn: 1, info: 2, ok: 3 };
  return alerts.sort((a, b) => rank[a.severity] - rank[b.severity]);
}

/** The worst severity present, for an exit code or a status endpoint. */
export function worst(alerts: Alert[]): Severity {
  const rank: Record<Severity, number> = { page: 0, warn: 1, info: 2, ok: 3 };
  return alerts.reduce<Severity>(
    (acc, a) => (rank[a.severity] < rank[acc] ? a.severity : acc),
    "ok",
  );
}
