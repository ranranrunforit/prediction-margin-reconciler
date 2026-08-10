import { test } from "node:test";
import assert from "node:assert/strict";
import { Window, evaluate, worst, defaultPolicy, type EngineState } from "./watcher.js";

const M = 1_000_000;

function state(over: Partial<EngineState["reconciliation"]> = {}, rest: Partial<EngineState> = {}): EngineState {
  return {
    reconciliation: {
      at: new Date().toISOString(),
      status: "ok",
      ledger_internal: 1000 * M,
      chain_finalized: 1000 * M,
      raw_delta: 0,
      explained: 0,
      unexplained: 0,
      terms: { withdrawn_not_recognised: 0, deposits_not_ingested: 0, provisional_credits: 0 },
      in_flight_intents: 0,
      healed_from_chain_state: 0,
      ...over,
    },
    violations: [],
    freezes: {},
    ...rest,
  };
}

function fill(states: EngineState[]): Window {
  const w = new Window(defaultPolicy.window);
  for (const s of states) w.push(s);
  return w;
}

test("no samples is not the same as healthy", () => {
  const alerts = evaluate(new Window(8));
  assert.equal(alerts[0]?.code, "no_data");
  assert.notEqual(alerts[0]?.severity, "ok");
});

test("a busy system with a zero residue is healthy", () => {
  const w = fill([
    state({ status: "explained", explained: 250 * M, raw_delta: 250 * M, in_flight_intents: 3,
            terms: { withdrawn_not_recognised: 250 * M, deposits_not_ingested: 0, provisional_credits: 0 } }),
  ]);
  const alerts = evaluate(w);
  assert.equal(worst(alerts), "ok");
  assert.equal(alerts[0]?.code, "explained");
});

test("a shortfall pages immediately, with no grace period", () => {
  const w = fill([state({ status: "unsafe_shortfall", unexplained: 5 * M })]);
  const alerts = evaluate(w);
  assert.equal(worst(alerts), "page");
  assert.ok(alerts.some((a) => a.code === "shortfall"));
});

test("a brief surplus is a race and is not alerted", () => {
  const w = fill([state(), state({ status: "unsafe_surplus", unexplained: -2 * M })]);
  assert.equal(worst(evaluate(w)), "ok");
});

test("a surplus that persists past the grace period warns", () => {
  const w = fill([
    state({ status: "unsafe_surplus", unexplained: -2 * M }),
    state({ status: "unsafe_surplus", unexplained: -2 * M }),
    state({ status: "unsafe_surplus", unexplained: -2 * M }),
  ]);
  const alerts = evaluate(w);
  assert.equal(worst(alerts), "warn");
  assert.equal(alerts.find((a) => a.code === "surplus")?.samples, 3);
});

test("recovery clears the streak straight away", () => {
  const w = fill([
    state({ status: "unsafe_surplus", unexplained: -2 * M }),
    state({ status: "unsafe_surplus", unexplained: -2 * M }),
    state({ status: "unsafe_surplus", unexplained: -2 * M }),
    state(),
  ]);
  assert.equal(worst(evaluate(w)), "ok");
});

test("a halt pages regardless of the reconciliation status", () => {
  const w = fill([state({}, { freezes: { "*": "unexplained shortfall of 5" } })]);
  const alerts = evaluate(w);
  assert.equal(worst(alerts), "page");
  assert.ok(alerts.some((a) => a.code === "withdrawals_halted"));
});

test("a structural invariant failure pages even when drift is zero", () => {
  const w = fill([
    state({}, { violations: [{ id: "I4", name: "margin matches collateral", detail: "off by 3" }] }),
  ]);
  assert.equal(worst(evaluate(w)), "page");
});

test("in-flight work that never moves is treated as wedged", () => {
  const stuck = state({
    status: "explained",
    explained: 100 * M,
    raw_delta: 100 * M,
    terms: { withdrawn_not_recognised: 100 * M, deposits_not_ingested: 0, provisional_credits: 0 },
  });
  const w = fill(Array.from({ length: defaultPolicy.stallGrace }, () => stuck));
  const alerts = evaluate(w);
  assert.ok(alerts.some((a) => a.code === "in_flight_stalled"));
});

test("in-flight work that churns is left alone", () => {
  const w = fill(
    Array.from({ length: defaultPolicy.stallGrace }, (_, i) =>
      state({
        status: "explained",
        explained: (100 + i) * M,
        raw_delta: (100 + i) * M,
        terms: { withdrawn_not_recognised: (100 + i) * M, deposits_not_ingested: 0, provisional_credits: 0 },
      }),
    ),
  );
  assert.equal(worst(evaluate(w)), "ok");
});

test("stalled escrow warns", () => {
  const w = fill([state({}, { escrow: { unexplained: 1, explained: 0, stuck: 2 } })]);
  const alerts = evaluate(w);
  assert.equal(worst(alerts), "warn");
  assert.ok(alerts.some((a) => a.code === "escrow_stalled"));
});
