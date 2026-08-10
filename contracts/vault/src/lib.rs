//! The on-chain half of the system: what the vault contract will and will not do.
//!
//! This crate is the authority for three rules the off-chain engine depends on
//! completely. If any of them were untrue, the reconciler's arithmetic would be
//! meaningless:
//!
//! 1. **A nonce executes at most once, and dedupe happens at execution.**
//!    Not at submission. A nonce sitting in a mempool has not executed, so a
//!    retry must be safe to include a second time and must be a no-op when it
//!    runs. This is what makes an unknown receipt survivable off chain: the
//!    engine can retry blindly and ask later.
//!
//! 2. **Expiry is enforced here, by height.** An off-chain timeout is only a
//!    guess. If the engine refunds a withdrawal and the transaction later lands
//!    anyway — out of a mempool, or back from a reorg — the vault pays money the
//!    engine already gave back. The only sound expiry is one the executing
//!    authority checks itself.
//!
//! 3. **Escrow can never exceed custody, and a withdrawal can never touch
//!    escrowed funds.** Escrow is a claim on money already held, not extra
//!    money.
//!
//! Everything is a `u128` in micro-units (1 USDC = 1_000_000) with
//! overflow checks on. There is no floating point anywhere near a balance.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};

pub type Micro = u128;
pub type Height = u64;

/// A call into the vault. Note that `EscrowSet` carries an absolute target and
/// not a delta: see [`Vault::execute`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Call {
    /// A user sends funds to the vault. Chain-originated, so it has a fact id
    /// rather than an engine-chosen nonce.
    Deposit {
        fact_id: String,
        user: String,
        amount: Micro,
    },
    /// Pay a user out of free (non-escrowed) custody.
    Withdraw {
        nonce: String,
        user: String,
        amount: Micro,
        expiry_height: Height,
    },
    /// Move free custody into, or out of, a market's escrow so that it equals
    /// `target`.
    EscrowSet {
        nonce: String,
        market: String,
        target: Micro,
        expiry_height: Height,
    },
    /// Resolve a market: release its whole escrow back to free custody and
    /// record the outcome permanently.
    SettleMarket {
        nonce: String,
        market: String,
        outcome: bool,
        expiry_height: Height,
    },
    /// Permanently refuse a nonce that is past its expiry. The engine calls this
    /// before releasing an off-chain reservation, so that "this can never
    /// execute" becomes a fact on chain rather than an assumption.
    Cancel {
        nonce: String,
        expiry_height: Height,
    },
}

impl Call {
    pub fn nonce(&self) -> &str {
        match self {
            Call::Deposit { fact_id, .. } => fact_id,
            Call::Withdraw { nonce, .. }
            | Call::EscrowSet { nonce, .. }
            | Call::SettleMarket { nonce, .. }
            | Call::Cancel { nonce, .. } => nonce,
        }
    }

    fn expiry(&self) -> Height {
        match self {
            Call::Deposit { .. } => 0,
            Call::Withdraw { expiry_height, .. }
            | Call::EscrowSet { expiry_height, .. }
            | Call::SettleMarket { expiry_height, .. }
            | Call::Cancel { expiry_height, .. } => *expiry_height,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    /// The nonce ran before. The caller should treat this as success: the effect
    /// it wanted has already happened.
    AlreadyProcessed,
    /// The nonce was cancelled or is past its expiry height. Terminal.
    Expired,
    /// Not enough free (non-escrowed) custody.
    InsufficientFree,
    /// Escrow would exceed what the vault actually holds.
    EscrowExceedsCustody,
    /// The market is already resolved; outcomes are immutable.
    MarketSettled,
    /// A cancel arrived while the nonce could still legitimately execute.
    NotYetExpired,
    ZeroAmount,
}

/// What a successful call did. The engine records this against the intent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Receipt {
    pub height: Height,
    pub free_after: Micro,
    pub escrow_total_after: Micro,
    /// Funds that actually left the vault (a withdrawal), for reconciliation.
    pub paid_out: Micro,
}

#[derive(Debug, Default, Clone)]
pub struct Vault {
    height: Height,
    /// Total custody. Escrow is carved out of this, never added to it.
    balance: Micro,
    escrow: BTreeMap<String, Micro>,
    settled: BTreeMap<String, bool>,
    processed: BTreeMap<String, Receipt>,
    cancelled: BTreeSet<String>,
    /// Bookkeeping only, so the contract can answer "how much of my balance is
    /// attributable to this user's deposits". Not a spendable balance: the
    /// engine's ledger decides who owns what.
    deposited: BTreeMap<String, Micro>,
}

impl Vault {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn height(&self) -> Height {
        self.height
    }
    pub fn balance(&self) -> Micro {
        self.balance
    }
    pub fn escrow_total(&self) -> Micro {
        self.escrow.values().sum()
    }
    pub fn escrow_of(&self, market: &str) -> Micro {
        self.escrow.get(market).copied().unwrap_or(0)
    }
    /// Custody not earmarked for any market. The only funds a withdrawal may
    /// draw on.
    pub fn free(&self) -> Micro {
        self.balance - self.escrow_total()
    }
    pub fn is_settled(&self, market: &str) -> bool {
        self.settled.contains_key(market)
    }
    pub fn outcome_of(&self, market: &str) -> Option<bool> {
        self.settled.get(market).copied()
    }
    pub fn receipt(&self, nonce: &str) -> Option<&Receipt> {
        self.processed.get(nonce)
    }
    pub fn is_cancelled(&self, nonce: &str) -> bool {
        self.cancelled.contains(nonce)
    }
    pub fn deposited_by(&self, user: &str) -> Micro {
        self.deposited.get(user).copied().unwrap_or(0)
    }

    /// Advance to the next block. Calls executed after this see the new height,
    /// which is what makes expiry meaningful.
    pub fn advance_block(&mut self) {
        self.height += 1;
    }

    /// Execute a call.
    ///
    /// Two ordering decisions here carry the whole design.
    ///
    /// **Dedupe before expiry.** A nonce that already executed returns
    /// `AlreadyProcessed` even once its expiry height has passed, so a late
    /// retry of a *successful* call never looks like a failure. Reversing these
    /// two checks would let a retry of a landed withdrawal report `Expired`, and
    /// the engine would refund money it had already paid out.
    ///
    /// **`EscrowSet` takes an absolute target, not a delta.** Under an unknown
    /// receipt the engine may submit the same call several times; an absolute
    /// assignment converges to the same state however many times it runs, while
    /// a delta silently compounds. When you cannot know whether your last call
    /// landed, idempotence has to come from the shape of the operation, not from
    /// the caller being careful.
    pub fn execute(&mut self, call: Call) -> Result<Receipt, Error> {
        let nonce = call.nonce().to_string();

        if let Some(r) = self.processed.get(&nonce) {
            return Err(if matches!(call, Call::Deposit { .. }) {
                Error::AlreadyProcessed
            } else {
                let _ = r;
                Error::AlreadyProcessed
            });
        }
        if self.cancelled.contains(&nonce) {
            return Err(Error::Expired);
        }
        let expiry = call.expiry();
        if expiry > 0 && self.height > expiry && !matches!(call, Call::Cancel { .. }) {
            return Err(Error::Expired);
        }

        let paid_out = match &call {
            Call::Deposit {
                user,
                amount,
                fact_id: _,
            } => {
                if *amount == 0 {
                    return Err(Error::ZeroAmount);
                }
                self.balance += amount;
                *self.deposited.entry(user.clone()).or_insert(0) += amount;
                0
            }

            Call::Withdraw { amount, .. } => {
                if *amount == 0 {
                    return Err(Error::ZeroAmount);
                }
                if self.free() < *amount {
                    return Err(Error::InsufficientFree);
                }
                self.balance -= amount;
                *amount
            }

            Call::EscrowSet { market, target, .. } => {
                if self.settled.contains_key(market) {
                    return Err(Error::MarketSettled);
                }
                let current = self.escrow_of(market);
                if *target > current {
                    let extra = target - current;
                    if self.free() < extra {
                        return Err(Error::EscrowExceedsCustody);
                    }
                }
                if *target == 0 {
                    self.escrow.remove(market);
                } else {
                    self.escrow.insert(market.clone(), *target);
                }
                0
            }

            Call::SettleMarket {
                market, outcome, ..
            } => {
                if self.settled.contains_key(market) {
                    return Err(Error::MarketSettled);
                }
                // Releasing escrow does not move money out of the vault; it just
                // stops earmarking it. Who gets paid is the ledger's business,
                // and it happens through ordinary withdrawals afterwards.
                self.escrow.remove(market);
                self.settled.insert(market.clone(), *outcome);
                0
            }

            Call::Cancel {
                nonce,
                expiry_height,
            } => {
                if *expiry_height == 0 || self.height <= *expiry_height {
                    return Err(Error::NotYetExpired);
                }
                self.cancelled.insert(nonce.clone());
                0
            }
        };

        let receipt = Receipt {
            height: self.height,
            free_after: self.free(),
            escrow_total_after: self.escrow_total(),
            paid_out,
        };
        if !matches!(call, Call::Cancel { .. }) {
            self.processed.insert(nonce, receipt.clone());
        }
        debug_assert!(self.check().is_ok(), "vault invariant broken: {:?}", self.check());
        Ok(receipt)
    }

    /// The contract's own invariants. Cheap enough to assert after every call.
    pub fn check(&self) -> Result<(), &'static str> {
        if self.escrow_total() > self.balance {
            return Err("escrow exceeds custody");
        }
        if self.escrow.values().any(|v| *v == 0) {
            return Err("zero-valued escrow entry should have been removed");
        }
        for market in self.escrow.keys() {
            if self.settled.contains_key(market) {
                return Err("settled market still holds escrow");
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------- tests

#[cfg(test)]
mod tests {
    use super::*;

    fn dep(v: &mut Vault, id: &str, user: &str, amt: Micro) {
        v.execute(Call::Deposit {
            fact_id: id.into(),
            user: user.into(),
            amount: amt,
        })
        .unwrap();
    }

    fn wd(v: &mut Vault, nonce: &str, amt: Micro, expiry: Height) -> Result<Receipt, Error> {
        v.execute(Call::Withdraw {
            nonce: nonce.into(),
            user: "alice".into(),
            amount: amt,
            expiry_height: expiry,
        })
    }

    #[test]
    fn deposit_then_withdraw() {
        let mut v = Vault::new();
        dep(&mut v, "d1", "alice", 1_000);
        assert_eq!(v.balance(), 1_000);
        assert_eq!(wd(&mut v, "w1", 400, 100).unwrap().paid_out, 400);
        assert_eq!(v.balance(), 600);
    }

    #[test]
    fn a_nonce_executes_at_most_once() {
        let mut v = Vault::new();
        dep(&mut v, "d1", "alice", 1_000);
        wd(&mut v, "w1", 400, 100).unwrap();
        // The retry the engine issues after an unknown receipt.
        assert_eq!(wd(&mut v, "w1", 400, 100), Err(Error::AlreadyProcessed));
        assert_eq!(v.balance(), 600, "a retried nonce must not pay twice");
    }

    #[test]
    fn dedupe_wins_over_expiry_for_a_call_that_already_landed() {
        // The ordering that matters most. If expiry were checked first, a late
        // retry of a *successful* withdrawal would report Expired, and the
        // engine would refund money the vault had already paid out.
        let mut v = Vault::new();
        dep(&mut v, "d1", "alice", 1_000);
        wd(&mut v, "w1", 400, 5).unwrap();
        for _ in 0..20 {
            v.advance_block();
        }
        assert_eq!(wd(&mut v, "w1", 400, 5), Err(Error::AlreadyProcessed));
        assert_eq!(v.balance(), 600);
    }

    #[test]
    fn expiry_is_enforced_by_height() {
        let mut v = Vault::new();
        dep(&mut v, "d1", "alice", 1_000);
        for _ in 0..10 {
            v.advance_block();
        }
        assert_eq!(wd(&mut v, "w1", 100, 5), Err(Error::Expired));
        assert_eq!(v.balance(), 1_000, "an expired call must not move money");
    }

    #[test]
    fn cancel_makes_expiry_a_fact_and_is_irreversible() {
        let mut v = Vault::new();
        dep(&mut v, "d1", "alice", 1_000);
        // Too early: the call could still legitimately land.
        assert_eq!(
            v.execute(Call::Cancel {
                nonce: "w1".into(),
                expiry_height: 5
            }),
            Err(Error::NotYetExpired)
        );
        for _ in 0..6 {
            v.advance_block();
        }
        v.execute(Call::Cancel {
            nonce: "w1".into(),
            expiry_height: 5,
        })
        .unwrap();
        assert!(v.is_cancelled("w1"));
        // Now the engine can refund off chain, because this can never execute.
        assert_eq!(wd(&mut v, "w1", 100, 999), Err(Error::Expired));
    }

    #[test]
    fn withdrawal_cannot_touch_escrowed_funds() {
        let mut v = Vault::new();
        dep(&mut v, "d1", "alice", 1_000);
        v.execute(Call::EscrowSet {
            nonce: "e1".into(),
            market: "m1".into(),
            target: 800,
            expiry_height: 0,
        })
        .unwrap();
        assert_eq!(v.free(), 200);
        assert_eq!(wd(&mut v, "w1", 500, 0), Err(Error::InsufficientFree));
        assert_eq!(wd(&mut v, "w2", 200, 0).unwrap().paid_out, 200);
    }

    #[test]
    fn escrow_cannot_exceed_custody() {
        let mut v = Vault::new();
        dep(&mut v, "d1", "alice", 500);
        assert_eq!(
            v.execute(Call::EscrowSet {
                nonce: "e1".into(),
                market: "m1".into(),
                target: 600,
                expiry_height: 0
            }),
            Err(Error::EscrowExceedsCustody)
        );
        assert_eq!(v.escrow_total(), 0);
    }

    #[test]
    fn escrow_set_is_absolute_so_retries_converge() {
        let mut v = Vault::new();
        dep(&mut v, "d1", "alice", 1_000);
        for n in ["e1", "e2", "e3"] {
            let _ = v.execute(Call::EscrowSet {
                nonce: n.into(),
                market: "m1".into(),
                target: 300,
                expiry_height: 0,
            });
        }
        // Three separate nonces, same target. A delta-shaped operation would
        // have reached 900.
        assert_eq!(v.escrow_of("m1"), 300);
    }

    #[test]
    fn settlement_releases_escrow_and_is_final() {
        let mut v = Vault::new();
        dep(&mut v, "d1", "alice", 1_000);
        v.execute(Call::EscrowSet {
            nonce: "e1".into(),
            market: "m1".into(),
            target: 400,
            expiry_height: 0,
        })
        .unwrap();
        v.execute(Call::SettleMarket {
            nonce: "s1".into(),
            market: "m1".into(),
            outcome: true,
            expiry_height: 0,
        })
        .unwrap();
        assert_eq!(v.escrow_of("m1"), 0);
        assert_eq!(v.free(), 1_000, "released escrow stays in custody");
        assert_eq!(v.outcome_of("m1"), Some(true));
        assert_eq!(
            v.execute(Call::SettleMarket {
                nonce: "s2".into(),
                market: "m1".into(),
                outcome: false,
                expiry_height: 0
            }),
            Err(Error::MarketSettled),
            "an outcome must never be rewritten"
        );
    }

    #[test]
    fn settled_market_rejects_new_escrow() {
        let mut v = Vault::new();
        dep(&mut v, "d1", "alice", 1_000);
        v.execute(Call::SettleMarket {
            nonce: "s1".into(),
            market: "m1".into(),
            outcome: true,
            expiry_height: 0,
        })
        .unwrap();
        assert_eq!(
            v.execute(Call::EscrowSet {
                nonce: "e1".into(),
                market: "m1".into(),
                target: 100,
                expiry_height: 0
            }),
            Err(Error::MarketSettled)
        );
    }

    /// A deterministic random driver instead of a proptest dependency: same
    /// spirit, no toolchain cost. It throws a long stream of calls at the vault,
    /// most of them invalid, and asserts the contract's own invariants after
    /// every single one.
    #[test]
    fn fuzz_invariants_hold_under_random_calls() {
        let mut seed: u64 = 0x2545F4914F6CDD1D;
        let mut rnd = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for run in 0..64u64 {
            let mut v = Vault::new();
            let mut paid: Micro = 0;
            let mut deposited: Micro = 0;
            for i in 0..400u64 {
                let r = rnd();
                let amt = ((r >> 8) % 500 + 1) as Micro;
                let market = format!("m{}", r % 5);
                let nonce = format!("n{}-{}-{}", run, i, r % 3); // collisions on purpose
                let expiry = if r % 4 == 0 { v.height() } else { v.height() + 5 };
                let call = match r % 6 {
                    0 | 1 => Call::Deposit {
                        fact_id: nonce.clone(),
                        user: format!("u{}", r % 4),
                        amount: amt,
                    },
                    2 => Call::Withdraw {
                        nonce: nonce.clone(),
                        user: format!("u{}", r % 4),
                        amount: amt,
                        expiry_height: expiry,
                    },
                    3 => Call::EscrowSet {
                        nonce: nonce.clone(),
                        market,
                        target: amt,
                        expiry_height: expiry,
                    },
                    4 => Call::SettleMarket {
                        nonce: nonce.clone(),
                        market,
                        outcome: r % 2 == 0,
                        expiry_height: expiry,
                    },
                    _ => Call::Cancel {
                        nonce: nonce.clone(),
                        expiry_height: expiry,
                    },
                };
                let before = v.balance();
                match v.execute(call.clone()) {
                    Ok(rc) => {
                        paid += rc.paid_out;
                        if let Call::Deposit { amount, .. } = call {
                            deposited += amount;
                        }
                    }
                    Err(_) => assert_eq!(
                        v.balance(),
                        before,
                        "a rejected call moved money: {:?}",
                        call
                    ),
                }
                v.check().expect("invariant broken");
                if r % 7 == 0 {
                    v.advance_block();
                }
            }
            // Conservation: every unit of custody is a deposit that was not paid out.
            assert_eq!(v.balance(), deposited - paid);
        }
    }
}
