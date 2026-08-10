//! Differential test: replay the Go simulator's finalised history through the
//! real Rust contract and require identical end state.
//!
//! The engine's correctness argument rests on the simulator behaving like the
//! contract. That is an assumption, and an unchecked assumption in this position
//! is how you get a reconciler that passes its own tests and still loses money.
//! So it gets checked: the Go side exports every finalised call in order, this
//! binary executes them against the implementation that would actually be
//! deployed, and the two must agree on the vault balance and on every market's
//! escrow, to the unit.
//!
//!     pmr trace -out /tmp/trace.txt      # Go writes finalised history
//!     cargo run --bin replay /tmp/trace.txt
//!
//! The trace is whitespace-separated text rather than JSON so the contract crate
//! keeps zero dependencies.

use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::process::exit;

use vault::{Call, Error, Vault};

fn advance_to(v: &mut Vault, h: u64) {
    while v.height() < h {
        v.advance_block();
    }
}

fn main() {
    let path = match env::args().nth(1) {
        Some(p) => p,
        None => {
            eprintln!("usage: replay <trace-file>");
            exit(2);
        }
    };
    let text = match fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("cannot read {path}: {e}");
            exit(2);
        }
    };

    let mut v = Vault::new();
    let mut applied = 0usize;
    let mut rejected: BTreeMap<&'static str, usize> = BTreeMap::new();
    let mut expects: Vec<(String, String, i128)> = Vec::new();
    let mut executed = 0usize;

    for (lineno, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let f: Vec<&str> = line.split_whitespace().collect();

        // Every call carries the height it executed at, so expiry evaluates
        // identically on both sides without replaying block structure.
        let res = match f.as_slice() {
            ["deposit", nonce, user, amount, height] => {
                advance_to(&mut v, num(height, lineno) as u64);
                executed += 1;
                v.execute(Call::Deposit {
                    fact_id: nonce.to_string(),
                    user: user.to_string(),
                    amount: num(amount, lineno) as u128,
                })
            }
            ["withdraw", nonce, user, amount, expiry, height] => {
                advance_to(&mut v, num(height, lineno) as u64);
                executed += 1;
                v.execute(Call::Withdraw {
                    nonce: nonce.to_string(),
                    user: user.to_string(),
                    amount: num(amount, lineno) as u128,
                    expiry_height: num(expiry, lineno) as u64,
                })
            }
            ["escrow_set", nonce, market, target, expiry, height] => {
                advance_to(&mut v, num(height, lineno) as u64);
                executed += 1;
                v.execute(Call::EscrowSet {
                    nonce: nonce.to_string(),
                    market: market.to_string(),
                    target: num(target, lineno) as u128,
                    expiry_height: num(expiry, lineno) as u64,
                })
            }
            ["settle", nonce, market, outcome, expiry, height] => {
                advance_to(&mut v, num(height, lineno) as u64);
                executed += 1;
                v.execute(Call::SettleMarket {
                    nonce: nonce.to_string(),
                    market: market.to_string(),
                    outcome: num(outcome, lineno) != 0,
                    expiry_height: num(expiry, lineno) as u64,
                })
            }
            ["cancel", nonce, expiry, height] => {
                advance_to(&mut v, num(height, lineno) as u64);
                executed += 1;
                v.execute(Call::Cancel {
                    nonce: nonce.to_string(),
                    expiry_height: num(expiry, lineno) as u64,
                })
            }
            ["expect", "escrow", market, value] => {
                expects.push(("escrow".into(), market.to_string(), num(value, lineno)));
                continue;
            }
            ["expect", what, value] => {
                expects.push((what.to_string(), String::new(), num(value, lineno)));
                continue;
            }
            _ => {
                eprintln!("line {}: cannot parse: {line}", lineno + 1);
                exit(2);
            }
        };

        match res {
            Ok(_) => applied += 1,
            Err(e) => *rejected.entry(name_of(e)).or_insert(0) += 1,
        }
        if let Err(e) = v.check() {
            eprintln!("line {}: contract invariant broken: {e}", lineno + 1);
            exit(1);
        }
    }

    let mut failures = 0;
    for (what, market, want) in &expects {
        let got: i128 = match what.as_str() {
            "balance" => v.balance() as i128,
            "escrow_total" => v.escrow_total() as i128,
            "escrow" => v.escrow_of(market) as i128,
            other => {
                eprintln!("unknown expectation {other}");
                exit(2);
            }
        };
        if got != *want {
            let label = if market.is_empty() {
                what.clone()
            } else {
                format!("{what}[{market}]")
            };
            eprintln!("MISMATCH {label}: rust contract says {got}, go simulator says {want}");
            failures += 1;
        }
    }

    println!(
        "replayed {executed} finalised call(s): {applied} applied, {} refused",
        rejected.values().sum::<usize>()
    );
    if !rejected.is_empty() {
        let detail: Vec<String> = rejected.iter().map(|(k, n)| format!("{k}={n}")).collect();
        println!("  refusals: {}", detail.join(" "));
    }
    println!(
        "  end state: balance={} escrow_total={} height={}",
        v.balance(),
        v.escrow_total(),
        v.height()
    );

    // Every call in the trace was accepted by the Go simulator, so the Rust
    // contract must accept all of them too. A refusal here means the two
    // implementations disagree about what is permissible -- most likely the
    // free-versus-escrowed funds rule -- even if the final balances happen to
    // line up.
    if !rejected.is_empty() {
        eprintln!(
            "\nFAIL: the Rust contract refused {} call(s) the Go simulator accepted",
            rejected.values().sum::<usize>()
        );
        exit(1);
    }
    if failures > 0 {
        eprintln!("\nFAIL: the Rust contract and the Go simulator disagree in {failures} place(s)");
        exit(1);
    }
    if expects.is_empty() {
        eprintln!("\nFAIL: trace carried no expectations; nothing was compared");
        exit(1);
    }
    println!("\nOK: the Rust contract and the Go simulator agree exactly");
}

fn num(s: &str, lineno: usize) -> i128 {
    match s.parse::<i128>() {
        Ok(v) => v,
        Err(_) => {
            eprintln!("line {}: not a number: {s}", lineno + 1);
            exit(2);
        }
    }
}

fn name_of(e: Error) -> &'static str {
    match e {
        Error::AlreadyProcessed => "already_processed",
        Error::Expired => "expired",
        Error::InsufficientFree => "insufficient_free",
        Error::EscrowExceedsCustody => "escrow_exceeds_custody",
        Error::MarketSettled => "market_settled",
        Error::NotYetExpired => "not_yet_expired",
        Error::ZeroAmount => "zero_amount",
    }
}
