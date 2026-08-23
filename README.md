# PolyBTC 15m Edge Lab v1.1 — PAPER only

This build removes the weak 5-minute sniper accounts and the inactive 15m scalp from v1.0. It concentrates the test on the only branch that was positive in the first half-day sample: 15-minute stale-quote / fair-value entries.

All four strategies share the exact same Polymarket books, Binance spot/perp state, volatility snapshot and decision loop. Each strategy has a completely independent PAPER account starting at $500.

## Four 15-minute experiments

### A — `S15_E10_60`
- 15m only
- final 60 seconds
- favorite side only
- ask 0.50–0.70
- robust net edge after fee: 0.10–0.20
- max $25 per take, max $50 per market

### B — `S15_E125_60`
Same as A, but minimum edge is 0.125. This is the middle threshold.

### C — `S15_E15_60`
Same as A, but minimum edge is 0.15. This tests whether fewer, stronger dislocations improve quality.

### D — `S15_E125_90`
Same 0.125 threshold as B, but the entry window is extended to the final 90 seconds. This isolates the effect of window length without changing the edge threshold.

This design lets us answer two questions cleanly:
1. On the same 60-second window, is 10c, 12.5c or 15c the better edge gate?
2. At the same 12.5c edge gate, is 60s or 90s the better entry window?

## Fair-value / fill logic retained from v1.0

- dual robust betas 0.83 / 1.36;
- fast and slow realized volatility, using the conservative value;
- Binance Spot + Perpetual with rolling basis adjustment;
- favorite-side only;
- edge is fair probability minus Polymarket ask minus estimated taker fee;
- 400ms PAPER taker latency by default;
- after the latency the original quote must still be available or the attempt is recorded as a FAK miss;
- liquidity is consumed only at or below the committed ask;
- sub-$10 fills are rejected.

## Telegram

`BALANCE`, `STATISTICS`, `POSITIONS`, and `TRADES` are reported separately for A/B/C/D. At each completed UTC hour the bot sends a ZIP with:

- `summary.csv`
- `trades.csv`
- `signals.csv`
- `results.csv`
- `report.txt`

The `signals.csv` file contains fair probabilities, edge, volatility, effective Binance price and FAK result for later analysis.

## New database

`/var/data/polybtc_15m_edge_lab_v1_1.db`

This intentionally does not reuse v1.0 results. Every strategy starts a clean $500 test.

## Render

1. Upload `main.py`, `requirements.txt`, `.env.example`.
2. Build command: `pip install -r requirements.txt`
3. Start command: `python main.py`
4. Persistent disk: `/var/data`
5. Configure `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID`.
6. In Telegram press `START` after deployment.

Expected log:

`1.1-polybtc-15m-edge-lab-paper started | PAPER ONLY | S15_E10_60=$500.00, S15_E125_60=$500.00, S15_E15_60=$500.00, S15_E125_90=$500.00`

Keep this PAPER-only until enough 15m fills are collected. The first v1.0 sample had only two filled 15m trades, so it was promising but far too small to infer profitability.
