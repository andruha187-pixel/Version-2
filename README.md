# PolyBTC Sniper Lab v1.0 — PAPER only

Separate research bot adapted to the user's existing Render/Telegram Polymarket setup. It does **not** replace the running M03 A/B/C bot. It tests the public `LukasNSteel/polybtc` stale-quote thesis on the same live Polymarket books with Binance spot + perpetual data.

## What the upstream research actually supports

The public study reports 1,853 resolved BTC Up/Down markets and 3.53M taker executions. Its strongest repeated finding is not generic momentum-following: the useful signal is concentrated in short-lived dislocations where Binance-derived fair value moves before resting Polymarket asks fully reprice. The later go-live review reports 202 paper snipe fills over ~54h, +$2,078 paper PnL, but also ~61% FAK capture and a $407 max drawdown; the authors explicitly say that sample is not enough to justify full live deployment.

The later June-16 review also supersedes one earlier recommendation: the June-12 study found very large model edges attractive, while live-paper forensics found the 0.20–0.25 edge band toxic and suggested `max_edge=0.20`, `max_ask=0.70`, and `min_take_usd=10`. This lab uses the **later** guardrails.

Sources:
- https://github.com/LukasNSteel/polybtc
- https://github.com/LukasNSteel/polybtc/blob/main/research/REPORT.md
- https://github.com/LukasNSteel/polybtc/blob/main/research/GO_LIVE_REVIEW_2026-06-16.md

## Four independent $500 accounts

### A — `S5_R10`
5-minute stale-quote sniper. Final 60 seconds only. Favorite side only. Ask 0.50–0.70. Net robust edge after fee must be 0.10–0.20.

### B — `S5_R15`
Same 5-minute sniper but stricter: robust net edge 0.15–0.20. This isolates the high-edge bucket from the more permissive gate.

### C — `S15_R10`
Same robust stale-quote logic on 15-minute markets, final 60 seconds, edge 0.10–0.20.

### D — `SCALP15`
15-minute end-window scalp, final 5–30 seconds. Favorite ask 0.90–0.99 and robust fair probability >= 0.997. The upstream study found 5m scalping negative and 15m/1h more promising, so 5m scalp is deliberately excluded.

Each strategy has its own persistent virtual cash balance starting at $500. Cash, trades, settlement PnL, fees, W/L and FAK fill rate are tracked separately.

## Fair-value adaptation

Implemented from the public research description:

`P(Up) = Phi( ln(S/O) / (sigma * sqrt(tau)) )`

with:
- dual-beta robust bounds: beta `0.83` and `1.36`; a trade must survive the worse regime;
- two-component Gaussian mixture to reduce tail overconfidence;
- Polymarket-mid blend, model weight 0.95 in the final minute;
- fast 60s and slow 10m realized-vol estimates, using the larger one;
- Binance perpetual-led spot estimate: if the perp quote is fresher, use `perp_mid - rolling_basis`; otherwise use spot;
- taker fee `shares * 0.07 * p * (1-p)`.

**Important:** `TAIL_WEIGHT=0.15`, `TAIL_SCALE=2.5`, and the precise EWMA half-lives in this package are adaptation defaults, not claimed byte-for-byte copies of upstream `config.yaml`. They are exposed as environment variables so we can calibrate them from our own hourly data instead of pretending they are known exactly.

## More realistic PAPER fills

A qualifying signal does not fill immediately. The lab waits `PAPER_TAKER_LATENCY_MS=400`, then rechecks the live book. If the original cheap ask disappeared, the FAK attempt is rejected. If it remains, only liquidity at or below the original ask is consumed. Sub-$10 fills are rejected. This is intentionally harsher than the older instant-fill simulator.

Sizing follows the upstream idea `edge / (2 * min_edge)`, capped. With the conservative test cap of $25 per take, an edge exactly at threshold asks for ~$12.50 and a full-strength edge asks for $25. Maximum cost per market is $50 for snipers and $20 for the scalp account.

## Telegram

Buttons:
- START / STOP
- BALANCE — all four accounts separately
- STATISTICS — separate W/L, PnL, fees, ROI-on-cost and FAK fill rate
- POSITIONS — separate open positions
- TRADES — separate last-10 messages for each strategy
- REPORT — ZIP for the last completed UTC hour

At every UTC hour the bot automatically sends a ZIP containing:
- `summary.csv`
- `trades.csv`
- `signals.csv`
- `results.csv`
- `report.txt`

The hourly `signals.csv` is especially important: it preserves fair value, robust probability, edge, spot/perp/effective BTC prices and fast/slow vol so we can diagnose why a strategy won or lost.

## Render

1. Put `main.py`, `requirements.txt`, `.env.example` in a new repo/service.
2. Build: `pip install -r requirements.txt`
3. Start: `python main.py`
4. Persistent disk: `/var/data`
5. Add `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID`.
6. Keep this service PAPER-only. There is no LIVE path in this build.

New DB: `/var/data/polybtc_sniper_lab_v1.db`.

## Expected startup log

`1.0-polybtc-sniper-lab-paper started | PAPER ONLY | S5_R10=$500.00, S5_R15=$500.00, S15_R10=$500.00, SCALP15=$500.00`

Do not judge the strategy from one or two hours. The upstream results themselves were fat-tailed and concentrated in a few windows; compare at least several days and pay special attention to FAK capture rate and maximum drawdown.
