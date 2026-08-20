# Strategy Simulator V2 — Binance Research

Отдельная исследовательская версия для второго репозитория и Telegram-бота.

Все 12 исходных Polymarket-стратегий сохранены без изменения. Binance BTCUSDT Futures используется только как внешний источник признаков.

Для каждого сигнала: 250ms/500ms/1s/3s/10s momentum, aggressive flow 1/3/10/30s, top-10 book imbalance, large-trade delta, EMA9/21, RSI14, цена BTC около старта 5m, distance-to-start, path efficiency, direction changes, режим TREND/MIXED/CHOP, freshness и confidence 0-100.

Shadow-пороги: CONF55 / CONF60 / CONF65 / CONF70 / CONF75. Они не меняют BASE-стратегии, а только считают, что было бы при фильтрации.

В часовом ZIP: обычные BASE CSV + binance_v2_features.csv, binance_shadow_trades.csv, binance_shadow_results.csv, binance_shadow_summary.csv.
