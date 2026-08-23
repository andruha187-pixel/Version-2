import math
import main

# Fee sanity.
assert abs(main.fee_per_share(0.5) - 0.0175) < 1e-12

# Fair probability rises as BTC moves above the candle open.
p0 = main.fair_up_bounds(100000, 100000, 0.0001, 30, 0.50)
p1 = main.fair_up_bounds(100050, 100000, 0.0001, 30, 0.50)
p2 = main.fair_up_bounds(99950, 100000, 0.0001, 30, 0.50)
assert min(p1) > min(p0) > min(p2)

# Robust gate is the worst beta for the selected side.
up_robust = min(p1)
down_robust = min(1.0 - x for x in p1)
assert up_robust > 0.5
assert down_robust < 0.5

# Sizing: at the threshold the research-style conviction scale is 0.5 of max take.
s = main.STRATEGY_BY_NAME["S5_R10"]
assert abs(main.target_take_usd(s, 0.10) - 12.5) < 1e-9
assert abs(main.target_take_usd(s, 0.20) - 25.0) < 1e-9

print("model regression: OK")
