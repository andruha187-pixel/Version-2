import os
import tempfile

# Must be set before importing main because DATA_DIR is read at import time.
tmp = tempfile.mkdtemp(prefix="polybtc15m_")
os.environ["DATA_DIR"] = tmp

import main

names = [s["name"] for s in main.STRATEGIES]
assert names == ["S15_E10_60", "S15_E125_60", "S15_E15_60", "S15_E125_90"]
assert all(s["kind"] == "15m" for s in main.STRATEGIES)
assert all(s["leg"] == "snipe" for s in main.STRATEGIES)
assert [s["min_edge"] for s in main.STRATEGIES[:3]] == [0.10, 0.125, 0.15]
assert [s["max_tau"] for s in main.STRATEGIES] == [60.0, 60.0, 60.0, 90.0]
assert all(s["max_edge"] == 0.20 for s in main.STRATEGIES)
assert all(s["min_ask"] == 0.50 and s["max_ask"] == 0.70 for s in main.STRATEGIES)

main.init_db()
for s in main.STRATEGIES:
    assert abs(main.paper_cash(s["name"]) - 500.0) < 1e-9
    assert abs(main.paper_initial(s["name"]) - 500.0) < 1e-9

# Sizing is independent per strategy and positive at threshold.
for s in main.STRATEGIES:
    size = main.target_take_usd(s, s["min_edge"])
    assert s["min_take_usd"] <= size <= s["max_take_usd"]

print("15m edge lab regression: OK")
