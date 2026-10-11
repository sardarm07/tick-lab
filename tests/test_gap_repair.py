from ticklab import gap_repair
from ticklab.gap_repair import WeightLimiter


def test_the_limiter_waits_once_the_minute_budget_is_spent(monkeypatch):
    clock = {"now": 1000.0}
    slept = []
    monkeypatch.setattr(gap_repair.time, "monotonic", lambda: clock["now"])

    def sleep(seconds: float) -> None:
        slept.append(seconds)
        clock["now"] += seconds

    monkeypatch.setattr(gap_repair.time, "sleep", sleep)
    limiter = WeightLimiter(per_minute=100)
    for _ in range(4):
        limiter.spend(25)  # 100 in the first minute: no waiting
    assert slept == []
    clock["now"] += 10
    limiter.spend(25)  # the oldest spend leaves the window 60 s after it was made
    assert slept == [50.0]
