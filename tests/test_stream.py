import random

from ticklab.stream import Backoff


def test_backoff_doubles_up_to_the_cap_with_jitter_in_the_upper_half():
    backoff = Backoff(base_s=1, cap_s=60, rng=random.Random(42))
    for attempt in range(10):
        ceiling = min(60, 2**attempt)
        delay = backoff.next()
        assert ceiling / 2 <= delay <= ceiling


def test_backoff_reset_starts_again_from_the_base():
    backoff = Backoff(base_s=1, cap_s=60, rng=random.Random(1))
    for _ in range(8):
        backoff.next()
    backoff.reset()
    assert 0.5 <= backoff.next() <= 1


def test_jitter_spreads_clients_out():
    delays = {round(Backoff(rng=random.Random(seed)).next(), 6) for seed in range(20)}
    assert len(delays) == 20
