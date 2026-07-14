from collections import Counter
from time import perf_counter


class Metrics:
    def __init__(self) -> None:
        self._counters: Counter[str] = Counter()

    def inc(self, key: str, value: int = 1) -> None:
        self._counters[key] += value

    def snapshot(self) -> dict[str, int]:
        return dict(self._counters)


class Timer:
    def __init__(self) -> None:
        self._started_at = perf_counter()

    def ms(self) -> int:
        return int((perf_counter() - self._started_at) * 1000)
