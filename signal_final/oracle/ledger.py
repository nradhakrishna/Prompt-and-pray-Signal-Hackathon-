"""Small idempotent in-memory reputation ledger for the demo."""

from __future__ import annotations

from dataclasses import dataclass

from .attribution import FaultCategory, worker_is_accountable


@dataclass(frozen=True, slots=True)
class LedgerEntry:
    key: str
    worker_id: str
    delta: int
    reason: str


class Ledger:
    def __init__(self, *, initial_balance: int = 100, reward: int = 5, slash: int = 10) -> None:
        self.initial_balance = initial_balance
        self.reward = reward
        self.slash = slash
        self._balances: dict[str, int] = {}
        self._entries: dict[str, LedgerEntry] = {}

    def balance(self, worker_id: str) -> int:
        return self._balances.get(worker_id, self.initial_balance)

    @property
    def entries(self) -> tuple[LedgerEntry, ...]:
        return tuple(self._entries.values())

    def settle(self, key: str, worker_id: str, passed: bool,
               category: FaultCategory | None = None) -> LedgerEntry:
        """Reward PASS, slash accountable faults, and record neutral faults once."""

        if key in self._entries:
            return self._entries[key]
        delta = self.reward if passed else (-self.slash if category and worker_is_accountable(category) else 0)
        reason = "PASS" if passed else (category.value if category else "FAIL")
        entry = LedgerEntry(key, worker_id, delta, reason)
        self._entries[key] = entry
        self._balances[worker_id] = self.balance(worker_id) + delta
        return entry
