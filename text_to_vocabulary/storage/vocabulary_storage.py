from abc import ABC, abstractmethod
from typing import Iterable


class VocabularyStorage(ABC):
    @abstractmethod
    def get_categories(self) -> list[str]:
        raise NotImplementedError

    @abstractmethod
    def get_words(
        self,
        category: str,
        *,
        search: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[str]:
        raise NotImplementedError

    @abstractmethod
    def add_words(self, category: str, words: Iterable[str], source: str | None = None) -> int:
        raise NotImplementedError

    @abstractmethod
    def merge_words(
        self, category: str, words: Iterable[str], source: str | None = None
    ) -> int:
        raise NotImplementedError

    @abstractmethod
    def is_empty(self) -> bool:
        raise NotImplementedError
