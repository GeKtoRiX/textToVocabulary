from dataclasses import dataclass
import os


@dataclass(frozen=True)
class CacheEntry:
    signature: tuple[float, int] | None
    words: list[str]


def _stat_signature(path):
    try:
        stats = os.stat(path)
    except FileNotFoundError:
        return None
    return (stats.st_mtime, stats.st_size)


class VocabularyCache:
    def __init__(self):
        self._entries = {}

    def get_words(self, path, loader):
        signature = _stat_signature(path)
        entry = self._entries.get(path)
        if entry and entry.signature == signature:
            return entry.words
        words = loader(path)
        self._entries[path] = CacheEntry(signature=signature, words=words)
        return words

    def update_words(self, path, words):
        signature = _stat_signature(path)
        self._entries[path] = CacheEntry(signature=signature, words=words)
