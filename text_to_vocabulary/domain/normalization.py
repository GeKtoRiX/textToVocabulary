import re
import warnings
from typing import Iterable


_SPACE_RE = re.compile(r"\s+")
_CATEGORY_POS = {
    "noun": "NOUN",
    "verb": "VERB",
    "phrasal_verb": "VERB",
}

_SPACY_INSTALL_MESSAGE = (
    "spaCy is required for lemmatization. Install it with `pip install spacy` "
    "and download the model with `python -m spacy download en_core_web_sm`."
)

_NLP = None
_LEMMATIZER = None
_NLP_ERROR = None
_WARNED = False


def canonicalize(word: str, category: str, *, use_casefold: bool = True) -> tuple[str, str]:
    surface = _normalize_surface(word)
    if not surface:
        raise ValueError("word is empty after normalization")

    normalized = _normalize_case(surface, use_casefold=use_casefold)
    lemma = normalized
    pos = _CATEGORY_POS.get((category or "").strip().lower())
    if pos:
        lemma = _lemmatize(normalized, pos) or normalized

    return _normalize_case(lemma, use_casefold=use_casefold), surface


def canonicalize_batch(
    words: Iterable[str], category: str, *, use_casefold: bool = True
) -> list[tuple[str, str]]:
    surfaces = []
    for word in words or []:
        surface = _normalize_surface(word)
        if surface:
            surfaces.append(surface)

    if not surfaces:
        return []

    normalized = [_normalize_case(surface, use_casefold=use_casefold) for surface in surfaces]
    pos = _CATEGORY_POS.get((category or "").strip().lower())
    if not pos:
        return list(zip(normalized, surfaces))

    unique = []
    index_map = {}
    for item in normalized:
        if item not in index_map:
            index_map[item] = len(unique)
            unique.append(item)

    lemmas = _lemmatize_batch(unique, pos)
    lemma_map = {}
    for item, lemma in zip(unique, lemmas):
        lemma_map[item] = _normalize_case(lemma or item, use_casefold=use_casefold)

    results = []
    for surface, fallback in zip(surfaces, normalized):
        results.append((lemma_map[fallback], surface))
    return results


def _normalize_surface(word: str | None) -> str | None:
    if word is None:
        return None
    if not isinstance(word, str):
        word = str(word)
    cleaned = _SPACE_RE.sub(" ", word.strip())
    return cleaned or None


def _normalize_case(text: str, *, use_casefold: bool) -> str:
    return text.casefold() if use_casefold else text.lower()


def _get_nlp():
    global _NLP, _LEMMATIZER, _NLP_ERROR
    if _NLP is not None:
        return _NLP
    if _NLP_ERROR is not None:
        raise _NLP_ERROR
    try:
        import spacy
    except ImportError as exc:
        _NLP_ERROR = RuntimeError(_SPACY_INSTALL_MESSAGE)
        raise _NLP_ERROR from exc
    try:
        _NLP = spacy.load("en_core_web_sm", disable=["parser", "ner"])
    except OSError as exc:
        _NLP_ERROR = RuntimeError(_SPACY_INSTALL_MESSAGE)
        raise _NLP_ERROR from exc
    try:
        _LEMMATIZER = _NLP.get_pipe("lemmatizer")
    except (KeyError, AttributeError):
        _LEMMATIZER = None
    return _NLP


def _warn_spacy(message: str) -> None:
    global _WARNED
    if _WARNED:
        return
    warnings.warn(message)
    _WARNED = True


def _lemmatize(text: str, pos: str) -> str | None:
    try:
        nlp = _get_nlp()
    except RuntimeError as exc:
        _warn_spacy(str(exc))
        return None
    return _lemmatize_doc(nlp.tokenizer(text), pos, _LEMMATIZER)


def _lemmatize_batch(texts: list[str], pos: str) -> list[str | None]:
    try:
        nlp = _get_nlp()
    except RuntimeError as exc:
        _warn_spacy(str(exc))
        return [None for _ in texts]
    lemmatizer = _LEMMATIZER
    return [_lemmatize_doc(nlp.tokenizer(text), pos, lemmatizer) for text in texts]


def _lemmatize_doc(doc, pos: str, lemmatizer) -> str | None:
    if not doc:
        return None
    parts = []
    for token in doc:
        if token.is_space:
            parts.append(token.text)
            continue
        lemma = _lemmatize_token(token, pos, lemmatizer)
        if not lemma or lemma == "-PRON-":
            lemma = token.text
        parts.append(lemma + token.whitespace_)
    joined = "".join(parts).strip()
    return joined or None


def _lemmatize_token(token, pos: str, lemmatizer) -> str:
    if lemmatizer is not None:
        lemma = _lemma_from_lemmatizer(lemmatizer, token.text, pos)
        if lemma:
            return lemma
    return token.lemma_ or token.text


def _lemma_from_lemmatizer(lemmatizer, text: str, pos: str) -> str | None:
    for method_name in ("lookup_lemmatize", "rule_lemmatize", "lemmatize"):
        method = getattr(lemmatizer, method_name, None)
        if not method:
            continue
        try:
            result = method(text, pos)
        except TypeError:
            continue
        if isinstance(result, (list, tuple)):
            for item in result:
                if item:
                    return item
        elif isinstance(result, str) and result:
            return result
    return None
