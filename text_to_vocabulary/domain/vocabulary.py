LEXICAL_CATEGORIES = [
    "noun",
    "verb",
    "adverb",
    "adjective",
    "phrasal_verb",
    "idiom",
    "preposition",
    "conjunction",
    "pronoun",
    "interjection",
    "article",
    "numeral",
    "particle",
    "determiner",
    "proper_noun",
    "other",
]


def dedupe_preserve_order(items):
    seen = set()
    result = []
    for item in items:
        if item is None:
            continue
        if not isinstance(item, str):
            item = str(item)
        item = item.strip()
        if item and item not in seen:
            seen.add(item)
            result.append(item)
    return result


def format_markdown_table(data):
    def join_words(words):
        return ", ".join(words) if words else "-"

    header = "| Type | Words |\n| --- | --- |\n"
    rows = [
        f"| {key} | {join_words(data.get(key, []))} |"
        for key in LEXICAL_CATEGORIES
    ]
    return header + "\n".join(rows)
