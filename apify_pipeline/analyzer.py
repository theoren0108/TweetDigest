import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Dict, Iterable, List

STOPWORDS = {
    "the",
    "and",
    "of",
    "a",
    "to",
    "in",
    "for",
    "on",
    "is",
    "are",
    "it",
    "this",
    "that",
    "with",
    "at",
    "we",
    "you",
    "i",
    "our",
    "by",
    "from",
    "as",
    "be",
    "an",
    "or",
    "was",
    "were",
    "has",
    "have",
}


def parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def normalize_text(text: str) -> List[str]:
    tokens = re.findall(r"[A-Za-z\\d_]+", text.lower())
    return [t for t in tokens if t not in STOPWORDS and len(t) > 2]


def extract_keywords(posts: Iterable[Dict], top_n: int = 10) -> List[str]:
    counter: Counter = Counter()
    for post in posts:
        for token in normalize_text(post.get("text", "")):
            counter[token] += 1
    return [word for word, _ in counter.most_common(top_n)]


def split_by_author(posts: Iterable[Dict]) -> Dict[str, List[Dict]]:
    buckets: Dict[str, List[Dict]] = defaultdict(list)
    for post in posts:
        buckets[post.get("author", "").lower()].append(post)
    return buckets


def format_post(post: Dict) -> str:
    created_at = parse_timestamp(post["created_at"]).strftime("%Y-%m-%d %H:%M UTC")
    text = post.get("text", "").strip().replace("\n", " ")
    return f"- {created_at} — {text} ({post.get('url', '')})"


def build_report(posts: List[Dict], window_label: str) -> str:
    if not posts:
        return f"## Daily digest ({window_label})\n\nNo new posts found in this window."

    keywords = extract_keywords(posts, top_n=12)
    by_author = split_by_author(posts)

    lines = [f"## Daily digest ({window_label})\n"]
    lines.append(f"Total new posts: **{len(posts)}**. Top keywords: {', '.join(keywords) if keywords else 'N/A'}.\n")

    lines.append("### Posts by account\n")
    for author, author_posts in sorted(by_author.items()):
        lines.append(f"**@{author}** — {len(author_posts)} posts")
        for post in sorted(author_posts, key=lambda p: p.get("created_at", "")):
            lines.append(format_post(post))
        lines.append("")

    lines.append("### Quick themes (frequency only)\n")
    for kw in keywords:
        lines.append(f"- {kw}")

    lines.append(
        "\n_This report was generated via an automated crawl + lightweight keyword stats. Consider layering an LLM for richer summaries when volume warrants it._"
    )
    return "\n".join(lines)
