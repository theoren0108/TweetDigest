import importlib.util
import os
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Sequence

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


def build_report(posts: List[Dict], window_label: str, summary: Optional[str] = None) -> str:
    if not posts:
        return f"## Daily digest ({window_label})\n\nNo new posts found in this window."

    keywords = extract_keywords(posts, top_n=12)
    by_author = split_by_author(posts)

    lines = [f"## Daily digest ({window_label})\n"]
    lines.append(f"Total new posts: **{len(posts)}**. Top keywords: {', '.join(keywords) if keywords else 'N/A'}.\n")
    if summary:
        lines.append("### LLM summary\n")
        lines.append(summary.strip())
        lines.append("")

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


def summarize_posts(
    posts: Sequence[Dict],
    model: str = "gpt-4o-mini",
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
    max_posts: int = 30,
) -> str:
    """
    Summarize a set of posts with an LLM.

    Args:
        posts: Iterable of post dictionaries with at least author/text/url keys.
        model: OpenAI chat model to use.
        api_key: OpenAI API key. Falls back to OPENAI_API_KEY env var.
        max_posts: Maximum number of posts to include in the prompt.
    """
    material = list(posts)
    if not material:
        return "No posts available to summarize."

    key = api_key or os.environ.get("OPENAI_API_KEY") or os.environ.get("DEEPSEEK_API_KEY")
    if not key:
        raise RuntimeError("Provide OPENAI_API_KEY or DEEPSEEK_API_KEY (or use --summary-api-key) to summarize posts")

    if importlib.util.find_spec("openai") is None:
        raise RuntimeError("Install the 'openai' package to enable LLM summarization")

    base_url = base_url or os.environ.get("OPENAI_BASE_URL") or os.environ.get("DEEPSEEK_API_BASE")

    from openai import OpenAI

    client = OpenAI(api_key=key, base_url=base_url)

    sorted_posts = sorted(material, key=lambda p: p.get("created_at") or "", reverse=True)
    trimmed_posts = sorted_posts[: max_posts if max_posts and max_posts > 0 else len(sorted_posts)]

    lines = []
    for post in trimmed_posts:
        created_at = post.get("created_at") or ""
        author = post.get("author", "")
        text = (post.get("text") or "").strip().replace("\n", " ")
        if len(text) > 400:
            text = text[:400] + "..."
        url = post.get("url") or ""
        lines.append(f"- [{created_at}] @{author}: {text} (link: {url})")

    prompt = (
        "Summarize the key developments, sentiment, and any noteworthy media references "
        "from these social posts. Keep the response concise (4-8 bullet points) and actionable, "
        "calling out accounts, dates, and tickers when helpful.\n\nPosts:\n"
        + "\n".join(lines)
    )

    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "You are an analyst who writes crisp summaries of social media posts."},
            {"role": "user", "content": prompt},
        ],
        temperature=0.2,
    )
    return (response.choices[0].message.content or "").strip()
