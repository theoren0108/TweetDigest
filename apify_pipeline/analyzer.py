import importlib.util
import os
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Sequence, Union

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
    tokens = re.findall(r"[A-Za-z\d_]+", text.lower())
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


def split_by_category(posts: Iterable[Dict], category_map: Dict[str, str]) -> Dict[str, List[Dict]]:
    buckets: Dict[str, List[Dict]] = defaultdict(list)
    for post in posts:
        author = post.get("author", "").lower()
        category = category_map.get(author, "Uncategorized")
        buckets[category].append(post)
    return buckets


def format_post(post: Dict) -> str:
    created_at = parse_timestamp(post["created_at"]).strftime("%Y-%m-%d %H:%M UTC")
    text = post.get("text", "").strip().replace("\n", " ")
    return f"- {created_at} — {text} ({post.get('url', '')})"


def build_report(
    posts: List[Dict],
    window_label: str,
    summary: Optional[Union[str, Dict[str, str]]] = None,
    category_map: Optional[Dict[str, str]] = None,
) -> str:
    if not posts:
        return f"## Daily digest ({window_label})\n\nNo new posts found in this window."

    keywords = extract_keywords(posts, top_n=12)
    
    # Organize posts
    if category_map:
        by_category = split_by_category(posts, category_map)
    else:
        by_category = {"All": posts}

    lines = [f"## Daily digest ({window_label})\n"]
    lines.append(f"Total new posts: **{len(posts)}**. Top keywords: {', '.join(keywords) if keywords else 'N/A'}.\n")

    # Handle summaries (Global or Per-Category)
    if summary:
        if isinstance(summary, dict):
            # Per-category summary
            lines.append("## Sector Summaries\n")
            for cat, text in summary.items():
                lines.append(f"### {cat}\n")
                lines.append(text.strip())
                lines.append("")
        else:
            # Global summary
            lines.append("### LLM summary\n")
            lines.append(summary.strip())
            lines.append("")

    lines.append("## Posts by Category\n")
    
    # Sort categories: specific ones first, "Uncategorized" last
    sorted_cats = sorted(by_category.keys(), key=lambda x: (x == "Uncategorized", x))
    
    for category in sorted_cats:
        cat_posts = by_category[category]
        if not cat_posts:
            continue
            
        lines.append(f"### {category}\n")
        
        by_author = split_by_author(cat_posts)
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
    model: str = "deepseek-reasoner",
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
    max_posts: int = 30,
    category: Optional[str] = None,
) -> str:
    """
    Summarize a set of posts with an LLM.

    Args:
        posts: Iterable of post dictionaries with at least author/text/url keys.
        model: OpenAI chat model to use.
        api_key: OpenAI API key. Falls back to OPENAI_API_KEY env var.
        max_posts: Maximum number of posts to include in the prompt.
        category: Optional category name to contextualize the summary.
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

    context_str = f"“{category} 领域的”" if category else ""
    
    prompt = (
        f"你是一名 buy-side 投研助理，任务是把{context_str}“过去48小时的X(KOL)内容”提炼成可交易、可验证、可跟踪的情报简报。不要复述流水账；要提纯信号、指出关键变量与下一步动作。输出用中文，结论优先，少形容词。\n\n"
        "【总原则（必须遵守）】\n"
        "1) Signal > Noise：默认把“情绪宣泄/社交互动/明显玩笑或讽刺/无信息量转发”归为噪音，除非它引发了市场交易或提供了可核验事实。\n"
        "2) 事实 vs 观点：每条关键结论必须标注【事实】或【观点】；事实需给出原文证据（引用不超过25个中文字符或15个英文单词）并附上对应链接。\n"
        "3) 可交易性：每个主题必须回答“为什么重要→影响谁→需要盯什么变量→可能的交易表达方式（泛化，不给具体买卖建议）→最大不确定性”。\n"
        "4) 可信度打分：对每个主题给出 1-5 分可信度（5最高），并用一句话解释评分依据（证据密度/可核验程度/是否自洽/是否仅喊单）。\n"
        "5) 输出要短而硬：总字数尽量控制在 600-900 中文字；宁可少写但更有用。\n\n"
        "【输出结构（严格按顺序）】\n"
        "A) 60秒TL;DR（<=5条）\n"
        "- 每条格式：结论一句话（<=26字）｜关键变量（<=12字）｜可信度(1-5)\n\n"
        "B) Top 3 主题拆解（按“可交易性×新信息量”排序）\n"
        "对每个主题输出一个小节，包含：\n"
        "1. 核心判断（1-2句）\n"
        "2. 证据（2-4条要点；每条：引用短句 + 链接 + 来自哪个账号）\n"
        "3. 关键变量/数据（列 3-6 个，必须是后续可跟踪的指标）\n"
        "4. 影响路径（用 1 条因果链：A→B→C）\n"
        "5. 潜在受益/受损标的（可写行业/类型/已有ticker；不确定就写“待补”）\n"
        "6. 最大不确定性（1句）\n"
        "7. 明日跟踪动作（2-4条，可执行：要看什么数据/新闻/价格/公告）\n\n"
        "C) Ticker/主题看板（表格）\n"
        "列：Ticker或主题｜提及账号&次数｜情绪(正/负/混合)｜“新信息点”一句话｜下一步验证\n"
        "- 只列你认为“有投研价值”的最多 8 行；其余不列\n\n"
        "D) 噪音与风险提示（<=4条）\n"
        "- 标注：哪些内容明显是玩笑/讽刺/未经证实的rumor/情绪喊单，并说明为什么应忽略或如何验证。\n\n"
        "【你必须覆盖的细节】\n"
        "- 如果某账号发了大量串推（如核燃料链条），要总结成“结构框架+变量”，不要逐条复述。\n"
        "- 如果出现明显虚构/讽刺（如夸张政治军事剧情），必须在D部分点名为“非事实信号”。\n"
        "- 如果出现具体数字（如发行份额、lbs、折溢价、NAV等），优先纳入证据与变量。\n\n"
        "下面是内容：\n"
        + "\n".join(lines)
    )

    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "You are a professional buy-side investment research assistant. Output in Chinese."},
            {"role": "user", "content": prompt},
        ],
        temperature=0.2,
    )
    return (response.choices[0].message.content or "").strip()
