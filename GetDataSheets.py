import os
import re
import time
import random
import hashlib
from urllib.parse import urlparse, urljoin

import pandas as pd
import requests

try:
    from ddgs import DDGS  # renamed package
except ImportError:  # pragma: no cover - fallback when ddgs isn't installed
    try:
        from duckduckgo_search import DDGS  # type: ignore
    except ImportError:  # pragma: no cover
        DDGS = None


# =========================
# CONFIG
# =========================
EXCEL_PATH = "products.xlsx"          # input Excel file path
OUTPUT_DIR = "data sheets"            # where PDFs will be saved

# IMPORTANT FIX:
# If SHEET_NAME is None, pandas returns a dict of DataFrames (one per sheet).
# So we default to reading a single sheet by setting SHEET_NAME to 0 (first sheet).
# If you know your sheet name, set it like: SHEET_NAME = "Products"
SHEET_NAME = 0                        # 0 = first sheet, or use a string sheet name e.g. "Products"

SEARCH_RESULTS_PER_ITEM = 15          # how many results to consider per product
REQUEST_TIMEOUT = 30
SLEEP_BETWEEN_PRODUCTS_SEC = (1.0, 2.5)  # random sleep range between products
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


# =========================
# HELPERS
# =========================
def safe_filename(name: str, max_len: int = 150) -> str:
    """
    Convert a product code into a filesystem-safe filename.
    Keeps it readable but strips characters that cause trouble on Windows/macOS/Linux.
    """
    name = str(name).strip()
    name = re.sub(r"[\\/:*?\"<>|]+", "_", name)  # Windows-illegal chars
    name = re.sub(r"\s+", " ", name)             # normalize spaces
    name = name.strip(" .")                      # avoid trailing dots/spaces on Windows
    if len(name) > max_len:
        # shorten deterministically
        h = hashlib.sha1(name.encode("utf-8")).hexdigest()[:8]
        name = name[: max_len - 9] + "_" + h
    return name


def normalize_column_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(name).strip().lower())


def resolve_column(df: pd.DataFrame, candidates: set[str]) -> str:
    normalized = {normalize_column_name(col): col for col in df.columns}
    for candidate in candidates:
        if candidate in normalized:
            return normalized[candidate]
    raise ValueError(
        "Excel must contain columns for Brand and ProductCode "
        f"(searched for {sorted(candidates)})."
    )


def resolve_optional_column(df: pd.DataFrame, candidates: set[str]) -> str | None:
    normalized = {normalize_column_name(col): col for col in df.columns}
    for candidate in candidates:
        if candidate in normalized:
            return normalized[candidate]
    return None


def build_output_filename(brand: str, code: str) -> str:
    parts = [code.strip()]
    if brand and brand.strip():
        parts.append(brand.strip())
    return safe_filename(" ".join(parts)) + ".pdf"


def looks_like_pdf_url(url: str) -> bool:
    try:
        path = urlparse(url).path.lower()
        return path.endswith(".pdf")
    except Exception:
        return False


def request_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    return s


def head_or_get_is_pdf(sess: requests.Session, url: str) -> bool:
    """
    Some datasheet links don't end in .pdf (CDN / redirects).
    This checks Content-Type quickly.
    """
    try:
        r = sess.head(url, allow_redirects=True, timeout=REQUEST_TIMEOUT)
        ctype = (r.headers.get("Content-Type") or "").lower()
        if "application/pdf" in ctype:
            return True
    except Exception:
        pass

    try:
        r = sess.get(url, allow_redirects=True, timeout=REQUEST_TIMEOUT, stream=True)
        ctype = (r.headers.get("Content-Type") or "").lower()
        return "application/pdf" in ctype
    except Exception:
        return False


def download_pdf(sess: requests.Session, url: str, out_path: str) -> bool:
    """
    Stream download to disk. Returns True on success.
    """
    try:
        with sess.get(url, allow_redirects=True, timeout=REQUEST_TIMEOUT, stream=True) as r:
            r.raise_for_status()
            ctype = (r.headers.get("Content-Type") or "").lower()
            # Allow if content-type is PDF OR the final URL ends with .pdf
            if "application/pdf" not in ctype and not looks_like_pdf_url(r.url):
                return False

            os.makedirs(os.path.dirname(out_path), exist_ok=True)
            tmp_path = out_path + ".part"

            with open(tmp_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 128):
                    if chunk:
                        f.write(chunk)

            # Basic sanity check: PDF files start with %PDF
            with open(tmp_path, "rb") as f:
                start = f.read(4)
            if start != b"%PDF":
                os.remove(tmp_path)
                return False

            os.replace(tmp_path, out_path)
            return True

    except Exception:
        return False


def build_queries(brand: str, code: str, description: str) -> list[str]:
    """
    Queries tuned to pull datasheets, not shopping pages.
    """
    bits = []
    if brand:
        bits.append(brand)
    bits.append(code)
    if description:
        bits.append(description)
    base = " ".join(bits)
    quoted_code = f"\"{code}\"" if code else ""
    quoted_brand = f"\"{brand}\"" if brand else ""
    return [
        f"{base} datasheet filetype:pdf",
        f"{base} datasheet pdf",
        f"{base} technical datasheet pdf",
        f"{base} technical data sheet pdf",
        f"{base} spec sheet pdf",
        f"{base} product data sheet pdf",
        f"{base} specifications pdf",
        f"{base} datasheet",
        f"{base} technical datasheet",
        f"{base} spec sheet",
        f"{base} technical specifications",
        f"{quoted_brand} {quoted_code} datasheet pdf".strip(),
        f"{quoted_brand} {quoted_code} data sheet pdf".strip(),
        f"{quoted_brand} {quoted_code} spec sheet pdf".strip(),
    ]


def normalize_match_text(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", text.lower())


def text_contains_token(text: str, token: str) -> bool:
    if not text or not token:
        return False
    return normalize_match_text(token).strip() in normalize_match_text(text)


def score_candidate(
    url: str,
    title: str,
    body: str,
    brand: str,
    code: str,
    description: str,
) -> int:
    text = " ".join([url or "", title or "", body or ""])
    score = 0
    if looks_like_pdf_url(url):
        score += 5
    for keyword in ("datasheet", "data sheet", "spec sheet", "technical datasheet"):
        if keyword in text.lower():
            score += 4
            break
    if text_contains_token(text, code):
        score += 6
    if text_contains_token(text, brand):
        score += 3
    if description and text_contains_token(text, description):
        score += 1

    negative_keywords = (
        "manual",
        "installation",
        "brochure",
        "catalog",
        "safety data sheet",
        "sds",
        "msds",
        "drawing",
        "cad",
        "certificate",
        "guide",
        "handbook",
        "user manual",
    )
    for keyword in negative_keywords:
        if keyword in text.lower():
            score -= 4
            break
    return score


def filter_pdf_links(
    pdf_links: list[str], brand: str, code: str, description: str
) -> list[str]:
    scored: list[tuple[int, str]] = []
    for link in pdf_links:
        score = score_candidate(link, "", "", brand, code, description)
        scored.append((score, link))
    scored.sort(key=lambda item: item[0], reverse=True)
    return [link for _, link in scored]


def extract_pdf_links_from_html(html: str, base_url: str) -> list[str]:
    """
    Extract PDF links from an HTML page.
    """
    links = re.findall(r'href=["\']([^"\']+\.pdf[^"\']*)["\']', html, re.IGNORECASE)
    resolved = []
    for link in links:
        resolved.append(urljoin(base_url, link))
    # De-dupe while preserving order
    seen = set()
    deduped = []
    for link in resolved:
        if link not in seen:
            seen.add(link)
            deduped.append(link)
    return deduped


def find_best_pdf_links(
    queries: list[str], brand: str, code: str, description: str
) -> list[str]:
    """
    Returns a list of candidate URLs (best-first) from DuckDuckGo results.
    """
    if DDGS is None:
        raise RuntimeError(
            "Search dependency missing. Install 'ddgs' (preferred) or 'duckduckgo_search'."
        )
    results: list[tuple[int, str]] = []
    with DDGS() as ddgs:
        for query in queries:
            for r in ddgs.text(query, max_results=SEARCH_RESULTS_PER_ITEM):
                u = r.get("href") or r.get("url")
                if not u:
                    continue
                title = r.get("title") or ""
                body = r.get("body") or ""
                score = score_candidate(u, title, body, brand, code, description)
                results.append((score, u))

    results.sort(key=lambda item: (item[0], 1 if looks_like_pdf_url(item[1]) else 0), reverse=True)

    # De-dupe while preserving order
    seen = set()
    deduped = []
    for _, u in results:
        if u not in seen:
            seen.add(u)
            deduped.append(u)
    return deduped


def load_products_excel(path: str, sheet_name):
    """
    Load Excel robustly. Always returns a DataFrame.
    """
    df = pd.read_excel(path, sheet_name=sheet_name)

    # If someone sets sheet_name=None, pandas returns a dict of DataFrames.
    if isinstance(df, dict):
        # Take the first sheet
        df = next(iter(df.values()))

    if not hasattr(df, "columns"):
        raise TypeError("Excel load did not return a DataFrame. Check sheet_name and file format.")

    # Normalize column names
    df.columns = [str(c).strip() for c in df.columns]
    return df


# =========================
# MAIN
# =========================
def main():
    df = load_products_excel(EXCEL_PATH, SHEET_NAME)

    brand_col = resolve_column(df, {"brand", "brandname"})
    code_col = resolve_column(df, {"productcode", "productid", "productnumber"})
    description_col = resolve_optional_column(
        df, {"description", "productdescription", "details", "notes"}
    )

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    sess = request_session()

    total = len(df)
    ok = 0
    skipped = 0
    failed = 0

    for idx, row in df.iterrows():
        brand = str(row.get(brand_col) or "").strip()
        code = str(row.get(code_col) or "").strip()
        description = ""
        if description_col:
            description = str(row.get(description_col) or "").strip()

        if not code:
            skipped += 1
            print(f"[{idx+1}/{total}] SKIP: empty ProductCode")
            continue

        filename = build_output_filename(brand, code)
        out_path = os.path.join(OUTPUT_DIR, filename)

        if os.path.exists(out_path) and os.path.getsize(out_path) > 10_000:
            skipped += 1
            print(f"[{idx+1}/{total}] SKIP: already exists -> {filename}")
            continue

        queries = build_queries(brand, code, description)
        print(f"[{idx+1}/{total}] Searching: {queries[0]}")

        try:
            candidates = find_best_pdf_links(queries, brand, code, description)
        except Exception as e:
            failed += 1
            print(f"  FAIL search: {e}")
            continue

        downloaded = False
        for url in candidates:
            is_pdf = looks_like_pdf_url(url) or head_or_get_is_pdf(sess, url)
            if is_pdf:
                print(f"  Trying: {url}")
                if download_pdf(sess, url, out_path):
                    ok += 1
                    downloaded = True
                    print(f"  OK -> {out_path}")
                    break
                continue

            try:
                r = sess.get(url, allow_redirects=True, timeout=REQUEST_TIMEOUT)
                if r.ok and "text/html" in (r.headers.get("Content-Type") or "").lower():
                    pdf_links = extract_pdf_links_from_html(r.text, r.url)
                    pdf_links = filter_pdf_links(pdf_links, brand, code, description)
                    for pdf_url in pdf_links:
                        print(f"  Trying: {pdf_url}")
                        if download_pdf(sess, pdf_url, out_path):
                            ok += 1
                            downloaded = True
                            print(f"  OK -> {out_path}")
                            break
                if downloaded:
                    break
            except Exception:
                continue

        if not downloaded:
            failed += 1
            print("  FAIL: no working PDF found")

        time.sleep(random.uniform(*SLEEP_BETWEEN_PRODUCTS_SEC))

    print("\n=== Summary ===")
    print(f"Downloaded: {ok}")
    print(f"Skipped:    {skipped}")
    print(f"Failed:     {failed}")


if __name__ == "__main__":
    main()
