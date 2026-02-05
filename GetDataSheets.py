import os
import re
import time
import random
import hashlib
from urllib.parse import urlparse

import pandas as pd
import requests
from duckduckgo_search import DDGS


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

SEARCH_RESULTS_PER_ITEM = 8           # how many results to consider per product
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


def build_query(brand: str, code: str) -> str:
    """
    Query tuned to pull datasheets, not shopping pages.
    """
    bits = []
    if brand:
        bits.append(brand)
    bits.append(code)
    bits.append("datasheet filetype:pdf")
    return " ".join(bits)


def find_best_pdf_links(query: str) -> list[str]:
    """
    Returns a list of candidate URLs (best-first) from DuckDuckGo results.
    """
    urls: list[str] = []
    with DDGS() as ddgs:
        for r in ddgs.text(query, max_results=SEARCH_RESULTS_PER_ITEM):
            u = r.get("href") or r.get("url")
            if u:
                urls.append(u)

    # Prefer direct PDFs first, then anything else (we'll content-type check)
    urls.sort(key=lambda u: 0 if looks_like_pdf_url(u) else 1)

    # De-dupe while preserving order
    seen = set()
    deduped = []
    for u in urls:
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

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    sess = request_session()

    total = len(df)
    ok = 0
    skipped = 0
    failed = 0

    for idx, row in df.iterrows():
        brand = str(row.get(brand_col) or "").strip()
        code = str(row.get(code_col) or "").strip()

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

        query = build_query(brand, code)
        print(f"[{idx+1}/{total}] Searching: {query}")

        try:
            candidates = find_best_pdf_links(query)
        except Exception as e:
            failed += 1
            print(f"  FAIL search: {e}")
            continue

        downloaded = False
        for url in candidates:
            is_pdf = looks_like_pdf_url(url) or head_or_get_is_pdf(sess, url)
            if not is_pdf:
                continue

            print(f"  Trying: {url}")
            if download_pdf(sess, url, out_path):
                ok += 1
                downloaded = True
                print(f"  OK -> {out_path}")
                break

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
