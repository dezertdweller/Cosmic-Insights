import os
import time
import zipfile
from pathlib import Path
from urllib.parse import urlparse
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv, find_dotenv

# ---------------- Env & Dirs ----------------
load_dotenv(find_dotenv())

def resolve_dirs():
    here = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd()
    data_dir = Path(os.getenv("DATA_DIR", here.parent / "data"))  # default: project_root/data
    raw_dir = Path(os.getenv("RAW_DIR", data_dir / "00_raw"))
    processed_dir = Path(os.getenv("PROCESSED_DIR", data_dir / "01_processed"))
    final_dir = Path(os.getenv("FINAL_DIR", data_dir / "02_final"))
    for d in (data_dir, raw_dir, processed_dir, final_dir):
        d.mkdir(parents=True, exist_ok=True)
    # expose for downstream tools this run
    os.environ.setdefault("DATA_DIR", str(data_dir))
    os.environ.setdefault("RAW_DIR", str(raw_dir))
    os.environ.setdefault("PROCESSED_DIR", str(processed_dir))
    os.environ.setdefault("FINAL_DIR", str(final_dir))
    return data_dir, raw_dir, processed_dir, final_dir

# ---------------- Auth ----------------
def token_header_basic():
    """
    UDL bulk links may be public (pre-signed) or require auth.
    If UDL_TOKEN is set, add 'Basic <token>' (works for UDL Dynamic Query tokens).
    """
    tok = (os.getenv("UDL_TOKEN") or "").strip()
    if tok:
        return {"Authorization": tok if tok.lower().startswith("basic ") else f"Basic {tok}"}
    # Fallback to username/password if present
    user, pwd = os.getenv("API_USERNAME"), os.getenv("API_PASSWORD")
    if user and pwd:
        import base64
        raw = f"{user}:{pwd}".encode("utf-8")
        return {"Authorization": "Basic " + base64.b64encode(raw).decode("ascii")}
    return {}

# ---------------- HTTP Session ----------------
def make_session():
    s = requests.Session()
    retries = Retry(
        total=6,
        backoff_factor=1.4,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s

# ---------------- Utilities ----------------
def safe_filename_from_url(url: str) -> str:
    name = os.path.basename(urlparse(url).path) or "download.zip"
    # Ensure .zip if no extension
    if "." not in name:
        name += ".zip"
    return name

def read_urls(urls_path: Path) -> list[str]:
    with urls_path.open("r", encoding="utf-8") as f:
        urls = [ln.strip() for ln in f if ln.strip() and not ln.strip().startswith("#")]
    # de-dup while preserving order
    seen, deduped = set(), []
    for u in urls:
        if u not in seen:
            deduped.append(u); seen.add(u)
    return deduped

def download_zip(session: requests.Session, url: str, headers: dict, out_zip: Path) -> None:
    # stream download
    with session.get(url, headers=headers, stream=True, timeout=300) as r:
        # If bulk links are public, auth header is harmless; if required, it helps
        if r.status_code == 401 or r.status_code == 403:
            # Retry once without auth in case it's a pre-signed public link
            with session.get(url, stream=True, timeout=300) as r2:
                r2.raise_for_status()
                with open(out_zip, "wb") as fh:
                    for chunk in r2.iter_content(chunk_size=1024 * 256):
                        if chunk:
                            fh.write(chunk)
            return
        r.raise_for_status()
        with open(out_zip, "wb") as fh:
            for chunk in r.iter_content(chunk_size=1024 * 256):
                if chunk:
                    fh.write(chunk)

def unzip_file(zpath: Path, dest: Path) -> None:
    with zipfile.ZipFile(zpath, "r") as zf:
        zf.extractall(dest)

# ---------------- Main ----------------
if __name__ == "__main__":
    _, RAW, _, _ = resolve_dirs()
    here = Path(__file__).resolve().parent
    # Default: urls.txt next to this script; override via BULK_URLS_FILE
    urls_file = Path(os.getenv("BULK_URLS_FILE", here / "urls.txt"))
    if not urls_file.exists():
        raise FileNotFoundError(f"urls.txt not found at {urls_file}. Put your links one per line.")

    urls = read_urls(urls_file)
    if not urls:
        raise ValueError(f"No URLs found in {urls_file}")

    print(f"Found {len(urls)} URLs in {urls_file}")
    s = make_session()
    headers = token_header_basic()

    keep_zip = True  # set to False if you want to delete zips after extracting
    min_interval_sec = 2.0  # gentle pacing between files

    for i, url in enumerate(urls, start=1):
        fname = safe_filename_from_url(url)
        zpath = RAW / f"{i:03d}_{fname}"
        print(f"[{i}/{len(urls)}] Downloading -> {zpath.name}")

        try:
            download_zip(s, url, headers, zpath)
            print(f"  ✓ Downloaded {zpath.stat().st_size:,} bytes")
        except requests.HTTPError as e:
            # Show server hints (e.g., Retry-After)
            print("  HTTP error:", e)
            ra = getattr(e.response, "headers", {}).get("Retry-After")
            if ra and ra.isdigit():
                sleep_s = int(ra)
                print(f"  Server asked to retry after {sleep_s}s. Sleeping…")
                time.sleep(sleep_s)
                # one retry
                download_zip(s, url, headers, zpath)
                print(f"  ✓ Downloaded after retry: {zpath.stat().st_size:,} bytes")
            else:
                raise

        # Unzip
        try:
            unzip_file(zpath, RAW)
            print(f"  ✓ Unzipped into {RAW}")
            if not keep_zip:
                zpath.unlink(missing_ok=True)
        except zipfile.BadZipFile:
            print("  !! Not a zip file or corrupted; leaving as-is.")

        time.sleep(min_interval_sec)

    print(f"All done. Files are in: {RAW}")
