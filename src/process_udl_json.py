import os, json
from pathlib import Path
from datetime import datetime
from typing import Iterator, Dict, List, Optional

import pandas as pd

# Optional streaming for big JSON arrays
try:
    import ijson  # type: ignore
    HAS_IJSON = True
except Exception:
    HAS_IJSON = False

import pyarrow as pa
import pyarrow.parquet as pq

RAW_SUBDIR = "00_raw"
PROC_SUBDIR = "01_processed"
DATASET_NAME = "elset_history_aodr"

# Keep ALL columns by default; or set a subset to trim size
COLUMNS_TO_KEEP: Optional[List[str]] = None

# Columns we will try to parse as datetimes
DATE_COLS = ["epoch", "createdAt", "effectiveFrom", "effectiveUntil"]

# Known numeric columns we coerce to numeric (floats) to avoid Decimal inference
NUMERIC_AS_FLOAT = [
    "agom","apogee","perigee","semiMajorAxis","period",
    "eccentricity","inclination","meanAnomaly","raan","argOfPerigee",
    "bStar","meanMotion","meanMotionDot","meanMotionDDot","ballisticCoeff"
]

# Nullable integer candidates
NUMERIC_AS_INT = ["satNo","revNo","idElset","idOnOrbit","idOrbitDetermination"]

# Columns that may be list/dict -> stringify
OBJECTY_TO_JSON = ["tags"]

def _to_text(x):
    # Convert anything (including bool/bytes) to a clean str, leave NaN/NA as-is
    if pd.isna(x):
        return pd.NA
    if isinstance(x, (list, dict)):
        return json.dumps(x)
    if isinstance(x, (bytes, bytearray)):
        try:
            return x.decode("utf-8", errors="replace")
        except Exception:
            return str(x)
    if isinstance(x, bool):
        return "true" if x else "false"
    return str(x)

# Columns we will *always* stringify (common metadata fields that can be inconsistent)
ALWAYS_STR_COLUMNS = [
    "uct", "classificationMarking", "origin", "source", "sourceDL",
    "descriptor", "createdBy", "transactionId", "tags"  # 'tags' also handled as list/dict above
]

def resolve_dirs() -> tuple[Path, Path, Path]:
    here = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd()
    data_dir = Path(os.getenv("DATA_DIR", here.parent / "data"))
    raw_dir = Path(os.getenv("RAW_DIR", data_dir / RAW_SUBDIR))
    proc_dir = Path(os.getenv("PROCESSED_DIR", data_dir / PROC_SUBDIR)) / DATASET_NAME
    for d in (data_dir, raw_dir, proc_dir):
        d.mkdir(parents=True, exist_ok=True)
    return data_dir, raw_dir, proc_dir

def detect_format(path: Path) -> str:
    with path.open("rb") as f:
        first = f.read(1)
    return "array" if first == b"[" else "ndjson"

def iter_records_ndjson(path: Path) -> Iterator[Dict]:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)

def iter_records_array(path: Path) -> Iterator[Dict]:
    if HAS_IJSON:
        with path.open("rb") as f:
            for obj in ijson.items(f, "item"):
                yield obj
    else:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        for obj in data:
            yield obj

def iter_records_any(path: Path) -> Iterator[Dict]:
    return iter_records_array(path) if detect_format(path) == "array" else iter_records_ndjson(path)

def to_chunks(iterable: Iterator[Dict], chunk_size: int = 50000) -> Iterator[List[Dict]]:
    buf: List[Dict] = []
    for rec in iterable:
        buf.append(rec)
        if len(buf) >= chunk_size:
            yield buf
            buf = []
    if buf:
        yield buf

def normalize_chunk(records: List[Dict]) -> pd.DataFrame:
    df = pd.json_normalize(records, max_level=1)

    # Keep subset if requested
    if COLUMNS_TO_KEEP:
        cols = [c for c in COLUMNS_TO_KEEP if c in df.columns]
        df = df.reindex(columns=cols)

    # Stringify list/dict columns we know about (and any dynamic leftovers)
    for col in OBJECTY_TO_JSON:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x)

    # Also auto-stringify any other columns pandas typed as "object" but containing lists/dicts
    for col in df.columns:
        if df[col].dtype == "object":
            # quick peek for non-scalars
            sample = df[col].dropna().head(10).tolist()
            if any(isinstance(x, (list, dict)) for x in sample):
                df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x)

    # Coerce numerics (floats)
    for col in NUMERIC_AS_FLOAT:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Coerce integers (nullable)
    for col in NUMERIC_AS_INT:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

        # --- Force known metadata columns to string (handles bool/bytes/mixed) ---
    for col in ALWAYS_STR_COLUMNS:
        if col in df.columns:
            df[col] = df[col].apply(_to_text).astype("string")

    # --- Also sanitize any remaining 'object' columns that look mixed (booleans/bytes) ---
    for col in df.columns:
        if df[col].dtype == "object" and col not in ALWAYS_STR_COLUMNS:
            sample = df[col].dropna().head(20).tolist()
            if any(isinstance(x, (bool, bytes, bytearray)) for x in sample):
                df[col] = df[col].apply(_to_text).astype("string")

    # Parse datetimes
    for c in DATE_COLS:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce", utc=True)

    # Partition column from epoch (day)
    if "epoch" in df.columns:
        df["epoch_date"] = df["epoch"].dt.date

    return df

def arrow_schema_for_df(df: pd.DataFrame) -> pa.Schema:
    fields = []
    for col in df.columns:
        s = df[col]
        if pd.api.types.is_datetime64_any_dtype(s.dtype):
            # UTC timestamp in microseconds
            t = pa.timestamp("us", tz="UTC")
        elif pd.api.types.is_integer_dtype(s.dtype):
            t = pa.int64()
        elif pd.api.types.is_float_dtype(s.dtype):
            t = pa.float64()
        elif pd.api.types.is_bool_dtype(s.dtype):
            t = pa.bool_()
        elif col == "epoch_date":
            # date partition column
            t = pa.date32()
        else:
            t = pa.string()
        fields.append(pa.field(col, t))
    return pa.schema(fields)

def dedupe_keys(df: pd.DataFrame) -> pd.DataFrame:
    keys = [c for c in ("satNo", "epoch", "idElset") if c in df.columns]
    if not keys:
        return df
    return df.sort_values(keys).drop_duplicates(subset=keys, keep="last")

def write_chunk(ds_dir: Path, df: pd.DataFrame):
    if df.empty:
        return
    # Ensure epoch_date is a date (not datetime64[ns, UTC] or object with datetime)
    if "epoch_date" in df.columns and pd.api.types.is_datetime64_any_dtype(df["epoch_date"].dtype):
        df["epoch_date"] = df["epoch_date"].dt.date

    schema = arrow_schema_for_df(df)
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)

    partition_cols = ["epoch_date"] if "epoch_date" in df.columns else []
    pq.write_to_dataset(
        table,
        root_path=str(ds_dir),
        partition_cols=partition_cols or None,
        existing_data_behavior="overwrite_or_ignore",
    )

def process_all_json(raw_dir: Path, proc_dir: Path):
    json_files = sorted(list(raw_dir.glob("*.json")))
    if not json_files:
        print(f"No JSON files found in {raw_dir}")
        return

    print(f"Found {len(json_files)} JSON files. Writing Parquet to: {proc_dir}")
    for i, fp in enumerate(json_files, start=1):
        print(f"[{i}/{len(json_files)}] {fp.name}")
        rec_iter = iter_records_any(fp)
        for j, chunk in enumerate(to_chunks(rec_iter, chunk_size=50000), start=1):
            df = normalize_chunk(chunk)
            if not df.empty:
                df = dedupe_keys(df)
                write_chunk(proc_dir, df)
                print(f"  - wrote chunk {j} ({len(df):,} rows)")
    print("Done.")

if __name__ == "__main__":
    _, raw_dir, proc_dir = resolve_dirs()
    process_all_json(raw_dir, proc_dir)
