from pathlib import Path
import pandas as pd

# project root = two levels up from this notebook
PROC_DIR = Path.cwd().parents[1] / "data" / "01_processed" / "elset_history_aodr"
print(PROC_DIR)  # sanity check
day = "2025-01-01"
day_dir = PROC_DIR / f"epoch_date={day}"

# read the whole partition (all files in that directory)
df_day = pd.read_parquet(day_dir)
df_day.head()
# Load multiple days
days = ["2025-01-01", "2025-01-02"]
paths = [PROC_DIR / f"epoch_date={d}" for d in days]
df = pd.concat([pd.read_parquet(p) for p in paths], ignore_index=True)
df.head()