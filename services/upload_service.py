from __future__ import annotations

from io import BytesIO
from typing import Dict, List

import pandas as pd


class UploadParsingError(RuntimeError):
    pass


def dataframe_preview(df: pd.DataFrame, limit: int = 50) -> Dict[str, object]:
    rows = df.fillna("").astype(str).head(limit).to_dict(orient="records")
    return {
        "columns": df.columns.tolist(),
        "rows": rows,
        "total_rows": len(df.index),
        "preview_count": len(rows),
    }


def parse_excel(file_bytes: bytes) -> Dict[str, object]:
    try:
        df = pd.read_excel(BytesIO(file_bytes), engine="openpyxl")
    except Exception as exc:  # pragma: no cover - defensive logging
        raise UploadParsingError("Unable to parse uploaded Excel file") from exc

    if df.empty:
        raise UploadParsingError("Sheet contains zero rows.")

    return dataframe_preview(df)

