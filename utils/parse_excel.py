import os
import re
import inspect
import asyncio
from functools import lru_cache
from pathlib import Path
from typing import Iterable, Sequence

import pandas as pd

from utils.consts.greek_tems import floor_level_dict

try:
    from googletrans import Translator
except ImportError:  # pragma: no cover
    Translator = None

GREEK_CHAR_PATTERN = re.compile(r"[\u0370-\u03ff\u1f00-\u1fff]")
translator = Translator() if Translator is not None else None

GREEK_TRANSLIT_MAP = {
    "Α": "A",
    "Β": "V",
    "Γ": "G",
    "Δ": "D",
    "Ε": "E",
    "Ζ": "Z",
    "Η": "I",
    "Θ": "Th",
    "Ι": "I",
    "Κ": "K",
    "Λ": "L",
    "Μ": "M",
    "Ν": "N",
    "Ξ": "X",
    "Ο": "O",
    "Π": "P",
    "Ρ": "R",
    "Σ": "S",
    "Τ": "T",
    "Υ": "Y",
    "Φ": "F",
    "Χ": "Ch",
    "Ψ": "Ps",
    "Ω": "O",
    "ά": "a",
    "έ": "e",
    "ί": "i",
    "ή": "i",
    "ώ": "o",
    "ό": "o",
    "ύ": "y",
    "ϊ": "i",
    "ΐ": "i",
    "ϋ": "y",
    "ΰ": "y",
    "α": "a",
    "β": "v",
    "γ": "g",
    "δ": "d",
    "ε": "e",
    "ζ": "z",
    "η": "i",
    "θ": "th",
    "ι": "i",
    "κ": "k",
    "λ": "l",
    "μ": "m",
    "ν": "n",
    "ξ": "x",
    "ο": "o",
    "π": "p",
    "ρ": "r",
    "σ": "s",
    "ς": "s",
    "τ": "t",
    "υ": "y",
    "φ": "f",
    "χ": "ch",
    "ψ": "ps",
    "ω": "o",
}


def parse_excel_if_has_columns(
    excel_path: str, required_columns: Sequence[str]
) -> pd.DataFrame:
    """
    Load an Excel file and return a DataFrame **only if** all required columns exist.

    Parameters
    ----------
    excel_path : str
        Path to the Excel file.
    required_columns : Sequence[str]
        Iterable with the column names that must be present in the Excel file.

    Returns
    -------
    pd.DataFrame
        The loaded DataFrame when all required columns are present.

    Raises
    ------
    ValueError
        If one or more of the required columns are missing.
    """
    df = pd.read_excel(excel_path)

    required_set = set(required_columns)
    existing_set = set(map(str, df.columns))

    missing = required_set - existing_set
    if missing:
        raise ValueError(f"Missing required columns in '{excel_path}': {missing}")

    return df


def parse_level_column(df: pd.DataFrame, column: str = "level") -> pd.Series:
    """
    Parse a 'level' column using `floor_level_dict` from `greek_tems.py`.

    The function returns a new `pd.Series` with the mapped numeric levels
    (e.g. Υπόγειο → -1, Ισόγειο → 0, 1ος → 1, ...).

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame that contains the level column.
    column : str, default "level"
        Name of the column to parse.

    Returns
    -------
    pd.Series
        Series with parsed numeric levels. Values that are not found in
        `floor_level_dict` are returned as `NaN`.
    """

    if column not in df.columns:
        raise KeyError(f"Column '{column}' not found in DataFrame")

    # Normalize to string so we can match keys like '1ος', '0', etc.
    def _normalize(value):
        if pd.isna(value):
            return value
        return str(value).strip()

    return df[column].map(lambda v: floor_level_dict.get(_normalize(v)))


@lru_cache(maxsize=4096)
def translate_text(value: str) -> str:
    """
    Translate a single text value from Greek to English, mirroring the
    dashboard logic: transliteration first, then optional Google Translate.
    """
    if not isinstance(value, str):
        return value
    trimmed = value.strip()
    if not trimmed:
        return value

    transliterated = "".join(GREEK_TRANSLIT_MAP.get(ch, ch) for ch in trimmed)

    # Same behavior as in the dashboard: if we don't have a translator instance
    # or there are no Greek characters, just return the transliteration.
    if translator is None or not GREEK_CHAR_PATTERN.search(trimmed):
        return transliterated or trimmed

    try:
        result = translator.translate(trimmed, dest="en")
        # In some versions of googletrans this may be a coroutine; handle both.
        if inspect.isawaitable(result):
            try:
                result = asyncio.run(result)
            except RuntimeError:
                # If an event loop is already running, fall back to transliteration
                return transliterated or trimmed
        translated = getattr(result, "text", None)
        return translated or transliterated or trimmed
    except Exception:  # pragma: no cover - network/third-party errors
        return transliterated or trimmed


def translate_column_to_english(
    series: pd.Series,
) -> pd.Series:
    """
    Translate a pandas Series (column) from Greek to English using the same
    technique as the dashboard: transliteration + optional Google Translate.

    Parameters
    ----------
    series : pd.Series
        Input column with (Greek) or mixed values.

    Returns
    -------
    pd.Series
        A **new** Series with translated (English) labels.
    """

    return series.map(translate_text)


def translate_gr_columns_to_english(
    df: pd.DataFrame,
    columns: Sequence[str],
) -> pd.DataFrame:
    """
    For all columns in `columns` that end with 'GR', create new translated
    columns on the same DataFrame without the 'GR' suffix.

    Example:
        Input columns: ['MunicipalityGR', 'TypeGR']
        Output columns created: ['Municipality', 'Type']

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing the Greek columns.
    columns : Sequence[str]
        Column names to check; only those ending with 'GR' and present in
        `df.columns` will be processed.

    Returns
    -------
    pd.DataFrame
        The same DataFrame instance with the new English columns added.
    """
    for col in columns:
        if not col.endswith("GR"):
            continue
        if col not in df.columns:
            continue

        new_col = col[:-2]  # drop 'GR' suffix
        df[new_col] = translate_column_to_english(df[col])

    return df


def save_parsed_excel(df: pd.DataFrame, original_path: str) -> str:
    """
    Save DataFrame to a new Excel file, using the original filename with
    a 'parsed' suffix added before the extension.

    Example:
        original_path = 'C:/data/assets.xlsx'
        -> 'C:/data/assets_parsed.xlsx'

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to save.
    original_path : str
        Path of the original Excel file.

    Returns
    -------
    str
        The path of the newly saved Excel file.
    """
    base, ext = os.path.splitext(original_path)
    if not ext:
        # default to .xlsx if no extension is present
        ext = ".xlsx"
    new_path = f"{base}_parsed{ext}"

    df.to_excel(new_path, index=False)
    return new_path

# Backwards compatible simple name if existing code expects `parse_excel`
def parse_excel(excel_path: str, required_columns: Iterable[str] | None = None):
    """
    Convenience wrapper around `parse_excel_if_has_columns`.

    If `required_columns` is provided, it enforces their presence; otherwise
    it simply loads the Excel and returns the DataFrame.
    """
    if required_columns is None:
        return pd.read_excel(excel_path)
    return parse_excel_if_has_columns(excel_path, required_columns)


def main() -> None:
    """
    Load `excel_db/all_assets.xlsx`, translate all `*GR` columns and the
    `level` column, and save a new Excel file with a `_parsed` suffix.

    The original Excel file is **not** overwritten.
    """
    excel_path = Path(__file__).resolve().parents[1] / "excel_db" / "all_assets.xlsx"

    # Use our generic Excel loader
    df = parse_excel(str(excel_path))

    # Detect all columns that end with "GR"
    gr_columns = [
        col for col in df.columns if isinstance(col, str) and col.endswith("GR")
    ]

    if gr_columns:
        df = translate_gr_columns_to_english(df, gr_columns)

    # Parse the 'level' column if present
    if "level" in df.columns:
        df["level"] = parse_level_column(df, "level")

    new_path = save_parsed_excel(df, str(excel_path))
    print(f"Parsed Excel saved to: {new_path}")


if __name__ == "__main__":
    main()


