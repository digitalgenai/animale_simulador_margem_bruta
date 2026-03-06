from __future__ import annotations

import numpy as np


def _br_number(val: float, decimals: int = 2) -> str:
    s = f"{float(val):,.{decimals}f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")


def fmt_real(val):
    try:
        if isinstance(val, (int, float)) and not np.isnan(val):
            return f"R$ {_br_number(val, 2)}"
    except Exception:
        pass
    return "-"


def fmt_perc(val):
    try:
        if isinstance(val, (int, float)) and not np.isnan(val):
            return f"{_br_number(val * 100, 2)}%"
    except Exception:
        pass
    return "-"


def fmt_media(val):
    try:
        val = float(val)
        return _br_number(val, 2) if not np.isnan(val) else "-"
    except Exception:
        return "-"


def fmt_qtd(val):
    try:
        val = float(val)
        return _br_number(val, 0) if not np.isnan(val) else "-"
    except Exception:
        return "-"


def fmt_str(val):
    return str(val) if val and str(val).strip() != "" and str(val).lower() != "nan" else "-"
