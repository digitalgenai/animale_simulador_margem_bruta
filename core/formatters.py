from __future__ import annotations

import numpy as np


def fmt_real(val):
    return f"R$ {val:,.2f}" if isinstance(val, (int, float)) and not np.isnan(val) else "-"


def fmt_perc(val):
    return f"{val:.1%}" if isinstance(val, (int, float)) and not np.isnan(val) else "-"


def fmt_media(val):
    try:
        val = float(val)
        return f"{val:,.2f}" if not np.isnan(val) else "-"
    except Exception:
        return "-"


def fmt_qtd(val):
    try:
        val = float(val)
        return f"{val:,.0f}" if not np.isnan(val) else "-"
    except Exception:
        return "-"


def fmt_str(val):
    return str(val) if val and str(val).strip() != "" and str(val).lower() != "nan" else "-"
