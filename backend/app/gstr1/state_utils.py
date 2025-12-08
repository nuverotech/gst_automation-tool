from typing import Dict, Optional
from dataclasses import dataclass
import re


def normalize_label(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value).lower())


@dataclass
class StateInfo:
    code: str       # e.g. '27'
    name: str       # e.g. 'Maharashtra'


STATE_DATA = [
    ("JK", "01", "Jammu & Kashmir", ["jammu and kashmir", "jk"]),
    ("HP", "02", "Himachal Pradesh", ["himachal pradesh", "hp"]),
    ("PB", "03", "Punjab", ["pb"]),
    ("CH", "04", "Chandigarh", ["ch"]),
    ("UT", "05", "Uttarakhand", ["uttaranchal", "uk", "uttarakhand"]),
    ("HR", "06", "Haryana", ["hr"]),
    ("DL", "07", "Delhi", ["new delhi", "dl"]),
    ("RJ", "08", "Rajasthan", ["rajasthan", "rj"]),
    ("UP", "09", "Uttar Pradesh", ["uttar pradesh", "up"]),
    ("BR", "10", "Bihar", ["bihar", "br"]),
    ("SK", "11", "Sikkim", ["sikkim", "sk"]),
    ("AR", "12", "Arunachal Pradesh", ["arunachal pradesh", "ar"]),
    ("NL", "13", "Nagaland", ["nagaland", "nl"]),
    ("MN", "14", "Manipur", ["manipur", "mn"]),
    ("MZ", "15", "Mizoram", ["mizoram", "mz"]),
    ("TR", "16", "Tripura", ["tripura", "tr"]),
    ("ML", "17", "Meghalaya", ["meghalaya", "ml"]),
    ("AS", "18", "Assam", ["assam", "as"]),
    ("WB", "19", "West Bengal", ["west bengal", "wb"]),
    ("JH", "20", "Jharkhand", ["jharkhand", "jh"]),
    ("OD", "21", "Odisha", ["odisha", "orissa", "od"]),
    ("CG", "22", "Chhattisgarh", ["chhattisgarh", "cg"]),
    ("MP", "23", "Madhya Pradesh", ["madhya pradesh", "mp"]),
    ("GJ", "24", "Gujarat", ["gujarat", "gj"]),
    ("DD", "25", "Daman & Diu", ["daman and diu", "dd"]),
    ("DN", "26", "Dadra & Nagar Haveli and Daman & Diu", ["dadra and nagar haveli", "dnhdd", "dn"]),
    ("MH", "27", "Maharashtra", ["maharashtra", "mh"]),
    ("AP", "37", "Andhra Pradesh", ["andhra pradesh", "ap"]),
    ("KA", "29", "Karnataka", ["karnataka", "ka"]),
    ("GA", "30", "Goa", ["goa", "ga"]),
    ("LD", "31", "Lakshadweep", ["lakshadweep", "ld"]),
    ("KL", "32", "Kerala", ["kerala", "kl"]),
    ("TN", "33", "Tamil Nadu", ["tamil nadu", "tn"]),
    ("PY", "34", "Puducherry", ["puducherry", "pondicherry", "py"]),
    ("AN", "35", "Andaman & Nicobar Islands", ["andaman", "andaman & nicobar islands", "an"]),
    ("TS", "36", "Telangana", ["telangana", "ts"]),
    ("LA", "38", "Ladakh", ["ladakh", "la"]),
    ("OT", "97", "Other Territory", ["other territory", "ot"]),
]


STATE_DETAILS: Dict[str, StateInfo] = {
    code: StateInfo(code=numeric, name=name) for code, numeric, name, _ in STATE_DATA
}

STATE_NAME_TO_CODE: Dict[str, str] = {}
STATE_NUMERIC_TO_CODE: Dict[str, str] = {}

for code, numeric, name, aliases in STATE_DATA:
    STATE_NAME_TO_CODE[normalize_label(name)] = code
    STATE_NAME_TO_CODE[normalize_label(code)] = code
    STATE_NUMERIC_TO_CODE[numeric] = code
    for alias in aliases:
        STATE_NAME_TO_CODE[normalize_label(alias)] = code

# Legacy numeric for old AP
STATE_NUMERIC_TO_CODE.setdefault("28", "AP")


def state_code_from_value(value) -> Optional[str]:
    if value is None:
        return None
    candidate = str(value).strip()
    if not candidate:
        return None

    upper = candidate.upper()
    if upper in STATE_DETAILS:
        return upper

    normalized = normalize_label(candidate)
    if normalized in STATE_NAME_TO_CODE:
        return STATE_NAME_TO_CODE[normalized]

    # Try prefix digits (e.g. "27-Maharashtra")
    if "-" in candidate:
        prefix = candidate.split("-")[0]
        digits = "".join(ch for ch in prefix if ch.isdigit())
        if len(digits) == 2 and digits in STATE_NUMERIC_TO_CODE:
            return STATE_NUMERIC_TO_CODE[digits]

    digits = "".join(ch for ch in candidate if ch.isdigit())
    if len(digits) == 2 and digits in STATE_NUMERIC_TO_CODE:
        return STATE_NUMERIC_TO_CODE[digits]

    return None


def format_place_of_supply(state_code: Optional[str]) -> Optional[str]:
    if not state_code:
        return None
    detail = STATE_DETAILS.get(state_code)
    if not detail:
        return state_code
    return f"{detail.code}-{detail.name}"
