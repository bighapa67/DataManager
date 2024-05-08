from dataclasses import dataclass

@dataclass
class CEF_price_nav_history:
    Symbol: str
    MFQSSym: str
    Date: str
    OpenPx: float
    HighPx: float
    LowPx: float
    ClosePx: float
    NAV: float