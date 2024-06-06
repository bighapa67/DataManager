from dataclasses import dataclass

@dataclass
class CEF_Dividends:
    Symbol: str
    xDivDate: str
    DivAmt: float