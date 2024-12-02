from enum import StrEnum


class AccountType(StrEnum):
    ASSET="asset"
    LIABILITY="liability"
    INCOME="income"
    EXPENSE="expense"
    
    @classmethod
    def from_value(cls, value:str):
        match value:
            case "asset":
                return cls.ASSET
            case "liability":
                return cls.LIABILITY
            case "income":
                return cls.INCOME
            case "expense":
                return cls.EXPENSE
            