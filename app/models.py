from datetime import datetime
from typing import Literal, Optional, List

from pydantic import BaseModel, Field, condecimal


class PaymentRequest(BaseModel):
    provider_id: str = Field(..., description="Identifier of the payment provider")
    amount: condecimal(gt=0) = Field(..., description="Payment amount, must be > 0")


class PaymentResponse(BaseModel):
    provider_id: str
    amount: condecimal(gt=0)
    status: Literal["success", "failed", "rejected"]
    reason: Optional[str] = None
    circuit_state: Literal["closed", "open", "half-open"]


class ProviderStatus(BaseModel):
    provider_id: str
    is_available: bool
    last_checked_at: datetime


class PopularProvider(BaseModel):
    provider_id: str
    successful_payments: int


class PopularProvidersResponse(BaseModel):
    providers: List[PopularProvider]
