from typing import Any, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator


class CheckResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str
    status: Literal["PASS", "WARN", "FAIL"]
    observed: dict[str, Any]
    threshold: dict[str, Any]
    details: dict[str, Any] = Field(default_factory=dict)
    checked_at: AwareDatetime


class Limit(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)

    warn: float = Field(ge=0)
    fail: float = Field(ge=0)

    @model_validator(mode="after")
    def ordered(self):
        if self.fail < self.warn:
            raise ValueError("fail must be at least warn")
        return self


class FreshnessLimit(Limit):
    future_tolerance: float = Field(default=60, ge=0)


class VolumeLimit(Limit):
    mad_floor: float = Field(gt=0)
    min_history: int = Field(ge=1)


class EmptyLimit(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Thresholds(BaseModel):
    model_config = ConfigDict(extra="forbid")

    freshness: FreshnessLimit
    lag: Limit
    cdc_lag: FreshnessLimit
    offset_gaps: Limit
    source_reconciliation: Limit
    volume: VolumeLimit
    orphans: Limit
    schema_drift: EmptyLimit
    dead_letters: Limit
    insert_health: Limit
    duplicates: Limit
    purchase_enrichment: Limit
