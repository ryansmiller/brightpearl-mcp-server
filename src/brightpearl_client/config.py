from pydantic_settings import BaseSettings, SettingsConfigDict


class BrightpearlConfig(BaseSettings):
    """Brightpearl connection settings, loaded from BRIGHTPEARL_* env vars / .env."""

    model_config = SettingsConfigDict(env_prefix="BRIGHTPEARL_", env_file=".env", extra="ignore")

    account: str
    datacenter: str = "use1"
    app_ref: str
    account_token: str

    # Account-wide budget is 200 req/min; everything (sweeps, webhook fetches,
    # live MCP calls) must share it through one rate limiter.
    requests_per_minute: int = 200
    # Fraction of the budget held back for priority calls (webhook fetches,
    # live MCP lookups). Background sweeps can't touch this headroom.
    reserve_fraction: float = 0.25

    timeout_seconds: float = 30.0
    max_retries: int = 5

    @property
    def base_url(self) -> str:
        return f"https://{self.datacenter}.brightpearlconnect.com/public-api/{self.account}"

    @property
    def auth_headers(self) -> dict[str, str]:
        return {
            "brightpearl-app-ref": self.app_ref,
            "brightpearl-account-token": self.account_token,
        }
