"""
Demo Payment Service with simulated latency and error issues.
This service generates traces in Jaeger to demonstrate TraceByte capabilities.
"""
import asyncio
import os
import random
import time
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import Status, StatusCode
from pydantic import BaseModel

# Configure OpenTelemetry
resource = Resource.create({"service.name": "payment-service"})
provider = TracerProvider(resource=resource)

otlp_exporter = OTLPSpanExporter(
    endpoint=os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://jaeger:4317"),
    insecure=True
)
provider.add_span_processor(BatchSpanProcessor(otlp_exporter))
trace.set_tracer_provider(provider)

tracer = trace.get_tracer(__name__)


class PaymentRequest(BaseModel):
    amount: float
    currency: str = "USD"
    customer_id: str
    order_id: str


class PaymentResponse(BaseModel):
    transaction_id: str
    status: str
    message: str
    processing_time_ms: float


# Simulated downstream services
DOWNSTREAM_SERVICES = {
    "fraud-detection": {"latency_base": 50, "error_rate": 0.05},
    "bank-gateway": {"latency_base": 100, "error_rate": 0.1},
    "inventory-check": {"latency_base": 30, "error_rate": 0.02},
}

# Config from GitOps repo - latency is calculated based on connection_pool_size
service_config = {
    "connection_pool_size": 50,  # Default - will be overridden by CONFIG_URL
    "config_hash": None,  # Track config changes
}

# Config reload settings
CONFIG_RELOAD_INTERVAL = int(os.getenv("CONFIG_RELOAD_INTERVAL", "30"))  # seconds


def calculate_latency_factor() -> float:
    """Calculate latency multiplier based on connection_pool_size.

    Lower pool size = higher latency (simulates pool exhaustion/contention)
    - pool_size >= 25: Normal (factor 1.0)
    - pool_size 10-24: Moderate latency (factor 2-5x)
    - pool_size < 10: Severe latency (factor 10-50x)
    """
    pool_size = service_config["connection_pool_size"]
    chaos = service_config.get("chaos", {})

    if chaos.get("error_code", False):
        # Calculate available connection slots after reserving for chaos load testing.
        # chaos.pool_limit caps usable connections; defaults to pool_size when not configured.
        chaos_pool_limit = chaos.get("pool_limit", pool_size)
        available_slots = pool_size - chaos_pool_limit  # BUG: zero when pool_limit unset (equals pool_size)
        fee_rate = 1.0 / available_slots  # ZeroDivisionError: calculate_latency: zero divisor in fee calculation
        return fee_rate

    if pool_size >= 25:
        return 1.0  # Normal operation
    elif pool_size >= 10:
        return 2.0 + (25 - pool_size) * 0.2  # 2-5x slower
    else:
        return 10.0 + (10 - pool_size) * 5.0  # 10-50x slower

async def fetch_config_from_github_api(repo: str, path: str, branch: str = "main", token: str = None) -> str | None:
    """Fetch file content from GitHub API (works with private repos).

    Args:
        repo: Repository in "owner/repo" format
        path: Path to file in repository
        branch: Branch name (default: main)
        token: GitHub token for authentication

    Returns:
        File content as string, or None if failed
    """
    import base64

    url = f"https://api.github.com/repos/{repo}/contents/{path}?ref={branch}"
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=headers, timeout=10.0)
            if response.status_code == 200:
                data = response.json()
                # GitHub API returns base64-encoded content
                content = base64.b64decode(data["content"]).decode("utf-8")
                return content
            else:
                print(f"⚠️  GitHub API error: {response.status_code} - {response.text[:200]}")
                return None
    except Exception as e:
        print(f"⚠️  GitHub API fetch error: {e}")
        return None


async def fetch_github_tokens() -> list[str]:
    """Get GitHub tokens for API access, in priority order.

    Returns a list of tokens to try (first that works wins):
    1. AUTOSRE_URL: Fetch a fresh installation token from AutoSRE API (auto-refreshing)
    2. GITHUB_TOKEN env var: Static PAT (works across orgs)
    """
    tokens = []

    autosre_url = os.getenv("AUTOSRE_URL")
    if autosre_url:
        try:
            # Pass repo so the API returns a token for the correct installation
            gitops_repo = os.getenv("GITOPS_REPO", "")
            params = {"repo": gitops_repo} if gitops_repo else {}
            async with httpx.AsyncClient() as client:
                response = await client.get(f"{autosre_url}/api/v1/github/token", params=params, timeout=5.0)
                if response.status_code == 200:
                    token = response.json().get("token")
                    if token:
                        tokens.append(token)
                else:
                    print(f"⚠️  TraceByte token API returned {response.status_code}")
        except Exception as e:
            print(f"⚠️  TraceByte token fetch failed: {e}")

    pat = os.getenv("GITHUB_TOKEN")
    if pat:
        tokens.append(pat)

    return tokens


# Keep backward-compatible function for any callers
async def fetch_github_token() -> str | None:
    """Get the best available GitHub token."""
    tokens = await fetch_github_tokens()
    return tokens[0] if tokens else None


def read_config_file(log_unchanged: bool = False) -> str | None:
    """Read config from mounted ConfigMap file (ArgoCD-managed).

    Returns file content if available, None otherwise.
    """
    config_file = os.getenv("CONFIG_FILE_PATH", "/config/payment-service.yaml")
    try:
        if os.path.isfile(config_file):
            with open(config_file) as f:
                content = f.read()
            if content.strip():
                if log_unchanged:
                    print(f"📋 Using ConfigMap file: {config_file}")
                return content
    except Exception as e:
        print(f"⚠️  ConfigMap file read error: {e}")
    return None


async def fetch_config(log_unchanged: bool = False) -> bool:
    """Fetch configuration from the best available source.

    Priority order:
    1. ConfigMap file mount (ArgoCD-managed, fastest)
    2. GITOPS_REPO + AUTOSRE_URL/GITHUB_TOKEN: GitHub API (works with private repos)
    3. CONFIG_URL: Raw URL (only works with public repos)
    4. Defaults

    Returns True if config changed, False otherwise.
    """
    global service_config

    content = None

    # Priority 1: ConfigMap file (ArgoCD-managed)
    content = read_config_file(log_unchanged=log_unchanged)

    # Priority 2: GitHub API (for private repos / fallback)
    if content is None:
        gitops_repo = os.getenv("GITOPS_REPO")
        gitops_path = os.getenv("GITOPS_PATH", "config/payment-service.yaml")
        gitops_branch = os.getenv("GITOPS_BRANCH", "main")
        github_tokens = await fetch_github_tokens()

        if gitops_repo and github_tokens:
            for token in github_tokens:
                content = await fetch_config_from_github_api(
                    repo=gitops_repo,
                    path=gitops_path,
                    branch=gitops_branch,
                    token=token,
                )
                if content:
                    if log_unchanged:
                        print(f"📋 Using GitHub API: {gitops_repo}/{gitops_path}")
                    break

    # Priority 3: Raw URL (public repos only)
    if content is None:
        config_url = os.getenv("CONFIG_URL")
        if not config_url:
            if log_unchanged:
                print("⚠️  No ConfigMap file, GITOPS_REPO, or CONFIG_URL available, using defaults")
            return False

        try:
            async with httpx.AsyncClient() as client:
                # Add cache-busting query param and headers to bypass GitHub CDN cache
                import time
                cache_bust_url = f"{config_url}?t={int(time.time())}"
                headers = {
                    "Cache-Control": "no-cache, no-store, must-revalidate",
                    "Pragma": "no-cache",
                }
                response = await client.get(cache_bust_url, timeout=10.0, headers=headers)
                if response.status_code == 200:
                    content = response.text
                    if log_unchanged:
                        print(f"📋 Using raw URL: {config_url}")
                else:
                    print(f"⚠️  Failed to fetch config: HTTP {response.status_code}")
                    return False
        except Exception as e:
            print(f"⚠️  Config fetch error: {e}")
            return False

    if content is None:
        return False

    try:
        import hashlib

        import yaml

        # Check if config actually changed
        config_hash = hashlib.md5(content.encode()).hexdigest()
        if config_hash == service_config.get("config_hash"):
            return False  # No change

        config = yaml.safe_load(content)
        old_pool_size = service_config["connection_pool_size"]
        pool_size = config.get("database", {}).get("connection_pool_size", 50)
        service_config["connection_pool_size"] = pool_size
        service_config["chaos"] = config.get("chaos", {})
        service_config["config_hash"] = config_hash

        latency_factor = calculate_latency_factor()
        if pool_size != old_pool_size:
            print(f"🔄 Config changed: connection_pool_size {old_pool_size} → {pool_size}")

        if pool_size < 10:
            print(f"⚠️  Config loaded: connection_pool_size={pool_size} (LOW - {latency_factor:.0f}x latency)")
        elif pool_size < 25:
            print(f"⚠️  Config loaded: connection_pool_size={pool_size} (MODERATE - {latency_factor:.1f}x latency)")
        else:
            print(f"✅ Config loaded: connection_pool_size={pool_size} (healthy)")
        return True
    except Exception as e:
        print(f"⚠️  Config parse error: {e}")
        return False


async def config_file_watcher():
    """Watch ConfigMap file for changes.

    Kubelet updates ConfigMap volumes every ~60s after the ConfigMap changes.
    We poll the file's mtime every 5s to detect updates quickly.
    Falls back to GitHub API polling if no ConfigMap file is mounted.
    """
    config_file = os.getenv("CONFIG_FILE_PATH", "/config/payment-service.yaml")

    if not os.path.isfile(config_file):
        print(f"📁 No ConfigMap file at {config_file}, using GitHub API polling")
        # Fall back to periodic GitHub API polling
        await config_reload_loop()
        return

    print(f"👁️  Watching ConfigMap file: {config_file} (poll every 5s)")
    last_mtime = os.path.getmtime(config_file)

    while True:
        await asyncio.sleep(5)
        try:
            current_mtime = os.path.getmtime(config_file)
            if current_mtime != last_mtime:
                last_mtime = current_mtime
                changed = await fetch_config(log_unchanged=True)
                if changed:
                    print("✅ Config reloaded from ConfigMap file change")
        except FileNotFoundError:
            # File might temporarily disappear during kubelet symlink swap
            await asyncio.sleep(1)
        except Exception as e:
            print(f"⚠️  File watcher error: {e}")


async def config_reload_loop():
    """Background task to periodically reload config from GitOps API."""
    print(f"🔄 Config API polling enabled (every {CONFIG_RELOAD_INTERVAL}s)")
    while True:
        await asyncio.sleep(CONFIG_RELOAD_INTERVAL)
        try:
            changed = await fetch_config(log_unchanged=False)
            if changed:
                print("✅ Config auto-reloaded from GitOps API")
        except Exception as e:
            print(f"⚠️  Config reload error: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    print("🚀 Payment Service starting...")
    await fetch_config(log_unchanged=True)
    pool_size = service_config["connection_pool_size"]
    if pool_size < 25:
        print(f"⚠️  Low pool size ({pool_size}) - expect increased latency")
    else:
        print("✅ Normal mode - healthy operation")

    # Start config watcher — uses ConfigMap file if mounted, else falls back to API polling
    reload_task = asyncio.create_task(config_file_watcher())

    yield

    # Cancel the reload task on shutdown
    reload_task.cancel()
    try:
        await reload_task
    except asyncio.CancelledError:
        pass
    print("👋 Payment Service shutting down...")


app = FastAPI(
    title="Demo Payment Service",
    description="A service with simulated latency issues for TraceByte demos",
    version="1.0.0",
    lifespan=lifespan,
)

FastAPIInstrumentor.instrument_app(app)


async def simulate_database_query(operation: str) -> dict:
    """Simulate a database query with latency based on connection_pool_size.

    Latency increases as pool_size decreases (simulates pool contention).
    """
    with tracer.start_as_current_span("db.query") as span:
        span.set_attribute("db.system", "postgresql")
        span.set_attribute("db.operation", operation)

        pool_size = service_config["connection_pool_size"]
        latency_factor = calculate_latency_factor()

        # Base delay: 10-50ms, scaled by latency factor
        base_delay = random.uniform(0.01, 0.05)
        delay = base_delay * latency_factor

        span.set_attribute("db.connection_pool_size", pool_size)
        span.set_attribute("db.latency_factor", latency_factor)
        span.set_attribute("db.query_time_ms", delay * 1000)

        if pool_size < 10:
            span.set_attribute("db.slow_query", True)
            span.add_event("Slow query - low pool size", {
                "pool_size": pool_size,
                "delay_seconds": delay,
                "latency_factor": latency_factor
            })

        await asyncio.sleep(delay)

        # Pool exhaustion more likely with low pool size
        exhaustion_chance = max(0, (10 - pool_size) * 0.01)  # 0-10% based on pool size
        if random.random() < exhaustion_chance:
            if pool_size == 0:
                span.set_status(Status(StatusCode.ERROR, "Connection pool exhausted"))
                span.record_exception(Exception("Connection pool exhausted"))
                raise HTTPException(status_code=503, detail="Database connection pool exhausted")
            else:
                span.set_status(Status(StatusCode.ERROR, "Connection pool exhausted"))
                span.record_exception(Exception("Connection pool exhausted"))
                raise HTTPException(status_code=503, detail="Database connection pool exhausted")

        return {"operation": operation, "duration_ms": delay * 1000, "pool_size": pool_size}


async def call_downstream_service(service_name: str, payload: dict) -> dict:
    """Simulate calling a downstream service with latency based on connection_pool_size.

    Latency increases as pool_size decreases (simulates upstream bottleneck).
    """
    config = DOWNSTREAM_SERVICES.get(service_name, {"latency_base": 50, "error_rate": 0.05})

    with tracer.start_as_current_span(f"http.client.{service_name}") as span:
        span.set_attribute("http.method", "POST")
        span.set_attribute("http.url", f"http://{service_name}.internal/api/v1/process")
        span.set_attribute("peer.service", service_name)

        pool_size = service_config["connection_pool_size"]
        latency_factor = calculate_latency_factor()

        # Base latency from config, scaled by latency factor
        base_latency = config["latency_base"] / 1000 + random.uniform(0, 0.1)
        latency = base_latency * latency_factor

        span.set_attribute("connection_pool_size", pool_size)
        span.set_attribute("latency_factor", latency_factor)
        span.set_attribute("http.response_time_ms", latency * 1000)

        if pool_size < 10:
            span.set_attribute("latency.spike", True)
            span.add_event("Latency spike - low pool size", {
                "pool_size": pool_size,
                "latency_seconds": latency,
                "latency_factor": latency_factor
            })

        await asyncio.sleep(latency)

        # Error rate kept very low - we want slow traces, not failed traces
        # Only ~5% total failure rate across all downstream calls
        error_rate = config["error_rate"] * 0.5  # Halve the base error rate
        if random.random() < min(error_rate, 0.03):  # Cap at 3% per service
            error_codes = [500, 502, 503, 504]
            error_code = random.choice(error_codes)
            span.set_attribute("http.status_code", error_code)
            span.set_status(Status(StatusCode.ERROR, f"{service_name} returned {error_code}"))
            span.record_exception(Exception(f"{service_name} failed with {error_code}"))
            raise HTTPException(
                status_code=502,
                detail=f"Downstream service {service_name} failed with status {error_code}"
            )

        span.set_attribute("http.status_code", 200)
        return {"service": service_name, "status": "success", "latency_ms": latency * 1000}


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "healthy", "service": "payment-service"}


@app.get("/ready")
async def ready():
    """Readiness check - might fail if DB is slow."""
    try:
        await simulate_database_query("SELECT 1")
        return {"status": "ready"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e)) from e


@app.post("/api/v1/payments", response_model=PaymentResponse)
async def process_payment(request: PaymentRequest):
    """
    Process a payment - this endpoint has intentional latency issues.

    The payment flow:
    1. Validate customer (DB query)
    2. Check fraud detection (downstream service)
    3. Verify inventory (downstream service)
    4. Process with bank gateway (downstream service)
    5. Record transaction (DB query)
    """
    start_time = time.time()

    with tracer.start_as_current_span("payment.process") as span:
        span.set_attribute("payment.amount", request.amount)
        span.set_attribute("payment.currency", request.currency)
        span.set_attribute("payment.customer_id", request.customer_id)
        span.set_attribute("payment.order_id", request.order_id)

        try:
            # Step 1: Validate customer
            span.add_event("Starting customer validation")
            await simulate_database_query("SELECT * FROM customers WHERE id = ?")

            # Step 2: Fraud detection
            span.add_event("Calling fraud detection service")
            await call_downstream_service("fraud-detection", {"amount": request.amount})

            # Step 3: Inventory check
            span.add_event("Checking inventory")
            await call_downstream_service("inventory-check", {"order_id": request.order_id})

            # Step 4: Bank gateway
            span.add_event("Processing with bank gateway")
            await call_downstream_service("bank-gateway", {
                "amount": request.amount,
                "currency": request.currency
            })

            # Step 5: Record transaction
            span.add_event("Recording transaction")
            await simulate_database_query("INSERT INTO transactions ...")

            processing_time = (time.time() - start_time) * 1000
            span.set_attribute("payment.processing_time_ms", processing_time)

            # Flag slow transactions
            if processing_time > 2000:
                span.add_event("Slow transaction detected", {
                    "processing_time_ms": processing_time,
                    "threshold_ms": 2000
                })
                span.set_attribute("payment.slow", True)

            return PaymentResponse(
                transaction_id=f"txn_{request.order_id}_{int(time.time())}",
                status="success",
                message="Payment processed successfully",
                processing_time_ms=processing_time
            )

        except HTTPException:
            span.set_status(Status(StatusCode.ERROR, "Payment processing failed"))
            raise
        except Exception as e:
            span.set_status(Status(StatusCode.ERROR, str(e)))
            span.record_exception(e)
            raise HTTPException(status_code=500, detail=str(e)) from e


@app.post("/api/v1/config/reload")
async def reload_config():
    """Reload configuration from GitOps repo."""
    await fetch_config()
    pool_size = service_config["connection_pool_size"]
    latency_factor = calculate_latency_factor()
    return {
        "connection_pool_size": pool_size,
        "latency_factor": latency_factor,
        "status": "low" if pool_size < 10 else "moderate" if pool_size < 25 else "healthy"
    }


@app.get("/api/v1/config/status")
async def config_status():
    """Get current configuration and latency status."""
    pool_size = service_config["connection_pool_size"]
    latency_factor = calculate_latency_factor()
    return {
        "connection_pool_size": pool_size,
        "latency_factor": latency_factor,
        "status": "low" if pool_size < 10 else "moderate" if pool_size < 25 else "healthy",
        "expected_db_delay_ms": 30 * latency_factor,  # ~30ms base * factor
        "expected_downstream_delay_ms": 100 * latency_factor  # ~100ms base * factor
    }


# Generate continuous load endpoint
@app.post("/api/v1/simulate/load")
async def simulate_load(count: int = 10, delay_between: float = 0.5):
    """Generate simulated load with multiple payment requests."""
    results = {"success": 0, "failed": 0, "errors": []}

    for i in range(count):
        try:
            request = PaymentRequest(
                amount=random.uniform(10, 1000),
                currency=random.choice(["USD", "EUR", "GBP"]),
                customer_id=f"cust_{random.randint(1000, 9999)}",
                order_id=f"order_{i}_{int(time.time())}"
            )
            await process_payment(request)
            results["success"] += 1
        except Exception as e:
            results["failed"] += 1
            results["errors"].append(str(e))

        await asyncio.sleep(delay_between)

    return results


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
