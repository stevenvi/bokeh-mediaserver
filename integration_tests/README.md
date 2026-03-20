# Bokeh Integration Tests

Integration tests for the Bokeh media server. Tests run entirely inside Docker —
no host filesystem is touched.

## Prerequisites

- Docker with Compose v2
- The server image is built automatically on first run (~2 min)

## Running

### Full run (CI)

Builds images, starts the full stack, runs all tests, tears everything down.
Exit code reflects pass/fail.

    cd server/integration_tests
    docker compose -f docker-compose.integration.yml up --build --abort-on-container-exit
    docker compose -f docker-compose.integration.yml down -v

### Development iteration

Keep postgres and the server running between test runs for faster feedback.

    # Start the stack (once)
    docker compose -f docker-compose.integration.yml up -d --build postgres server --wait

    # Re-run tests as many times as needed
    docker compose -f docker-compose.integration.yml run --rm test-runner

    # Tear down when done
    docker compose -f docker-compose.integration.yml down -v

## Writing Tests

Add new `test_*.py` files alongside the existing ones. Each file gets a clean
DB slate before it runs (devices, collections, media, and jobs are truncated).
The seeded `admin` / `admin` user is always present.

See `test_auth.py` as the model for test structure and assertion style.

Helpers:
- `helpers/auth.py` — `bearer(token)`, `decode_jwt(token)`
- `helpers/poll.py` — `wait_for_job(base_url, token, job_id)` for async job tests
- `helpers/filesystem.py` — `variant_path(data_path, item_id, variant)` for image variant checks
