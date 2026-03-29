import sys
from typing import Any

import requests

BASE_URL = "http://127.0.0.1:8000"
TIMEOUT = 300

# Cambiar a True solo cuando se quiera probar POST /refresh
# Consultara a la EIA API y reescribira los parquet
RUN_REFRESH = True


def fail(message: str) -> None:
    print(f"FAIL - {message}")
    sys.exit(1)


def ok(message: str) -> None:
    print(f"OK   - {message}")


def assert_true(condition: bool, message: str) -> None:
    if not condition:
        fail(message)
    ok(message)


def get_json(response: requests.Response) -> Any:
    try:
        return response.json()
    except Exception:
        fail(f"No se pudo parsear JSON. Status={response.status_code} Body={response.text[:300]}")


def request_get(path: str, **params: Any) -> requests.Response:
    return requests.get(f"{BASE_URL}{path}", params=params, timeout=TIMEOUT)


def request_post(path: str) -> requests.Response:
    return requests.post(f"{BASE_URL}{path}", timeout=TIMEOUT)


def check_health() -> None:
    response = request_get("/health")
    payload = get_json(response)

    assert_true(response.status_code == 200, "/health responde 200")
    assert_true(payload.get("status") == "ok", "/health.status == 'ok'")
    assert_true("message" in payload, "/health incluye message")


def check_data_contract() -> dict[str, Any]:
    response = request_get("/data", limit=3)
    payload = get_json(response)

    assert_true(response.status_code == 200, "/data responde 200")
    assert_true(isinstance(payload, dict), "/data devuelve objeto JSON")
    assert_true("items" in payload, "/data incluye items")
    assert_true("pagination" in payload, "/data incluye pagination")
    assert_true(isinstance(payload["items"], list), "items es lista")
    assert_true(isinstance(payload["pagination"], dict), "pagination es objeto")
    assert_true(payload["pagination"].get("page") == 1, "pagination.page == 1")
    assert_true(payload["pagination"].get("limit") == 3, "pagination.limit == 3")

    items = payload["items"]
    if items:
        required_keys = {
            "outage_id",
            "plant_id",
            "plant_name",
            "period",
            "capacity_mw",
            "outage_mw",
            "percent_outage",
            "run_id",
            "ingested_at",
        }
        first = items[0]
        assert_true(required_keys.issubset(first.keys()), "cada item trae las columnas esperadas")

    return payload


def check_filter_by_plant_name(sample_payload: dict[str, Any]) -> None:
    items = sample_payload["items"]
    if not items:
        ok("se omite filtro por plant_name porque no hay datos")
        return

    sample_name = items[0]["plant_name"]
    needle = str(sample_name)[:3]

    response = request_get("/data", plant_name=needle, limit=10)
    payload = get_json(response)

    assert_true(response.status_code == 200, "/data con plant_name responde 200")

    for item in payload["items"]:
        plant_name = str(item["plant_name"]).lower()
        assert_true(needle.lower() in plant_name, f"plant_name contiene '{needle}'")


def check_filter_by_date(sample_payload: dict[str, Any]) -> None:
    items = sample_payload["items"]
    if not items:
        ok("se omite filtro por fecha porque no hay datos")
        return

    sample_period = items[0]["period"]

    response = request_get("/data", start_date=sample_period, end_date=sample_period, limit=20)
    payload = get_json(response)

    assert_true(response.status_code == 200, "/data con filtro de fecha responde 200")

    for item in payload["items"]:
        assert_true(item["period"] == sample_period, "todas las filas respetan el rango de fecha")


def check_sorting() -> None:
    response = request_get("/data", sort_by="outage_mw", sort_order="desc", limit=10)
    payload = get_json(response)

    assert_true(response.status_code == 200, "/data con sorting responde 200")

    values = [item["outage_mw"] for item in payload["items"]]
    assert_true(values == sorted(values, reverse=True), "outage_mw viene ordenado desc")


def check_pagination() -> None:
    page_1 = get_json(request_get("/data", page=1, limit=3))
    page_2 = get_json(request_get("/data", page=2, limit=3))

    ids_1 = {item["outage_id"] for item in page_1["items"]}
    ids_2 = {item["outage_id"] for item in page_2["items"]}

    # Si hay suficientes datos, idealmente no deben repetirse entre páginas.
    if ids_1 and ids_2:
        assert_true(ids_1.isdisjoint(ids_2), "page=1 y page=2 no repiten outage_id")


def check_bad_requests() -> None:
    bad_sort = request_get("/data", sort_by="hacker_column")
    bad_sort_payload = get_json(bad_sort)
    assert_true(bad_sort.status_code == 400, "sort_by inválido devuelve 400")
    assert_true("detail" in bad_sort_payload, "sort_by inválido incluye detail")

    bad_date = request_get("/data", start_date="no-es-fecha")
    bad_date_payload = get_json(bad_date)
    assert_true(bad_date.status_code == 400, "fecha inválida devuelve 400")
    assert_true("detail" in bad_date_payload, "fecha inválida incluye detail")

    bad_limit = request_get("/data", limit=101)
    # Esto normalmente lo rechaza FastAPI antes de entrar al service.
    assert_true(bad_limit.status_code == 422, "limit > 100 devuelve 422 por validación HTTP")


def check_refresh() -> None:
    response = request_post("/refresh")
    payload = get_json(response)

    assert_true(response.status_code == 200, "/refresh responde 200")
    assert_true(payload.get("status") in {"success", "warning"}, "/refresh devuelve status esperado")
    assert_true("message" in payload, "/refresh incluye message")

    if payload.get("status") == "success":
        run_id = payload.get("run_id")
        assert_true(bool(run_id), "/refresh success incluye run_id")

        data_after = get_json(request_get("/data", limit=3))
        items = data_after.get("items", [])

        if items:
            assert_true(
                all(item.get("run_id") == run_id for item in items),
                "/data ya está leyendo el nuevo run_id después del refresh",
            )


def main() -> None:
    print(f"Probando API en {BASE_URL}\n")

    check_health()
    sample_payload = check_data_contract()
    check_filter_by_plant_name(sample_payload)
    check_filter_by_date(sample_payload)
    check_sorting()
    check_pagination()
    check_bad_requests()

    if RUN_REFRESH:
        check_refresh()
    else:
        ok("refresh omitido (RUN_REFRESH = False)")

    print("\nTodo bien. Tu API pasó el smoke test básico.")


if __name__ == "__main__":
    main()