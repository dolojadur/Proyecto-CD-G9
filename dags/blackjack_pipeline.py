from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.hooks.http import HttpHook

from datetime import datetime, timedelta
import os, json, csv
from requests.exceptions import ConnectionError, HTTPError

# ===== Config =====
API_CONN_ID = "blackjack_api"   # creala en Admin > Connections
OUTPUT_DIR = "/usr/local/airflow/include/exports/blackjack"  # mapea a include/exports/blackjack

# Endpoint con Jinja, como en tu pokedag (templating en `endpoint`)
ENDPOINT_JUGADAS = (
    "/jugadas?"
    "match_id=run-{{ data_interval_start | ds_nodash }}"
    "&autogenerar=true"
    "&n_manos=100"
    "&limit=1000"
    "&num_decks=6"
    "&apuesta=50.0"
)

default_args = {
    "owner": "airflow",
    "start_date": datetime.today() - timedelta(days=1),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
    "depends_on_past": False,
}

# ====== Tarea health (como función con HttpHook) ======
def health_check(**kwargs):
    hook = HttpHook(method="GET", http_conn_id=API_CONN_ID)
    try:
        resp = hook.run(endpoint="/health", extra_options={"timeout": 10})
        resp.raise_for_status()
        print("HEALTH OK:", resp.json())
    except (ConnectionError, HTTPError) as e:
        raise RuntimeError(f"API /health falló: {e}")

# ====== Tarea download (guardar JSON y CSV) ======
def download_files(**kwargs):
    ti = kwargs["ti"]
    data_text = ti.xcom_pull(task_ids="fetch")  # lo empuja HttpOperator como texto
    try:
        data = json.loads(data_text)
    except Exception as e:
        raise ValueError(f"No pude parsear JSON de /jugadas: {e}\nTexto: {data_text[:300]}...")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    run_id = kwargs.get("run_id") or kwargs["ds_nodash"]
    base = os.path.join(OUTPUT_DIR, f"{run_id}_jugadas")

    # JSON
    json_path = f"{base}.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    # CSV
    csv_path = f"{base}.csv"
    cols = ["match_id", "strategy", "dealer_card", "player_card", "doubled", "won", "profit"]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for row in (data or []):
            w.writerow({k: row.get(k) for k in cols})

    total_profit = sum((row.get("profit") or 0) for row in data) if isinstance(data, list) else 0
    print(f"[blackjack] manos: {len(data) if isinstance(data, list) else 0} | profit total: {total_profit:.2f}")
    print(f"[blackjack] JSON: {json_path}")
    print(f"[blackjack] CSV : {csv_path}")
    return {"json_path": json_path, "csv_path": csv_path}

with DAG(
    dag_id="blackjack_pipeline",
    description="DAG estilo pokedag: health -> fetch -> download",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["blackjack", "etl"],
    params={"dag_id": "blackjack_pipeline"},
) as dag:

    health = PythonOperator(
        task_id="health",
        python_callable=health_check,
    )

    # Igual que en tu pokedag: HttpOperator con endpoint templatable, response_filter y XCom
    fetch = HttpOperator(
        task_id="fetch",
        http_conn_id=API_CONN_ID,
        endpoint=ENDPOINT_JUGADAS,       # querystring con Jinja
        method="GET",
        log_response=True,
        response_filter=lambda r: r.text,  # guardamos texto, lo parsea download_files
        do_xcom_push=True,
    )

    download = PythonOperator(
        task_id="download",
        python_callable=download_files,
    )

    health >> fetch >> download
