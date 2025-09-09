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

# ====== Construir body JSON para /simulate (desde dag params) ======
def build_body(**kwargs):
    """
    Lee params del DAG (o usa defaults) y devuelve un JSON string.
    Params admitidos:
      rounds, num_decks, base_bet, strategy, seed, bet_mode
    """
    params = kwargs["params"] or {}
    payload = {
        "rounds":     int(params.get("rounds", 20)),
        "num_decks":  int(params.get("num_decks", 6)),
        "base_bet":   float(params.get("base_bet", 10.0)),
        "strategy":   str(params.get("strategy", "basic")),
        "seed":       params.get("seed", 42),
        "bet_mode":   str(params.get("bet_mode", "fixed")),  # "fixed" | "hi-lo"
    }
    print("REQUEST /simulate payload:", payload)
    return json.dumps(payload)

# ====== Guardar JSON y CSV ======
def download_files(**kwargs):
    ti = kwargs["ti"]
    data_text = ti.xcom_pull(task_ids="fetch")
    try:
        data = json.loads(data_text)
        if not isinstance(data, list):
            raise ValueError("La API no devolvió una lista de manos.")
    except Exception as e:
        raise ValueError(f"No pude parsear JSON de /simulate: {e}\nTexto: {data_text[:300]}...")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    run_id = kwargs.get("run_id") or kwargs["ds_nodash"]
    base = os.path.join(OUTPUT_DIR, f"{run_id}_simulate")

    # JSON
    json_path = f"{base}.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    # CSV: columnas según el response que definiste
    csv_path = f"{base}.csv"
    cols = [
        "round_id",
        "hand_number",
        "player_cards",
        "dealer_cards",
        "actions",
        "bet_amount",
        "final_result",
        "blackjack",
        "busted",
        "strategy_used",
        "bet_mode",
        "true_count_prev_round",
        "running_count_end",
        "true_count_end",
        "cards_remaining",
        "decks_remaining",
    ]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for row in data:
            # Listas -> string "A,7" / "Q,8" / "hit,stand"
            def join_if_list(v):
                if isinstance(v, list):
                    return ",".join(map(str, v))
                return v
            out = {k: join_if_list(row.get(k)) for k in cols}
            w.writerow(out)

    print(f"[blackjack] manos: {len(data)}")
    print(f"[blackjack] JSON: {json_path}")
    print(f"[blackjack] CSV : {csv_path}")
    return {"json_path": json_path, "csv_path": csv_path}


with DAG(
    dag_id="blackjack_pipeline",
    description="Blackjack: health -> fetch(simulate) -> download(JSON/CSV)",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["blackjack", "etl"],
    # Defaults que podés cambiar desde la UI en 'Trigger DAG with config'
    params={
        "rounds": 20,
        "num_decks": 6,
        "base_bet": 10.0,
        "strategy": "basic",
        "seed": 42,
        "bet_mode": "fixed",   # o "hi-lo"
    },
) as dag:

    health = PythonOperator(
        task_id="health",
        python_callable=health_check,
    )

    build = PythonOperator(
        task_id="build_body",
        python_callable=build_body,
    )

    # POST /simulate con body JSON desde XCom
    fetch = HttpOperator(
        task_id="fetch",
        http_conn_id=API_CONN_ID,
        endpoint="/simulate",
        method="POST",
        headers={"Content-Type": "application/json"},
        data="{{ ti.xcom_pull(task_ids='build_body') }}",
        log_response=True,
        response_filter=lambda r: r.text,  # guardamos texto; lo parsea download_files
        do_xcom_push=True,
    )

    download = PythonOperator(
        task_id="download",
        python_callable=download_files,
    )

    health >> build >> fetch >> download