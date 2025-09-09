# 🚀 Cómo correrlo

Guía rápida para ejecutar la **API de Blackjack** en tu host y el **DAG** en **Astro/Airflow**, y guardar las jugadas en archivos JSON/CSV.

---

## ⚙️ Requisitos

* API levantada en tu máquina (Windows) con FastAPI/uvicorn.
* Proyecto Astro/Airflow en contenedores (`astro` CLI instalado).
* Conexión HTTP en Airflow llamada `blackjack_api`.

---

## 1) Levantar la API

```powershell
cd api
uvicorn api.main:app --reload --port 8000
```

* Docs interactivas: `http://127.0.0.1:8000/docs`
* Healthcheck: `http://127.0.0.1:8000/health`

> Asegurate de tener configurado `api/.env` con `BLACKJACK_REPO_DIR` apuntando al repo donde están `blackjack.py` y `Simulate_premade_strategy.py`.

---

## 2) Levantar Airflow (Astro)

```bash
astro dev start
```

---

## 3) Crear la conexión en Airflow

UI → **Admin → Connections → +**

| Campo           | Valor                  |
| --------------- | ---------------------- |
| Connection ID   | `blackjack_api`        |
| Connection Type | `HTTP`                 |
| Host            | `host.docker.internal` |
| Schema          | `http`                 |
| Port            | `8000`                 |
| Login/Password  | *(vacío)*              |
| Extra           | *(vacío)*              |

> Esta URL permite que el contenedor de Airflow llegue a tu API corriendo en Windows.

---

## 4) Ejecutar el DAG

1. En la UI de Airflow, **activá** el DAG `blackjack_pipeline`.
2. Hacé **Trigger** y completa con los datos de la request deseados. Ver proyecto API-Blackjack para mejor información.

---

## 5) Resultado

* Se generan archivos en tu proyecto Astro:

  ```
  include/exports/blackjack/<run_id>_jugadas.json
  include/exports/blackjack/<run_id>_jugadas.csv
  ```

---

