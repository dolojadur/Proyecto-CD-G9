## Como correrlo

Tener tu API corriendo en tu host (Windows):

uvicorn api.main:app --reload --port 8000

En el repo Astro:

astro dev start


En la UI de Airflow:

Confirmá la conexión blackjack_api (Host http://host.docker.internal:8000). En Airflow>Admin>Connections.
Connection ID: blackjack_api

Connection Type: HTTP

Host: host.docker.internal

Schema: http

Port: 8000

(Login/Password vacíos, Extra vacío)

Activá el DAG blackjack_pipeline.

Trigger.

Salida:

Logs del task download te muestran cantidad de manos y profit total.

Archivos en tu host: include/exports/blackjack/<run_id>_jugadas.json y .csv.