from __future__ import annotations

import json
import os
import shutil
import signal
import socket
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

from airflow import DAG
from airflow.exceptions import AirflowException
try:
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.providers.standard.operators.python import BranchPythonOperator
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.empty import EmptyOperator
    from airflow.operators.python import BranchPythonOperator
    from airflow.operators.python import PythonOperator


DEFAULT_PROJECT_DIR = Path(__file__).resolve().parents[2]
if not (DEFAULT_PROJECT_DIR / "docker-compose.yml").exists():
    DEFAULT_PROJECT_DIR = Path("/home/gaurav/Downloads/PROJECT")

PROJECT_DIR = Path(os.environ.get("SMART_CITY_PROJECT_DIR", str(DEFAULT_PROJECT_DIR)))
RUNTIME_DIR = PROJECT_DIR / "runtime"
LOG_DIR = RUNTIME_DIR / "logs"
PID_DIR = RUNTIME_DIR / "pids"

ELASTICSEARCH_URL = os.environ.get("ELASTICSEARCH_URL", "http://localhost:9200")
API_URL = os.environ.get("SMART_CITY_API_URL", "http://localhost:8000")
KAFKA_HOST = os.environ.get("KAFKA_HOST", "localhost")
KAFKA_PORT = int(os.environ.get("KAFKA_PORT", "9092"))
PYTHON_BIN = os.environ.get("SMART_CITY_PYTHON", "/usr/bin/python3")
DEFAULT_SPARK_SUBMIT = Path("/home/gaurav/Downloads/0setup/spark-4.1.1-bin-hadoop3/bin/spark-submit")
SPARK_SUBMIT_BIN = os.environ.get(
    "SPARK_SUBMIT_BIN",
    str(DEFAULT_SPARK_SUBMIT) if DEFAULT_SPARK_SUBMIT.exists() else shutil.which("spark-submit") or "spark-submit",
)
UVICORN_BIN = os.environ.get("UVICORN_BIN", shutil.which("uvicorn") or "uvicorn")
SPARK_KAFKA_PACKAGE = os.environ.get(
    "SPARK_KAFKA_PACKAGE",
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1",
)
PIPELINE_PROCESS_NAMES = [
    "dashboard_api",
    "unified_consumer",
    "weather_producer",
    "traffic_producer",
    "aqi_producer",
]


def ensure_runtime_dirs() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    PID_DIR.mkdir(parents=True, exist_ok=True)


def requested_action(**context) -> str:
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    params = context.get("params") or {}
    action = str(conf.get("action", params.get("action", "start"))).strip().lower()

    if action in {"start", "run"}:
        return "start_elasticsearch_and_kibana"
    if action in {"stop", "stop_all"}:
        return "stop_pipeline"

    raise AirflowException("Unsupported action. Use action=start, action=stop, or action=stop_all.")


def http_json(method: str, url: str, payload: dict | None = None, timeout: int = 10) -> dict:
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    request = Request(url, data=data, headers=headers, method=method)
    with urlopen(request, timeout=timeout) as response:
        body = response.read()
    return json.loads(body.decode("utf-8")) if body else {}


def wait_for_elasticsearch() -> None:
    deadline = time.time() + 180
    last_error = None
    while time.time() < deadline:
        try:
            info = http_json("GET", ELASTICSEARCH_URL)
            if info.get("version"):
                return
        except (OSError, URLError) as exc:
            last_error = exc
        time.sleep(5)
    raise AirflowException(f"Elasticsearch did not become ready: {last_error}")


def install_elasticsearch_template() -> None:
    template_path = PROJECT_DIR / "elasticsearch_index_template.json"
    with template_path.open(encoding="utf-8") as template_file:
        template = json.load(template_file)

    http_json(
        "PUT",
        f"{ELASTICSEARCH_URL.rstrip('/')}/_index_template/smart-city-unified-template",
        template,
    )


def start_elasticsearch_and_kibana() -> str:
    if not (PROJECT_DIR / "docker-compose.yml").exists():
        raise AirflowException(
            f"Project directory is wrong or incomplete: {PROJECT_DIR}. "
            "Set SMART_CITY_PROJECT_DIR to the project folder."
        )

    try:
        info = http_json("GET", ELASTICSEARCH_URL, timeout=3)
        if info.get("version"):
            return f"Elasticsearch already reachable at {ELASTICSEARCH_URL}; skipping docker compose."
    except (OSError, URLError):
        pass

    command = ["docker", "compose", "-f", str(PROJECT_DIR / "docker-compose.yml"), "up", "-d"]
    try:
        completed = subprocess.run(
            command,
            cwd=PROJECT_DIR,
            check=True,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError as exc:
        raise AirflowException("Docker is not available to the Airflow process.") from exc
    except subprocess.CalledProcessError as exc:
        message = (exc.stderr or exc.stdout or str(exc)).strip()
        raise AirflowException(
            "Could not start Elasticsearch/Kibana with Docker. "
            f"Command failed: {message}"
        ) from exc

    return completed.stdout.strip() or "Docker Compose started Elasticsearch and Kibana."


def check_kafka() -> None:
    try:
        with socket.create_connection((KAFKA_HOST, KAFKA_PORT), timeout=10):
            return
    except OSError as exc:
        raise AirflowException(
            f"Kafka is not reachable at {KAFKA_HOST}:{KAFKA_PORT}. "
            "Start Kafka before triggering this DAG."
        ) from exc


def process_running(pid_file: Path) -> bool:
    if not pid_file.exists():
        return False

    try:
        pid = int(pid_file.read_text(encoding="utf-8").strip())
    except ValueError:
        return False

    try:
        os.kill(pid, 0)
    except OSError:
        pid_file.unlink(missing_ok=True)
        return False
    return True


def start_detached(name: str, command: list[str]) -> str:
    ensure_runtime_dirs()
    pid_file = PID_DIR / f"{name}.pid"
    log_file = LOG_DIR / f"{name}.log"

    if process_running(pid_file):
        return f"{name} already running with PID {pid_file.read_text(encoding='utf-8').strip()}"

    with log_file.open("ab") as log:
        process = subprocess.Popen(
            command,
            cwd=PROJECT_DIR,
            stdout=log,
            stderr=subprocess.STDOUT,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
            start_new_session=True,
        )

    pid_file.write_text(str(process.pid), encoding="utf-8")
    time.sleep(2)
    if process.poll() is not None:
        pid_file.unlink(missing_ok=True)
        recent_log = tail_text(log_file)
        raise AirflowException(
            f"{name} exited immediately with code {process.returncode}. "
            f"Recent log output:\n{recent_log}"
        )

    return f"Started {name} with PID {process.pid}; logs: {log_file}"


def tail_text(path: Path, max_lines: int = 40) -> str:
    if not path.exists():
        return "(log file was not created)"
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return "\n".join(lines[-max_lines:]) or "(log file is empty)"


def stop_process(name: str, timeout: int = 20) -> str:
    pid_file = PID_DIR / f"{name}.pid"
    if not pid_file.exists():
        return f"{name}: no PID file."

    try:
        pid = int(pid_file.read_text(encoding="utf-8").strip())
    except ValueError:
        pid_file.unlink(missing_ok=True)
        return f"{name}: removed invalid PID file."

    try:
        process_group = os.getpgid(pid)
    except OSError:
        pid_file.unlink(missing_ok=True)
        return f"{name}: process is not running; removed stale PID file."

    os.killpg(process_group, signal.SIGTERM)
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            os.kill(pid, 0)
        except OSError:
            pid_file.unlink(missing_ok=True)
            return f"{name}: stopped."
        time.sleep(1)

    os.killpg(process_group, signal.SIGKILL)
    pid_file.unlink(missing_ok=True)
    return f"{name}: forced stop after {timeout}s."


def stop_docker_services() -> str:
    command = ["docker", "compose", "-f", str(PROJECT_DIR / "docker-compose.yml"), "down"]
    try:
        completed = subprocess.run(
            command,
            cwd=PROJECT_DIR,
            check=True,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        return "Docker not available; skipped Elasticsearch/Kibana shutdown."
    except subprocess.CalledProcessError as exc:
        message = (exc.stderr or exc.stdout or str(exc)).strip()
        return f"Docker shutdown skipped/failed: {message}"

    return completed.stdout.strip() or "Elasticsearch and Kibana stopped."


def stop_pipeline(**context) -> str:
    ensure_runtime_dirs()
    messages = [stop_process(name) for name in PIPELINE_PROCESS_NAMES]

    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    params = context.get("params") or {}
    action = str(conf.get("action", params.get("action", ""))).strip().lower()
    if action == "stop_all":
        messages.append(stop_docker_services())

    return "\n".join(messages)


def start_python_process(script_name: str) -> str:
    return start_detached(script_name.removesuffix(".py"), [PYTHON_BIN, str(PROJECT_DIR / script_name)])


def start_unified_consumer() -> str:
    return start_detached(
        "unified_consumer",
        [
            SPARK_SUBMIT_BIN,
            "--master",
            "local[*]",
            "--packages",
            SPARK_KAFKA_PACKAGE,
            str(PROJECT_DIR / "unified_consumer.py"),
        ],
    )


def start_dashboard_api() -> str:
    return start_detached(
        "dashboard_api",
        [
            UVICORN_BIN,
            "dashboard_api:app",
            "--host",
            "0.0.0.0",
            "--port",
            "8000",
        ],
    )


def wait_for_dashboard_api() -> None:
    deadline = time.time() + 60
    last_error = None
    while time.time() < deadline:
        try:
            health = http_json("GET", f"{API_URL.rstrip('/')}/api/health")
            if health.get("status") == "ok":
                return
        except (OSError, URLError) as exc:
            last_error = exc
        time.sleep(3)
    raise AirflowException(f"Dashboard API did not become ready: {last_error}")


with DAG(
    dag_id="smart_city_pipeline",
    description="Start and verify the local Smart City streaming pipeline.",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={"action": "start"},
    tags=["smart-city", "streaming"],
) as dag:
    prepare_runtime = PythonOperator(
        task_id="prepare_runtime_dirs",
        python_callable=ensure_runtime_dirs,
    )

    choose_action = BranchPythonOperator(
        task_id="choose_action",
        python_callable=requested_action,
    )

    start_elasticsearch = PythonOperator(
        task_id="start_elasticsearch_and_kibana",
        python_callable=start_elasticsearch_and_kibana,
    )

    wait_elasticsearch = PythonOperator(
        task_id="wait_for_elasticsearch",
        python_callable=wait_for_elasticsearch,
    )

    install_template = PythonOperator(
        task_id="install_elasticsearch_template",
        python_callable=install_elasticsearch_template,
    )

    verify_kafka = PythonOperator(
        task_id="verify_kafka",
        python_callable=check_kafka,
    )

    start_aqi_producer = PythonOperator(
        task_id="start_aqi_producer",
        python_callable=start_python_process,
        op_args=["aqi_producer.py"],
    )

    start_traffic_producer = PythonOperator(
        task_id="start_traffic_producer",
        python_callable=start_python_process,
        op_args=["traffic_producer.py"],
    )

    start_weather_producer = PythonOperator(
        task_id="start_weather_producer",
        python_callable=start_python_process,
        op_args=["weather_producer.py"],
    )

    start_stream_processor = PythonOperator(
        task_id="start_unified_consumer",
        python_callable=start_unified_consumer,
    )

    start_api = PythonOperator(
        task_id="start_dashboard_api",
        python_callable=start_dashboard_api,
    )

    wait_api = PythonOperator(
        task_id="wait_for_dashboard_api",
        python_callable=wait_for_dashboard_api,
    )

    stop_pipeline_task = PythonOperator(
        task_id="stop_pipeline",
        python_callable=stop_pipeline,
    )

    stop_complete = EmptyOperator(task_id="stop_complete")

    prepare_runtime >> choose_action
    choose_action >> start_elasticsearch >> wait_elasticsearch >> install_template
    install_template >> verify_kafka
    verify_kafka >> [start_aqi_producer, start_traffic_producer, start_weather_producer]
    [start_aqi_producer, start_traffic_producer, start_weather_producer] >> start_stream_processor
    start_stream_processor >> start_api >> wait_api
    choose_action >> stop_pipeline_task >> stop_complete
