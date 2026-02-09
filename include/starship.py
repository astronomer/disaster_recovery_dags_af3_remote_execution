import logging
from datetime import datetime

import requests
from pydantic import BaseModel, TypeAdapter

logger = logging.getLogger(__name__)


class Base(BaseModel):
    pass


class Connection(Base):
    conn_id: str
    conn_type: str
    host: str | None
    port: int | None
    schema: str | None
    login: str | None
    password: str | None
    extra: dict | None
    description: str | None


class Dag(Base):
    dag_id: str
    is_paused: bool
    dag_run_count: int


class DagRun(Base):
    dag_id: str
    run_id: str
    queued_at: datetime | None = None
    logical_date: datetime | None = None
    run_after: datetime
    start_date: datetime | None = None
    end_date: datetime | None = None
    state: str | None = None
    run_type: str
    creating_job_id: int | None = None
    conf: dict | None = None
    data_interval_start: datetime | None = None
    data_interval_end: datetime | None = None
    last_scheduling_decision: datetime | None = None
    triggered_by: str | None = None
    created_dag_version_id: str | None = None
    backfill_id: int | None = None
    bundle_version: str | None = None


class TaskInstance(Base):
    id: str
    dag_id: str
    run_id: str
    task_id: str
    map_index: int
    try_number: int
    start_date: datetime | None = None
    end_date: datetime | None = None
    duration: float | None = None
    state: str | None = None
    max_tries: int | None = None
    hostname: str | None = None
    unixname: str | None = None
    pool: str | None = None
    pool_slots: int | None = None
    queue: str | None = None
    priority_weight: int | None = None
    operator: str | None = None
    custom_operator_name: str | None = None
    queued_dttm: datetime | None = None
    scheduled_dttm: datetime | None = None
    pid: int | None = None
    executor: str | None = None
    external_executor_id: str | None = None
    trigger_id: str | None = None
    trigger_timeout: datetime | None = None
    executor_config: str | None = None
    rendered_map_index: str | None = None
    task_display_name: str | None = None
    dag_version_id: str | None = None


class TaskInstancesResponse(Base):
    dag_run_count: int
    task_instances: list[TaskInstance]


class TaskInstancesBody(Base):
    task_instances: list[TaskInstance]


class TaskInstanceHistory(Base):
    task_instance_id: str
    dag_id: str
    run_id: str
    task_id: str
    map_index: int
    try_number: int
    start_date: datetime | None = None
    end_date: datetime | None = None
    duration: float | None = None
    state: str | None = None
    max_tries: int | None = None
    hostname: str | None = None
    unixname: str | None = None
    pool: str | None = None
    pool_slots: int | None = None
    queue: str | None = None
    priority_weight: int | None = None
    operator: str | None = None
    custom_operator_name: str | None = None
    queued_dttm: datetime | None = None
    scheduled_dttm: datetime | None = None
    pid: int | None = None
    executor: str | None = None
    external_executor_id: str | None = None
    trigger_id: str | None = None
    trigger_timeout: datetime | None = None
    executor_config: str | None = None
    rendered_map_index: str | None = None
    task_display_name: str | None = None
    dag_version_id: str | None = None


class TaskInstanceHistoryResponse(Base):
    dag_run_count: int
    task_instances: list[TaskInstanceHistory]


class TaskInstanceHistoryBody(Base):
    task_instances: list[TaskInstanceHistory]


class DagRunsResponse(Base):
    dag_runs: list[DagRun]
    dag_run_count: int


class DagRunsBody(Base):
    dag_runs: list[DagRun]


class Pool(Base):
    name: str
    slots: int
    description: str
    include_deferred: bool

    @property
    def is_default(self) -> bool:
        return self.name == "default_pool"


class Variable(Base):
    key: str
    val: str
    description: str | None


DagsResponse = TypeAdapter(list[Dag])
ConnectionsResponse = TypeAdapter(list[Connection])
PoolsResponse = TypeAdapter(list[Pool])
VariablesResponse = TypeAdapter(list[Variable])


class StarshipClient:
    def __init__(self, ui_url: str, api_key: str):
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {api_key}"})
        self.base_url = f"{ui_url}/api/starship"

    def _request(self, method: str, path: str, **kwargs) -> requests.Response:
        url = f"{self.base_url}{path}"
        try:
            response = self.session.request(method, url, **kwargs)

            response.raise_for_status()
            return response
        except requests.HTTPError as e:
            logger.error("HTTP error occurred: %s - %s", e.response.status_code, e.response.text)
            raise

    def get_dags(self) -> list[Dag]:
        response = self._request("GET", "/dags")
        return DagsResponse.validate_json(response.text)

    def set_dag_paused(self, dag_id: str, is_paused: bool) -> None:
        self._request("PATCH", "/dags", json={"dag_id": dag_id, "is_paused": is_paused})

    def get_pools(self) -> list[Pool]:
        response = self._request("GET", "/pools")
        return PoolsResponse.validate_json(response.text)

    def set_pool(self, pool: Pool) -> None:
        self._request("POST", "/pools", json=pool.model_dump())

    def delete_pool(self, pool_name: str) -> None:
        self._request("DELETE", "/pools", params={"name": pool_name})

    def get_connections(self) -> list[Connection]:
        response = self._request("GET", "/connections")
        return ConnectionsResponse.validate_json(response.text)

    def set_connection(self, connection: Connection) -> None:
        self._request("POST", "/connections", json=connection.model_dump())

    def delete_connection(self, conn_id: str) -> None:
        self._request("DELETE", "/connections", params={"conn_id": conn_id})

    def get_variables(self) -> list[Variable]:
        response = self._request("GET", "/variables")
        return VariablesResponse.validate_json(response.text)

    def set_variable(self, variable: Variable) -> None:
        self._request("POST", "/variables", json=variable.model_dump())

    def delete_variable(self, key: str) -> None:
        self._request("DELETE", "/variables", params={"key": key})

    def get_dag_runs(self, dag_id: str, *, limit: int = 50, offset: int = 0) -> list[DagRun]:
        response = self._request(
            "GET",
            "/dag_runs",
            params={"limit": limit, "offset": offset, "dag_id": dag_id},
        )
        dag_runs_response = DagRunsResponse.model_validate_json(response.text)
        return dag_runs_response.dag_runs

    def set_dag_runs(self, dag_runs: list[DagRun]) -> None:
        body = DagRunsBody(dag_runs=dag_runs)
        self._request(
            "POST",
            "/dag_runs",
            data=body.model_dump_json(),
            headers={"Content-Type": "application/json"},
        )

    def delete_dag_runs(self, dag_id: str) -> None:
        self._request("DELETE", "/dag_runs", params={"dag_id": dag_id})

    def get_task_instances(self, dag_id: str, *, limit: int = 50, offset: int = 0) -> TaskInstancesResponse:
        response = self._request(
            "GET",
            "/task_instances",
            params={"dag_id": dag_id, "limit": limit, "offset": offset},
        )
        return TaskInstancesResponse.model_validate_json(response.text)

    def set_task_instances(self, task_instances: list[TaskInstance]) -> None:
        body = TaskInstancesBody(task_instances=task_instances)
        self._request(
            "POST",
            "/task_instances",
            data=body.model_dump_json(),
            headers={"Content-Type": "application/json"},
        )

    def get_task_instance_history(
        self, dag_id: str, *, limit: int = 50, offset: int = 0
    ) -> TaskInstanceHistoryResponse:
        response = self._request(
            "GET",
            "/task_instance_history",
            params={"dag_id": dag_id, "limit": limit, "offset": offset},
        )
        return TaskInstanceHistoryResponse.model_validate_json(response.text)

    def set_task_instance_history(self, task_instances: list[TaskInstanceHistory]) -> None:
        body = TaskInstanceHistoryBody(task_instances=task_instances)
        self._request(
            "POST",
            "/task_instance_history",
            data=body.model_dump_json(),
            headers={"Content-Type": "application/json"},
        )
