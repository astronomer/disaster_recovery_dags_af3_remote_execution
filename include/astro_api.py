from __future__ import annotations

import logging
from datetime import datetime
from typing import Literal, Self

import requests
from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel

logger = logging.getLogger(__name__)

BASE_URL = "https://api.astronomer.io/v1"


class Base(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)


# Remote Execution Models
class RemoteExecution(Base):
    enabled: bool
    allowed_ip_address_ranges: list[str] | None = None


# Subject Profile Models
class BasicSubjectProfile(Base):
    id: str
    subject_type: Literal["USER", "SERVICEKEY"] | None = None
    api_token_name: str | None = None
    avatar_url: str | None = None
    full_name: str | None = None
    username: str | None = None


# Environment Variable Models
class DeploymentEnvironmentVariable(Base):
    key: str
    is_secret: bool
    updated_at: datetime
    value: str | None = None

    def update(self) -> DeploymentEnvironmentVariableRequest:
        return DeploymentEnvironmentVariableRequest(
            key=self.key,
            is_secret=self.is_secret,
            value=self.value,
        )


class DeploymentEnvironmentVariableRequest(Base):
    key: str
    is_secret: bool
    value: str | None = None


# Legacy alias for backward compatibility
class EnvironmentVariable(Base):
    name: str
    value: str
    is_secret: bool


# Hibernation Models
class DeploymentHibernationSchedule(Base):
    hibernate_at_cron: str
    is_enabled: bool
    wake_at_cron: str
    description: str | None = None


class DeploymentHibernationOverride(Base):
    is_active: bool | None = None
    is_hibernating: bool
    override_until: datetime | None = None

    def update(self) -> DeploymentHibernationOverrideRequest:
        return DeploymentHibernationOverrideRequest(
            is_hibernating=self.is_hibernating,
            override_until=self.override_until,
        )


class DeploymentHibernationOverrideRequest(Base):
    is_hibernating: bool | None = None
    override_until: datetime | None = None


class DeploymentHibernationSpec(Base):
    override: DeploymentHibernationOverride | None = None
    schedules: list[DeploymentHibernationSchedule] | None = None

    def update(self) -> DeploymentHibernationSpecRequest:
        return DeploymentHibernationSpecRequest(
            override=self.override.update() if self.override else None,
            schedules=self.schedules,
        )


class DeploymentHibernationSpecRequest(Base):
    override: DeploymentHibernationOverrideRequest | None = None
    schedules: list[DeploymentHibernationSchedule] | None = None


class DeploymentHibernationStatus(Base):
    is_hibernating: bool
    next_event_at: str | None = None
    next_event_type: Literal["HIBERNATE", "WAKE"] | None = None
    reason: str | None = None


# Scaling Models
class DeploymentScalingSpec(Base):
    hibernation_spec: DeploymentHibernationSpec | None = None

    def update(self) -> DeploymentScalingSpecRequest:
        return DeploymentScalingSpecRequest(
            hibernation_spec=self.hibernation_spec.update() if self.hibernation_spec else None,
        )


class DeploymentScalingSpecRequest(Base):
    hibernation_spec: DeploymentHibernationSpecRequest | None = None


class DeploymentScalingStatus(Base):
    hibernation_status: DeploymentHibernationStatus | None = None


class WorkerQueue(Base):
    id: str
    is_default: bool
    max_worker_count: int
    min_worker_count: int
    name: str
    worker_concurrency: int
    astro_machine: Literal["A5", "A10", "A20", "A40", "A60", "A120", "A160"]

    def update(self) -> WorkerQueueRequest:
        return WorkerQueueRequest(
            id=self.id,
            is_default=self.is_default,
            max_worker_count=self.max_worker_count,
            min_worker_count=self.min_worker_count,
            name=self.name,
            worker_concurrency=self.worker_concurrency,
            astro_machine=self.astro_machine,
        )


# Worker Queue Models
class WorkerQueueRequest(Base):
    astro_machine: Literal["A5", "A10", "A20", "A40", "A60", "A120", "A160"]
    is_default: bool
    max_worker_count: int
    min_worker_count: int
    name: str
    worker_concurrency: int
    id: str | None = None


# Main Deployment Model
class Deployment(Base):
    airflow_version: str
    api_url: str
    id: str
    image_repository: str
    is_development_mode: bool
    image_tag: str
    name: str
    status: Literal["CREATING", "DEPLOYING", "HEALTHY", "UNHEALTHY", "UNKNOWN", "HIBERNATING"]
    web_server_ingress_hostname: str
    environment_variables: list[DeploymentEnvironmentVariable]
    astro_runtime_version: str | None = None
    cloud_provider: Literal["AWS", "AZURE", "GCP"] | None = None
    cluster_id: str | None = None
    cluster_name: str | None = None
    contact_emails: list[str] | None = None
    dag_tarball_version: str | None = None
    default_task_pod_cpu: str | None = None
    default_task_pod_memory: str | None = None
    description: str | None = None
    desired_dag_tarball_version: str | None = None
    dr_external_ips: list[str] | None = None
    dr_oidc_issuer_url: str | None = None
    effective_dr_workload_identity: str | None = None
    effective_workload_identity: str | None = None
    executor: Literal["CELERY", "KUBERNETES", "ASTRO"]
    external_ips: list[str] | None = None
    image_version: str | None = None
    is_cicd_enforced: bool
    is_dag_deploy_enabled: bool
    is_high_availability: bool
    namespace: str | None = None
    oidc_issuer_url: str | None = None
    organization_id: str | None = None
    region: str | None = None
    remote_execution: RemoteExecution | None = None
    resource_quota_cpu: str | None = None
    resource_quota_memory: str | None = None
    runtime_version: str | None = None
    scaling_spec: DeploymentScalingSpec | None = None
    scaling_status: DeploymentScalingStatus | None = None
    scheduler_au: int | None = None
    scheduler_cpu: str | None = None
    scheduler_memory: str | None = None
    scheduler_replicas: int | None = None
    scheduler_size: Literal["SMALL", "MEDIUM", "LARGE", "EXTRA_LARGE"]
    status_reason: str | None = None
    task_pod_node_pool_id: str | None = None
    type: Literal["DEDICATED", "HYBRID", "STANDARD"]
    ui_url: str
    updated_at: datetime | None = None
    updated_by: BasicSubjectProfile | None = None
    web_server_airflow_api_url: str | None = None
    web_server_cpu: str | None = None
    web_server_memory: str | None = None
    web_server_replicas: int | None = None
    web_server_url: str | None = None
    workspace_id: str
    workload_identity: str | None = None
    worker_queues: list[WorkerQueue] | None = None

    def update(self) -> UpdateDeploymentRequest:
        env_vars = [
            DeploymentEnvironmentVariableRequest(
                key=env_var.key,
                is_secret=env_var.is_secret,
                value=env_var.value,
            )
            for env_var in self.environment_variables
        ]

        return UpdateDeploymentRequest(
            environment_variables=env_vars,
            executor=self.executor,
            is_cicd_enforced=self.is_cicd_enforced,
            is_dag_deploy_enabled=self.is_dag_deploy_enabled,
            is_high_availability=self.is_high_availability,
            name=self.name,
            scheduler_size=self.scheduler_size,
            type=self.type,
            workspace_id=self.workspace_id,
            contact_emails=self.contact_emails,
            default_task_pod_cpu=self.default_task_pod_cpu,
            default_task_pod_memory=self.default_task_pod_memory,
            description=self.description,
            dr_workload_identity=self.effective_dr_workload_identity,
            is_development_mode=self.is_development_mode,
            remote_execution=self.remote_execution,
            resource_quota_cpu=self.resource_quota_cpu,
            resource_quota_memory=self.resource_quota_memory,
            scaling_spec=self.scaling_spec.update() if self.scaling_spec else None,
            worker_queues=[wq.update() for wq in self.worker_queues] if self.worker_queues else None,
            workload_identity=self.workload_identity,
        )

    def get_env_var(self, key: str) -> DeploymentEnvironmentVariable | None:
        return next((ev for ev in self.environment_variables if ev.key == key), None)

    def is_hibernating(self) -> bool:
        if self.scaling_status and self.scaling_status.hibernation_status:
            return self.scaling_status.hibernation_status.is_hibernating
        return False


# Update Deployment Request Models
class UpdateDeploymentRequest(Base):
    environment_variables: list[DeploymentEnvironmentVariableRequest]
    executor: Literal["CELERY", "KUBERNETES", "ASTRO"]
    is_cicd_enforced: bool
    is_dag_deploy_enabled: bool
    is_high_availability: bool
    name: str
    scheduler_size: Literal["SMALL", "MEDIUM", "LARGE", "EXTRA_LARGE"]
    type: Literal["DEDICATED", "HYBRID", "STANDARD"]
    workspace_id: str
    contact_emails: list[str] | None = None
    default_task_pod_cpu: str | None = None
    default_task_pod_memory: str | None = None
    description: str | None = None
    dr_workload_identity: str | None = None
    is_development_mode: bool | None = None
    remote_execution: RemoteExecution | None = None
    resource_quota_cpu: str | None = None
    resource_quota_memory: str | None = None
    scaling_spec: DeploymentScalingSpecRequest | None = None
    worker_queues: list[WorkerQueueRequest] | None = None
    workload_identity: str | None = None

    def set_env_var(self, key: str, value: str, is_secret: bool = False) -> Self:
        env_var = DeploymentEnvironmentVariableRequest(key=key, value=value, is_secret=is_secret)
        existing_var = next((ev for ev in self.environment_variables if ev.key == key), None)
        if existing_var:
            self.environment_variables.remove(existing_var)
        self.environment_variables.append(env_var)
        return self

    def del_env_var(self, key: str) -> Self:
        existing_var = next((ev for ev in self.environment_variables if ev.key == key), None)
        if existing_var:
            self.environment_variables.remove(existing_var)
        return self

    def set_hibernation_override(self, is_hibernating: bool, override_until: datetime | None = None) -> Self:
        if not self.scaling_spec:
            self.scaling_spec = DeploymentScalingSpecRequest()
        if not self.scaling_spec.hibernation_spec:
            self.scaling_spec.hibernation_spec = DeploymentHibernationSpecRequest()
        self.scaling_spec.hibernation_spec.override = DeploymentHibernationOverrideRequest(
            is_hibernating=is_hibernating,
            override_until=override_until,
        )
        return self

    def del_hibernation_override(self) -> Self:
        if self.scaling_spec and self.scaling_spec.hibernation_spec:
            self.scaling_spec.hibernation_spec.override = None
        return self


class AstroApiClient:
    def __init__(self, org_id: str, api_key: str):
        self.org_id = org_id
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {api_key}"})
        self.base_url = f"{BASE_URL}/organizations/{org_id}"

    def _request(self, method: str, path: str, **kwargs) -> requests.Response:
        url = f"{self.base_url}{path}"
        try:
            response = self.session.request(method, url, **kwargs)

            response.raise_for_status()
            return response
        except requests.HTTPError as e:
            logger.error("HTTP error occurred: %s - %s", e.response.status_code, e.response.text)
            raise

    def get_deployment(self, deployment_id: str) -> Deployment:
        response = self._request("GET", f"/deployments/{deployment_id}")
        return Deployment.model_validate_json(response.text)

    def update_deployment(
        self,
        deployment_id: str,
        request: UpdateDeploymentRequest,
    ) -> Deployment:
        response = self._request(
            "POST",
            f"/deployments/{deployment_id}",
            json=request.model_dump(by_alias=True, exclude_none=True),
        )
        return Deployment.model_validate_json(response.text)
