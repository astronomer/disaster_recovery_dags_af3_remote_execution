import json
import logging
import os
from datetime import timedelta

from airflow.exceptions import AirflowFailException
from airflow.sdk import Param, PokeReturnValue, Variable, dag, get_current_context, task, task_group
from packaging.specifiers import SpecifierSet
from packaging.version import Version

from include.astro_api import (
    AstroApiClient,
    Deployment,
    DeploymentHibernationOverride,
)
from include.starship import StarshipClient

logger = logging.getLogger(__name__)

SUPPORTED_AIRFLOW_VERSIONS = SpecifierSet("~=3.0,<3.2")
SUPPORTED_STARSHIP_VERSIONS = SpecifierSet("~=2.8")

ASTRO_ORGANIZATION_ID = os.environ["ASTRO_ORGANIZATION_ID"]
ASTRO_API_KEY = os.environ["ASTRO_API_KEY"]
DR_DEPLOYMENTS = json.loads(os.environ.get("DR_DEPLOYMENTS", "{}"))
"""A mapping from active deployment IDs to standby deployment IDs."""


@task
def get_deployments() -> list[dict[str, Deployment]]:
    astro_client = AstroApiClient(ASTRO_ORGANIZATION_ID, ASTRO_API_KEY)
    deployments = []
    is_failover = Variable.get("dr_failover_enabled", default=False, deserialize_json=True)

    for act, sby in DR_DEPLOYMENTS.items():
        act_deployment = astro_client.get_deployment(act)
        assert act_deployment is not None, f"active deployment with ID {act} not found"
        logger.info("Found active deployment: %s", act_deployment.name)

        sby_deployment = astro_client.get_deployment(sby)
        assert sby_deployment is not None, f"standby deployment with ID {sby} not found"
        logger.info("Found standby deployment: %s", sby_deployment.name)

        if is_failover:
            deployments.append({"active": sby_deployment, "standby": act_deployment})
        else:
            deployments.append({"active": act_deployment, "standby": sby_deployment})

    return deployments


@task
def set_hibernation(deployment: Deployment, is_hibernating: bool) -> DeploymentHibernationOverride | None:
    if not deployment.is_development_mode:
        logger.info("Deployment %s is not in development mode, skipping", deployment.name)
        return None

    astro_client = AstroApiClient(ASTRO_ORGANIZATION_ID, ASTRO_API_KEY)
    deployment = astro_client.get_deployment(deployment.id)

    if deployment.scaling_spec is not None and deployment.scaling_spec.hibernation_spec is not None:
        override = deployment.scaling_spec.hibernation_spec.override
    else:
        override = None

    astro_client.update_deployment(
        deployment.id, deployment.update().set_hibernation_override(is_hibernating)
    )
    logger.info("Override set for deployment %s", deployment.name)
    return override


@task.sensor(poke_interval=10, timeout=600, mode="poke")
def wait_for_deployment_wake_up(deployment: Deployment) -> PokeReturnValue:
    astro_client = AstroApiClient(ASTRO_ORGANIZATION_ID, ASTRO_API_KEY)
    deployment = astro_client.get_deployment(deployment.id)

    logger.info(
        "Checking deployment: status= %s, is_hibernating= %s",
        deployment.status,
        deployment.is_hibernating(),
    )

    if not deployment.is_hibernating() and deployment.status == "HEALTHY":
        logger.info("Deployment %s is awake", deployment.name)
        return PokeReturnValue(is_done=True)

    return PokeReturnValue(is_done=False)


@task
def use_job_schedule(deployment: Deployment, use: bool) -> None:
    astro_client = AstroApiClient(ASTRO_ORGANIZATION_ID, ASTRO_API_KEY)
    deployment = astro_client.get_deployment(deployment.id)

    astro_client.update_deployment(
        deployment.id, deployment.update().set_env_var("AIRFLOW__SCHEDULER__USE_JOB_SCHEDULE", str(use))
    )


@task
def revert_hibernation(deployment: Deployment, override: DeploymentHibernationOverride | None) -> None:
    if not deployment.is_development_mode:
        logger.info("Deployment %s is not in development mode, skipping", deployment.name)
        return None

    astro_client = AstroApiClient(ASTRO_ORGANIZATION_ID, ASTRO_API_KEY)
    deployment = astro_client.get_deployment(deployment.id)

    if override is not None:
        logger.info("Restoring hibernation override for deployment %s", deployment.name)
        request = deployment.update().set_hibernation_override(
            override.is_hibernating, override.override_until
        )
    else:
        request = deployment.update().del_hibernation_override()

    astro_client.update_deployment(deployment.id, request)
    logger.info("Deployment %s hibernation override restored", deployment.name)


@task
def check_version(active: Deployment, standby: Deployment) -> None:
    starship_act = StarshipClient(active.ui_url, ASTRO_API_KEY)
    starship_sby = StarshipClient(standby.ui_url, ASTRO_API_KEY)
    info_act = starship_act.get_info()
    info_sby = starship_sby.get_info()

    for info in [info_act, info_sby]:
        airflow_version = Version(info.airflow_version)
        starship_version = Version(info.starship_version)

        if airflow_version not in SUPPORTED_AIRFLOW_VERSIONS:
            raise AirflowFailException(
                f"Airflow version {airflow_version} not supported. Must be within {SUPPORTED_AIRFLOW_VERSIONS}"
            )

        if starship_version not in SUPPORTED_STARSHIP_VERSIONS:
            raise AirflowFailException(
                f"Starship version {starship_version} not supported. Must be within {SUPPORTED_STARSHIP_VERSIONS}"
            )

    if info_act.airflow_version != info_sby.airflow_version:
        logger.warning(
            "Airflow version mismatch: active=%s, standby=%s",
            info_act.airflow_version,
            info_sby.airflow_version,
        )

    if info_act.starship_version != info_sby.starship_version:
        logger.warning(
            "Starship version mismatch: active=%s, standby=%s",
            info_act.starship_version,
            info_sby.starship_version,
        )


@task
def dags_paused(active: Deployment, standby: Deployment) -> None:
    starship_act = StarshipClient(active.ui_url, ASTRO_API_KEY)
    starship_sby = StarshipClient(standby.ui_url, ASTRO_API_KEY)

    for d in starship_act.get_dags():
        starship_sby.set_dag_paused(d.dag_id, d.is_paused)


@task
def dag_runs(active: Deployment, standby: Deployment) -> None:
    starship_act = StarshipClient(active.ui_url, ASTRO_API_KEY)
    starship_sby = StarshipClient(standby.ui_url, ASTRO_API_KEY)

    limit = 100
    for d in starship_act.get_dags():
        starship_sby.delete_dag_runs(d.dag_id)

        offset = 0
        while True:
            dag_runs = starship_act.get_dag_runs(d.dag_id, limit=limit, offset=offset)
            if not dag_runs:
                break

            starship_sby.set_dag_runs(dag_runs)
            offset += limit


@task
def task_instances(active: Deployment, standby: Deployment) -> None:
    starship_act = StarshipClient(active.ui_url, ASTRO_API_KEY)
    starship_sby = StarshipClient(standby.ui_url, ASTRO_API_KEY)

    limit = 10
    for d in starship_act.get_dags():
        offset = 0
        while True:
            response = starship_act.get_task_instances(d.dag_id, limit=limit, offset=offset)
            starship_sby.set_task_instances(response.task_instances)

            offset += limit
            if offset > response.dag_run_count:
                break


@task
def task_instance_history(active: Deployment, standby: Deployment) -> None:
    starship_act = StarshipClient(active.ui_url, ASTRO_API_KEY)
    starship_sby = StarshipClient(standby.ui_url, ASTRO_API_KEY)

    limit = 10
    for d in starship_act.get_dags():
        offset = 0
        while True:
            response = starship_act.get_task_instance_history(d.dag_id, limit=limit, offset=offset)
            starship_sby.set_task_instance_history(response.task_instances)

            offset += limit
            if offset > response.dag_run_count:
                break


@task
def variables(active: Deployment, standby: Deployment) -> None:
    starship_act = StarshipClient(active.ui_url, ASTRO_API_KEY)
    starship_sby = StarshipClient(standby.ui_url, ASTRO_API_KEY)

    for variable in starship_sby.get_variables():
        starship_sby.delete_variable(variable.key)

    for variable in starship_act.get_variables():
        starship_sby.set_variable(variable)


@task
def connections(active: Deployment, standby: Deployment) -> None:
    starship_act = StarshipClient(active.ui_url, ASTRO_API_KEY)
    starship_sby = StarshipClient(standby.ui_url, ASTRO_API_KEY)

    for connection in starship_sby.get_connections():
        starship_sby.delete_connection(connection.conn_id)

    for connection in starship_act.get_connections():
        starship_sby.set_connection(connection)


@task
def pools(active: Deployment, standby: Deployment) -> None:
    starship_act = StarshipClient(active.ui_url, ASTRO_API_KEY)
    starship_sby = StarshipClient(standby.ui_url, ASTRO_API_KEY)

    for pool in starship_sby.get_pools():
        if pool.is_default:
            continue
        starship_sby.delete_pool(pool.name)

    for pool in starship_act.get_pools():
        if pool.is_default:
            continue
        starship_sby.set_pool(pool)


@task
def set_failover_state():
    ctx = get_current_context()
    enable_failover = bool(ctx["params"]["enable_failover"])
    Variable.set(
        "dr_failover_enabled",
        enable_failover,
        serialize_json=True,
        description="Whether the system is in failover state.",
    )


@task_group
def starship(active: Deployment, standby: Deployment):
    version_checked = check_version(active, standby)
    version_checked >> dags_paused(active, standby)
    (
        version_checked
        >> dag_runs(active, standby)
        >> task_instances(active, standby)
        >> task_instance_history(active, standby)
    )
    version_checked >> variables(active, standby)
    version_checked >> connections(active, standby)
    version_checked >> pools(active, standby)


@task_group
def replicate(active: Deployment, standby: Deployment):
    active_hibernation_override = set_hibernation.override(task_id="wake_up_active")(active, False).as_setup()
    standby_hibernation_override = set_hibernation.override(task_id="wake_up_standby")(
        standby, False
    ).as_setup()

    active_woken_up = active_hibernation_override >> wait_for_deployment_wake_up.override(
        task_id="wait_for_active",
    )(active)
    standby_woken_up = standby_hibernation_override >> wait_for_deployment_wake_up.override(
        task_id="wait_for_standby",
    )(standby)

    scheduling_disabled = [active_woken_up, standby_woken_up] >> use_job_schedule.override(
        task_id="disable_scheduling_standby"
    )(standby, False)

    replicated = scheduling_disabled >> starship(active, standby)

    (
        replicated
        >> revert_hibernation.override(task_id="hibernate_active")(
            active,
            active_hibernation_override,  # ty:ignore[invalid-argument-type]
        ).as_teardown()
    )
    (
        replicated
        >> revert_hibernation.override(task_id="hibernate_standby")(
            standby,
            standby_hibernation_override,  # ty:ignore[invalid-argument-type]
        ).as_teardown()
    )


@task_group
def failover(active: Deployment, standby: Deployment):
    # active
    use_job_schedule.override(task_id="disable_scheduling_active")(active, False) >> set_hibernation.override(
        task_id="hibernate_active"
    )(active, True)

    # standby
    use_job_schedule.override(task_id="enable_scheduling_standby")(standby, True) >> set_hibernation.override(
        task_id="wake_up_standby"
    )(standby, False)


@dag(
    schedule=None,  # TODO set schedule
    catchup=False,
    tags=["DR"],
    default_args={
        "retries": 5,
        "retry_delay": timedelta(seconds=30),
    },
    max_active_runs=1,
)
def dr_replication():
    deployments = get_deployments()
    replicate.expand_kwargs(deployments)


dr_replication()


@dag(
    schedule=None,
    catchup=False,
    tags=["DR"],
    default_args={
        "retries": 5,
        "retry_delay": timedelta(seconds=30),
    },
    params={
        "enable_failover": Param(
            True,
            description="Whether to enable failover. Set to False revert failover state back to normal operations.",
        ),
    },
    max_active_runs=1,
)
def dr_failover():
    deployments = get_deployments()
    deployments >> set_failover_state() >> failover.expand_kwargs(deployments)


dr_failover()
