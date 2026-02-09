import logging
import os
from datetime import timedelta

from airflow.sdk import Param, PokeReturnValue, Variable, dag, get_current_context, task, task_group

from include.astro_api import (
    AstroApiClient,
    Deployment,
    DeploymentHibernationOverride,
)
from include.starship import StarshipClient

logger = logging.getLogger(__name__)


ASTRO_ORGANIZATION_ID = os.environ["ASTRO_ORGANIZATION_ID"]
ASTRO_API_KEY = os.environ["ASTRO_API_KEY"]

DR_DEPLOYMENTS = {  # TODO add deployment IDs
    # "active-deployment-id": "standby-deployment-id",
}
"""A mapping from active deployment IDs to standby deployment IDs."""


@task
def get_deployments() -> list[dict[str, Deployment]]:
    astro_client = AstroApiClient(ASTRO_ORGANIZATION_ID, ASTRO_API_KEY)
    deployments = []
    is_failover = Variable.get("dr_failover_enabled", default=False, deserialize_json=True)

    for src, dst in DR_DEPLOYMENTS.items():
        src_deployment = astro_client.get_deployment(src)
        assert src_deployment is not None, f"Source deployment with ID {src} not found"
        logger.info("Found source deployment: %s", src_deployment.name)

        dst_deployment = astro_client.get_deployment(dst)
        assert dst_deployment is not None, f"Target deployment with ID {dst} not found"
        logger.info("Found target deployment: %s", dst_deployment.name)

        if is_failover:
            deployments.append({"source": dst_deployment, "target": src_deployment})
        else:
            deployments.append({"source": src_deployment, "target": dst_deployment})

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
def dags_paused(source: Deployment, target: Deployment) -> None:
    starship_src = StarshipClient(source.ui_url, ASTRO_API_KEY)
    starship_dst = StarshipClient(target.ui_url, ASTRO_API_KEY)

    for d in starship_src.get_dags():
        starship_dst.set_dag_paused(d.dag_id, d.is_paused)


@task
def dag_runs(source: Deployment, target: Deployment) -> None:
    starship_src = StarshipClient(source.ui_url, ASTRO_API_KEY)
    starship_dst = StarshipClient(target.ui_url, ASTRO_API_KEY)

    limit = 100
    for d in starship_src.get_dags():
        starship_dst.delete_dag_runs(d.dag_id)

        offset = 0
        while True:
            dag_runs = starship_src.get_dag_runs(d.dag_id, limit=limit, offset=offset)
            if not dag_runs:
                break

            starship_dst.set_dag_runs(dag_runs)
            offset += limit


@task
def task_instances(source: Deployment, target: Deployment) -> None:
    starship_src = StarshipClient(source.ui_url, ASTRO_API_KEY)
    starship_dst = StarshipClient(target.ui_url, ASTRO_API_KEY)

    limit = 10
    for d in starship_src.get_dags():
        offset = 0
        while True:
            response = starship_src.get_task_instances(d.dag_id, limit=limit, offset=offset)
            starship_dst.set_task_instances(response.task_instances)

            offset += limit
            if offset > response.dag_run_count:
                break


@task
def task_instance_history(source: Deployment, target: Deployment) -> None:
    starship_src = StarshipClient(source.ui_url, ASTRO_API_KEY)
    starship_dst = StarshipClient(target.ui_url, ASTRO_API_KEY)

    limit = 10
    for d in starship_src.get_dags():
        offset = 0
        while True:
            response = starship_src.get_task_instance_history(d.dag_id, limit=limit, offset=offset)
            starship_dst.set_task_instance_history(response.task_instances)

            offset += limit
            if offset > response.dag_run_count:
                break


@task
def variables(source: Deployment, target: Deployment) -> None:
    starship_src = StarshipClient(source.ui_url, ASTRO_API_KEY)
    starship_dst = StarshipClient(target.ui_url, ASTRO_API_KEY)

    for variable in starship_dst.get_variables():
        starship_dst.delete_variable(variable.key)

    for variable in starship_src.get_variables():
        starship_dst.set_variable(variable)


@task
def connections(source: Deployment, target: Deployment) -> None:
    starship_src = StarshipClient(source.ui_url, ASTRO_API_KEY)
    starship_dst = StarshipClient(target.ui_url, ASTRO_API_KEY)

    for connection in starship_dst.get_connections():
        starship_dst.delete_connection(connection.conn_id)

    for connection in starship_src.get_connections():
        starship_dst.set_connection(connection)


@task
def pools(source: Deployment, target: Deployment) -> None:
    starship_src = StarshipClient(source.ui_url, ASTRO_API_KEY)
    starship_dst = StarshipClient(target.ui_url, ASTRO_API_KEY)

    for pool in starship_dst.get_pools():
        if pool.is_default:
            continue
        starship_dst.delete_pool(pool.name)

    for pool in starship_src.get_pools():
        if pool.is_default:
            continue
        starship_dst.set_pool(pool)


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
def starship(source: Deployment, target: Deployment):
    dags_paused(source, target)
    dag_runs(source, target) >> task_instances(source, target) >> task_instance_history(source, target)
    variables(source, target)
    connections(source, target)
    pools(source, target)


@task_group
def replicate(source: Deployment, target: Deployment):
    source_hibernation_override = set_hibernation.override(task_id="wake_up_source")(source, False).as_setup()
    target_hibernation_override = set_hibernation.override(task_id="wake_up_target")(target, False).as_setup()

    source_woken_up = source_hibernation_override >> wait_for_deployment_wake_up.override(
        task_id="wait_for_source",
    )(source)
    target_woken_up = target_hibernation_override >> wait_for_deployment_wake_up.override(
        task_id="wait_for_target",
    )(target)

    scheduling_disabled = [source_woken_up, target_woken_up] >> use_job_schedule.override(
        task_id="disable_scheduling_target"
    )(target, False)

    replicated = scheduling_disabled >> starship(source, target)

    (
        replicated
        >> revert_hibernation.override(task_id="hibernate_source")(
            source,
            source_hibernation_override,  # ty:ignore[invalid-argument-type]
        ).as_teardown()
    )
    (
        replicated
        >> revert_hibernation.override(task_id="hibernate_target")(
            target,
            target_hibernation_override,  # ty:ignore[invalid-argument-type]
        ).as_teardown()
    )


@task_group
def failover(source: Deployment, target: Deployment):
    # source
    use_job_schedule.override(task_id="disable_scheduling_source")(source, False) >> set_hibernation.override(
        task_id="hibernate_source"
    )(source, True)

    # target
    use_job_schedule.override(task_id="enable_scheduling_target")(target, True) >> set_hibernation.override(
        task_id="wake_up_target"
    )(target, False)


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
