# disaster_recovery_dags_af3

DAGs for user-level DR replication and failover in Airflow 3 using Starship

## Configuration

### DR deployment

The DR deployment is responsible for performing the replication and failover actions for the provided deployments. The dags are designed to run on Astro Runtime 3.1.

The following environment variables can be used to configure the DR dags.

| Name | Description |
| --- | --- |
| `ASTRO_API_KEY` | A API token with the owner permissions for the active and standby deployments. |
| `ASTRO_ORGANIZATION_ID` | The Astronomer organization ID. |
| `DR_DEPLOYMENTS` | A JSON mapping of deployment IDs from active to standby. |

### Active & standby deployments

The active and standby deployments for which we perform replication and failover must fulfill the following requirements:

* Airflow 3.0 - 3.1 installed
* Starship 2.8+ installed
