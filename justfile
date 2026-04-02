PYTHON_VERSION := "3.12"
RUNTIME_UPDATES_URL := "https://updates.astronomer.io/astronomer-runtime"
RUNTIME_VERSION := "3.1-14"
VENV := ".venv"
AIRFLOW_VERSION := shell('curl -fs $1 | jq -r ".runtimeVersionsV3[\"$2\"].metadata.airflowVersion"', RUNTIME_UPDATES_URL, RUNTIME_VERSION)

default: sync

_venv:
    @uv python --quiet install {{ PYTHON_VERSION }}
    @uv venv --quiet -p {{ PYTHON_VERSION }} --allow-existing --seed {{ VENV }}

_requirements_in: _venv
    @cp requirements.in {{ VENV }}/requirements.in
    @echo "\napache-airflow=={{ AIRFLOW_VERSION }}" >> {{ VENV }}/requirements.in
    @cat requirements-dev.in >> {{ VENV }}/requirements.in

# Compile requirements.txt based on requirements*.in files and Astro Runtime image
requirements: _requirements_in
    @docker run -qit --rm --entrypoint python astrocrpublic.azurecr.io/runtime:{{ RUNTIME_VERSION }} -m pip freeze > {{ VENV }}/requirements.tmp
    @uv pip compile --index-url https://pip.astronomer.io/v2/ --emit-index-url --quiet --no-emit-package apache-airflow --python-version {{ PYTHON_VERSION }} {{ VENV }}/requirements.in -o {{ VENV }}/requirements.tmp
    @cp {{ VENV }}/requirements.tmp requirements.txt

# Sync virtual env with requirements.txt, including apache-airflow
sync: _venv
    @cp requirements.txt {{ VENV }}/requirements.tmp
    @echo "\napache-airflow=={{ AIRFLOW_VERSION }}" >> {{ VENV }}/requirements.tmp
    @uv pip sync {{ VENV }}/requirements.tmp

version:
    @echo Apache Airflow Version: {{ AIRFLOW_VERSION }}
    @echo Astro Runtime Version: {{ RUNTIME_VERSION }}
    @astro version

type-check: sync
    @uvx ty check
