FROM astrocrpublic.azurecr.io/runtime:3.1-14

ENV AIRFLOW__CORE__ALLOWED_DESERIALIZATION_CLASSES="include.*"
