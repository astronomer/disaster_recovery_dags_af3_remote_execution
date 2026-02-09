FROM astrocrpublic.azurecr.io/runtime:3.1-12

ENV AIRFLOW__CORE__ALLOWED_DESERIALIZATION_CLASSES="include.*"
