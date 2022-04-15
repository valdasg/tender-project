FROM apache/airflow:2.2.3

# Airflow and SPARK arguments
ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW_CONFIG=$AIRFLOW_HOME/airflow.cfg

# Change user to root temporarrly for instalation of personal dependancies
USER root
# -y assume yes, do not install recommendations
# autoremove packages that were needed for instalation, but are not needed for running, assume yes, quiet, purge config files
# clean cache
# install curl and wget

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && apt-get install -y wget \
    && apt-get install -y curl \
    && apt-get install -y git

# Install packages from requirements.txt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install GCP

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

ARG CLOUD_SDK_VERSION=322.0.0
ENV GCLOUD_HOME=/home/google-cloud-sdk

ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"

RUN DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/google-cloud-sdk.tar.gz" \
    && mkdir -p "${GCLOUD_HOME}" \
    && tar xzf "${TMP_DIR}/google-cloud-sdk.tar.gz" -C "${GCLOUD_HOME}" --strip-components=1 \
    && "${GCLOUD_HOME}/install.sh" \
       --bash-completion=false \
       --path-update=false \
       --usage-reporting=false \
       --quiet \
    && rm -rf "${TMP_DIR}" \
    && gcloud --version

WORKDIR $AIRFLOW_HOME
USER $AIRFLOW_UID



