## -----------------------------
# Project: DIH Ingestion
# Docker: ARM setup
# Author: 
## -----------------------------

FROM python:3.14.2-bookworm

LABEL version="1.0"

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN apt-get --fix-missing update \
    && apt-get install -y xz-utils \
    && apt-get -y install curl \
    && apt-get -y install git iproute2 procps lsb-release \
    && apt-get -y install sudo

# Install Azure CLI
RUN curl -sL https://aka.ms/InstallAzureCLIDeb | bash

# Install Databricks CLI
RUN curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

# Install the ARM compatible version of the default JDK
RUN sudo apt-get -y install openjdk-17-jdk

# Install claude
RUN curl -fsSL https://claude.ai/install.sh | bash
RUN export PATH="$HOME/.local/bin:$PATH"

# Pip install
RUN pip install --upgrade pip

# Set working directory
WORKDIR /workspace/dih-ingestion
RUN export PYTHONPATH="/workspace/dih-ingestion/:$PYTHONPATH"

COPY requirements.txt /workspace/dih-ingestion

# install python dependencies
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt --root-user-action=ignore