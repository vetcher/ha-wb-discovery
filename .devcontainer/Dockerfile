FROM ghcr.io/home-assistant/devcontainer:2-addons

RUN apt-get update && apt-get install -y python3 python3-pip python3-venv

# Create and activate virtual environment
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Upgrade pip and setuptools in the virtual environment
RUN pip install --upgrade pip setuptools virtualenv
