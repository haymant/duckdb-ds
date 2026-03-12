# Dockerfile for the duckdb-query-api service
# Uses python:3.12-slim as a base and creates an isolated virtualenv

FROM python:3.12-slim

# Create and activate a virtual environment early to ensure
# that all subsequent `pip` invocations use it.
ENV VENV_PATH=/opt/venv
RUN python -m venv ${VENV_PATH}
ENV PATH="${VENV_PATH}/bin:$PATH"

# Install any system dependencies needed to build wheels.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# when using the repository root (`.`) as the build context, all paths
# below are relative to that root.  Docker will copy the contents of the
# `duck-server` subdirectory into the image before installing.
#
# build command from project root:
#
#   docker build -f duck-server/Dockerfile -t duck-server .
#
# (context `.` includes both `duck-server` and sibling packages such as
# `featureHandler`.)

# copy service code and metadata first to take advantage of caching
COPY duck-server/pyproject.toml ./
# If you maintain a setup.cfg in the future you can add a COPY here;
# it's omitted now since the file does not exist and Docker COPY has no
# built‑in "optional" flag.  The full source tree is copied on the next
# line anyway.
COPY duck-server /app

# copy the sibling `featureHandler` package
COPY featureHandler /app/featureHandler

# upgrade pip and install packages
RUN pip install --upgrade pip setuptools wheel \
    && pip install --no-cache-dir /app/featureHandler \
    && pip install --no-cache-dir /app

# expose the port uvicorn will listen on (match CMD below)
EXPOSE 8201

# default command: launch uvicorn with the application defined
# in `main.py` (adjust if your entry point differs)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8201"]
