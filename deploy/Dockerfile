FROM julia:1.12

ARG BIOCIRCUITS_EXPLORER_VERSION=""
ARG BIOCIRCUITS_EXPLORER_REVISION=unknown
ARG BIOCIRCUITS_EXPLORER_CREATED=unknown

LABEL org.opencontainers.image.title="Biocircuits Explorer"
LABEL org.opencontainers.image.version="${BIOCIRCUITS_EXPLORER_VERSION}"
LABEL org.opencontainers.image.revision="${BIOCIRCUITS_EXPLORER_REVISION}"
LABEL org.opencontainers.image.created="${BIOCIRCUITS_EXPLORER_CREATED}"
LABEL org.opencontainers.image.source="https://github.com/YanzhangJiang/Biocircuits-Explorer"

# Keep Julia package images portable across EC2 host changes.
ENV JULIA_CPU_TARGET=generic
ENV BIOCIRCUITS_EXPLORER_VERSION="${BIOCIRCUITS_EXPLORER_VERSION}"
ENV BIOCIRCUITS_EXPLORER_REVISION="${BIOCIRCUITS_EXPLORER_REVISION}"
ENV BIOCIRCUITS_EXPLORER_CREATED="${BIOCIRCUITS_EXPLORER_CREATED}"
ENV JULIA_NUM_THREADS=auto

# Install system dependencies. AWS CLI is used by the optional Batch worker
# path for S3 artifact transfer and job submission/status calls.
RUN apt-get update && \
    apt-get install -y --no-install-recommends libcdd-dev curl python3-venv && \
    python3 -m venv /opt/awscli && \
    /opt/awscli/bin/pip install --no-cache-dir awscli && \
    ln -s /opt/awscli/bin/aws /usr/local/bin/aws && \
    rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -s /bin/bash rop
WORKDIR /home/rop/app

# Copy Bnc_julia first (Pkg.develop needs full source)
COPY VERSION VERSION
COPY Bnc_julia/ Bnc_julia/
COPY webapp/Project.toml webapp/Manifest.toml webapp/
COPY webapp/ webapp/

# Ensure the runtime depot and named-volume mountpoint inherit app-user
# ownership when Docker initializes them from the image.
RUN mkdir -p /home/rop/.julia/compiled /home/rop/app/webapp/job_store && \
    chown -R rop:rop /home/rop

USER rop

# Install Julia dependencies as the runtime user so the depot and caches
# are created with the same ownership used in the container.
RUN julia --project=webapp -e ' \
    using Pkg; \
    Pkg.develop(path="Bnc_julia"); \
    Pkg.instantiate(); \
    Pkg.precompile()'

EXPOSE 8088

HEALTHCHECK --interval=30s --timeout=10s --retries=5 --start-period=600s \
    CMD curl -fsS http://127.0.0.1:8088/health || exit 1

CMD ["julia", "--project=webapp", "webapp/server.jl"]
