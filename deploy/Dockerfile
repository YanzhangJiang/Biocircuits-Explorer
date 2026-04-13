FROM julia:1.12

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends libcdd-dev curl && \
    rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -s /bin/bash rop
WORKDIR /home/rop/app

# Copy Bnc_julia first (Pkg.develop needs full source)
COPY Bnc_julia/ Bnc_julia/
COPY webapp/Project.toml webapp/Manifest.toml webapp/
COPY webapp/ webapp/

# Ensure the runtime depot exists and is writable by the app user.
RUN mkdir -p /home/rop/.julia/compiled && \
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
    CMD curl -f http://localhost:8088/ || exit 1

CMD ["julia", "-t", "auto", "--project=webapp", "webapp/server.jl"]
