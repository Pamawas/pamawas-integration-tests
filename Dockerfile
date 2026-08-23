# Dockerfile for Pamawas Integration Tests
# This image contains all tools needed for integration tests

FROM golang:1.26-bullseye

# Install additional tools
RUN apt-get update && apt-get install -y \
    git \
    wget \
    curl \
    postgresql-client \
    build-essential \
    libssl-dev \
    zlib1g-dev \
    libncurses5-dev \
    libncursesw5-dev \
    libreadline-dev \
    libsqlite3-dev \
    libgdbm-dev \
    libdb5.3-dev \
    libbz2-dev \
    libexpat1-dev \
    liblzma-dev \
    libffi-dev \
    uuid-dev \
    tk-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python 3.14 from source
RUN cd /tmp && \
    wget https://www.python.org/ftp/python/3.14.0/Python-3.14.0.tgz && \
    tar xzf Python-3.14.0.tgz && \
    cd Python-3.14.0 && \
    ./configure --enable-optimizations && \
    make -j$(nproc) altinstall && \
    sudo ldconfig && \
    rm -rf /tmp/Python-3.14.0*

# Verify installations
RUN go version && python3.14 --version && pg_isready --version

WORKDIR /github/workspace

# Default command
CMD ["/bin/bash"]