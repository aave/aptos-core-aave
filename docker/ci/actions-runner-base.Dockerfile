FROM ghcr.io/actions/actions-runner:2.316.1

COPY scripts/dev_setup.sh scripts/dev_setup.sh
COPY rust-toolchain.toml rust-toolchain.toml

RUN sudo apt-get update -y && sudo apt-get install -y git

RUN scripts/dev_setup.sh -b -k
