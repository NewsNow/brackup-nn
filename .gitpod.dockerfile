# FROM gitpod/workspace-full
FROM debian:latest

USER root
# Install custom tools, runtime, etc.
RUN apt-get update && apt-get install -y \
        libanyevent-perl libcoro-perl libdata-dump-perl libmodule-build-xsutil-perl \
        libyaml-libyaml-perl \
    && apt-get clean && rm -rf /var/cache/apt/* && rm -rf /var/lib/apt/lists/* && rm -rf /tmp/*

USER gitpod
# Apply user-specific settings
# ENV ...

# Give back control
USER root
