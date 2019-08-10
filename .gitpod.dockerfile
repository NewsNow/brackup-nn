FROM i386/debian:stretch

USER root
# Install custom tools, runtime, etc.
RUN apt-get update && \
    apt-get install -y \
        curl vim less procps psmisc bash-completion locales \
        libfile-find-rule-perl libmoose-perl libcoro-perl libjson-perl libjson-xs-perl libdata-dump-perl \
    && apt-get clean && rm -rf /var/cache/apt/* && rm -rf /var/lib/apt/lists/* && rm -rf /tmp/* && \
    curl https://storage.googleapis.com/perl-ls/perl-languageserver-debian-stretch-i386.tgz | tar xz -C /

# USER gitpod
# Apply user-specific settings
# ENV ...

# Give back control
USER root
