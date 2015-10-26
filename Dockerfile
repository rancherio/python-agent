FROM rancher/dind:v1.9.0-rc2
COPY ./scripts/bootstrap /scripts/bootstrap
RUN /scripts/bootstrap
WORKDIR /source
