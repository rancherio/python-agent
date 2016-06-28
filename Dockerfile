FROM rancher/dind:v1.10.3-rancher1
COPY ./scripts/bootstrap /scripts/bootstrap
RUN /scripts/bootstrap
WORKDIR /source
