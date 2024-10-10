FROM python:3.10-alpine
# Package
RUN apk update && apk add --no-cache git mariadb-dev build-base

RUN addgroup -S appgroup && adduser -S appuser -G appgroup
RUN mkdir -p /usr/src/app
RUN chown -R appuser:appgroup /usr/src/app/

USER appuser
WORKDIR /usr/src/app
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV PIP_ROOT_USER_ACTION=ignore

COPY --chown=appuser:appgroup . /usr/src/app/
ENV PATH="/usr/src/app/venv/bin:$PATH"

RUN python -m venv /usr/src/app/venv/
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
RUN pip install .
RUN pip uninstall -y ensembl-hive

CMD ["/usr/src/app/venv/bin/python", "/usr/src/app/src/ensembl/production/metadata/grpc/service.py"]
