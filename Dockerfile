FROM python:3.8

RUN mkdir /service

COPY protos/ /service/protos/

COPY src/ensembl/production/metadata /service/ensembl/production/metadata

WORKDIR /service/

RUN python -m pip install --upgrade pip

RUN python -m pip install -r ensembl/production/metadata/requirements.txt

RUN python -m grpc_tools.protoc -I protos --python_out=. \

           --grpc_python_out=. protos/ensembl_metadata.proto

EXPOSE 50051

ENTRYPOINT [ "python", "ensembl/production/metadata/service.py" ]
