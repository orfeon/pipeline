# Containers

Dockerfiles for building Mercari Pipeline images that are not built with Jib.

Profiles that use Jib (e.g. `dataflow`, `direct`, `server`) build and push images directly via `mvn package -Dimage=...`. The containers listed here require a separate `docker build` step after the Maven build.

## flink

Flink Application Mode image for running pipelines on Apache Flink clusters.

- **Base image**: `flink:1.20-java21`
- **Contents**: Pipeline fat JAR placed at `/opt/flink/usrlib/pipeline.jar`
- **Usage**: JobManager and TaskManager for Flink Kubernetes Operator or standalone clusters

```sh
mvn clean package -DskipTests -Pflink
docker build -f containers/flink/Dockerfile -t {image} .
```

## spark

Spark Application Mode image for running pipelines on Apache Spark clusters.

- **Base image**: `apache/spark:4.0.0-java21`
- **Contents**: Pipeline fat JAR placed at `/opt/spark/usrlib/pipeline.jar`
- **Usage**: Driver and executor for Spark on Kubernetes or standalone clusters

```sh
mvn clean package -DskipTests -Pspark
docker build -f containers/spark/Dockerfile -t {image} .
```

## dataflow-gpu

Dataflow worker SDK container image with CUDA/cuDNN for GPU-accelerated inference.

This is **not** a Flex Template launcher image. The launcher is built with Jib via `mvn package -Pdataflow-gpu -Dimage=...` (same as standard Dataflow). This image is the worker SDK container that runs on Dataflow worker VMs with attached GPUs.

- **Base image**: `apache/beam_java21_sdk` + CUDA/cuDNN runtime libraries from `nvidia/cuda`
- **Contents**: Beam SDK with CUDA runtime libraries (`libcudart`, `libcublas`, `libcudnn`, etc.)
- **Usage**: Specified via `--sdk_container_image` when launching a Dataflow GPU job

```sh
docker build -f containers/dataflow-gpu/Dockerfile \
  --build-arg BEAM_VERSION=2.74.0 \
  -t {image} .
```

| Image | Role | Build method |
|---|---|---|
| `dataflow-gpu` (launcher) | DAG construction and job submission | `mvn package -Pdataflow-gpu -Dimage=...` (Jib) |
| `dataflow-gpu-worker` | Pipeline execution with GPU | `docker build -f containers/dataflow-gpu/Dockerfile` |
