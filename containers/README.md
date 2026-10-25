# Containers

Dockerfiles for building Mercari Pipeline images that are not built with Jib.

Profiles that use Jib (e.g. `dataflow`, `direct`, `server`) build and push images directly via `mvn package -Dimage=...`. The containers listed here require a separate `docker build` step after the Maven build.

## flink

Flink Application Mode image for running pipelines on Apache Flink clusters.

- **Base image**: `flink:2.0.2-java21` (the `flink` profile bundles `beam-runners-flink-2.0`)
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
