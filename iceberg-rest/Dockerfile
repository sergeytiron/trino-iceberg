# Build the Iceberg REST adapter fixture jar from source
ARG ICEBERG_REF=main

FROM gradle:8-jdk17 AS build
ARG ICEBERG_REF
RUN set -eux; \
    git clone --depth 1 --branch "${ICEBERG_REF}" https://github.com/apache/iceberg.git /src
WORKDIR /src
# Build only the open-api runtime test fixtures jar (no tests to keep it quick)
RUN ./gradlew :open-api:testFixturesRuntimeJar -x test --no-daemon

FROM azul/zulu-openjdk:17-jre-headless
# Copy the built runtime jar
COPY --from=build /src/open-api/build/libs/iceberg-open-api-test-fixtures-runtime-*.jar /usr/lib/iceberg-rest/iceberg-rest-adapter.jar

# Default env; can be overridden by docker-compose
ENV CATALOG_CATALOG__IMPL=org.apache.iceberg.jdbc.JdbcCatalog \
    CATALOG_URI=jdbc:postgresql://postgres:5432/iceberg \
    CATALOG_JDBC_USER=iceberg \
    CATALOG_JDBC_PASSWORD=iceberg \
    CATALOG_JDBC_STRICT__MODE=true \
    CATALOG_WAREHOUSE=s3://warehouse/ \
    CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO \
    CATALOG_S3_ENDPOINT=http://minio:9000 \
    CATALOG_S3_PATH__STYLE__ACCESS=true \
    CATALOG_S3_REGION=us-east-1 \
    CATALOG_S3_ACCESS__KEY__ID=minioadmin \
    CATALOG_S3_SECRET__ACCESS__KEY=minioadmin

EXPOSE 8181
ENTRYPOINT ["java","-jar","/usr/lib/iceberg-rest/iceberg-rest-adapter.jar"]
