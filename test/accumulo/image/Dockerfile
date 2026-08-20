FROM maven:3.9.11-eclipse-temurin-21 AS accumulo-build

ARG ACCUMULO_REVISION=1a716b2c1bb5762ead4b46d2bc4f53e13873b314
ARG ACCUMULO_SOURCE_SHA512=bf2cac6f7b5f759358ac306bba2342dc74c51bf63d70905da1004fc1eb0cfa66f6a73023ab250824a67f17d410e9b3351f097ddfd742bbc4b14f7a7feb6c4b2e

ADD https://github.com/apache/accumulo/archive/${ACCUMULO_REVISION}.tar.gz /accumulo-source.tar.gz

RUN set -eux; \
    echo "${ACCUMULO_SOURCE_SHA512}  /accumulo-source.tar.gz" | sha512sum -c -; \
    mkdir /source; \
    tar -xzf /accumulo-source.tar.gz --strip-components=1 -C /source; \
    cd /source; \
    mvn --batch-mode --no-transfer-progress \
      -pl assemble -am \
      -DskipTests \
      -Dcheckstyle.skip \
      -Dspotbugs.skip \
      -Drat.skip \
      package; \
    archive="$(find assemble/target -maxdepth 1 -type f -name 'accumulo-*-bin.tar.gz' -print -quit)"; \
    test -n "${archive}"; \
    mkdir /opt/accumulo; \
    tar -xzf "${archive}" --strip-components=1 -C /opt/accumulo

FROM eclipse-temurin:21-jdk-jammy

ARG HADOOP_VERSION=3.4.2
ARG HADOOP_SHA512=79a383e156022d6690da359120b25db8146452265d92a4e890d9ea78c2078a01b661daf78163ee9b4acef7106b01fd5c8d1a55f7ad284f88b31ab3f402ae3acf

ADD https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz /hadoop.tar.gz

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends bash libsnappy1v5 procps; \
    rm -rf /var/lib/apt/lists/*; \
    echo "${HADOOP_SHA512}  /hadoop.tar.gz" | sha512sum -c -; \
    tar -xzf /hadoop.tar.gz -C /opt; \
    mv "/opt/hadoop-${HADOOP_VERSION}" /opt/hadoop; \
    rm /hadoop.tar.gz

COPY --from=accumulo-build /opt/accumulo /opt/accumulo

ENV ACCUMULO_HOME=/opt/accumulo
ENV HADOOP_HOME=/opt/hadoop
ENV PATH="${PATH}:/opt/accumulo/bin:/opt/hadoop/bin"

ENTRYPOINT ["/opt/accumulo/bin/accumulo"]
CMD ["help"]
