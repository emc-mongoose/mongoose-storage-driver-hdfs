ARG BASE_VERSION

FROM adoptopenjdk/openjdk11:alpine
ADD src ./src/
ADD gradle ./gradle/
ADD build.gradle ./build.gradle
ADD gradlew ./gradlew
ADD settings.gradle ./settings.gradle
RUN ./gradlew clean jar

FROM emcmongoose/mongoose-base:${BASE_VERSION}
ARG BASE_VERSION
ARG STORAGE_DRIVER_COOP_VERSION
ARG STORAGE_DRIVER_NIO_VERSION
ARG VERSION
RUN mkdir -p $HOME/.mongoose/${BASE_VERSION}/ext
COPY --from=0 /build/libs/mongoose-storage-driver-hdfs-${VERSION}.jar /tmp/
ADD ci/docker/entrypoint_storage_driver_hdfs.sh /opt/mongoose/entrypoint_storage_driver_hdfs.sh
RUN mv -f /tmp/mongoose-storage-driver-hdfs-*.jar $HOME/.mongoose/${BASE_VERSION}/ext/ \
    && chmod +x /opt/mongoose/entrypoint_storage_driver_hdfs.sh \
    && curl http://repo.maven.apache.org/maven2/com/github/emc-mongoose/mongoose-storage-driver-coop/${STORAGE_DRIVER_COOP_VERSION}/mongoose-storage-driver-coop-${STORAGE_DRIVER_COOP_VERSION}.jar -o $HOME/.mongoose/${BASE_VERSION}/ext/mongoose-storage-driver-coop.jar \
    && curl http://repo.maven.apache.org/maven2/com/github/emc-mongoose/mongoose-storage-driver-nio/${STORAGE_DRIVER_NIO_VERSION}/mongoose-storage-driver-nio-${STORAGE_DRIVER_NIO_VERSION}.jar -o $HOME/.mongoose/${BASE_VERSION}/ext/mongoose-storage-driver-nio.jar
ENTRYPOINT ["/opt/mongoose/entrypoint_storage_driver_hdfs.sh"]
