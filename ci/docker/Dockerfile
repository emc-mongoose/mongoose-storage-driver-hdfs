ARG BASE_DRIVER_VERSION

FROM adoptopenjdk/openjdk11:alpine
ADD src ./src/
ADD gradle ./gradle/
ADD build.gradle ./build.gradle
ADD gradlew ./gradlew
ADD settings.gradle ./settings.gradle
RUN ./gradlew clean jar

FROM emcmongoose/mongoose-storage-driver-nio:${BASE_DRIVER_VERSION}
ARG BASE_VERSION
ARG VERSION
COPY --from=0 /build/libs/mongoose-storage-driver-hdfs-${VERSION}.jar /tmp/
RUN mv -f /tmp/mongoose-storage-driver-hdfs-*.jar $HOME/.mongoose/${BASE_VERSION}/ext/
