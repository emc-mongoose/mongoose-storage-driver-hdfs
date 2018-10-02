ARG MONGOOSE_VERSION

FROM emcmongoose/mongoose:${MONGOOSE_VERSION}

ARG MONGOOSE_VERSION

ADD build/libs/mongoose-storage-driver-hdfs.jar /opt/mongoose/mongoose-storage-driver-hdfs.jar
ADD docker/entrypoint-storage-driver-hdfs.sh /opt/mongoose/entrypoint-storage-driver-hdfs.sh
ADD docker/entrypoint-storage-driver-hdfs-debug.sh /opt/mongoose/entrypoint-storage-driver-hdfs-debug.sh

RUN mkdir $HOME/.mongoose/${MONGOOSE_VERSION}; \
    mkdir $HOME/.mongoose/${MONGOOSE_VERSION}/ext; \
    ln -s /opt/mongoose/mongoose-storage-driver-hdfs.jar $HOME/.mongoose/${MONGOOSE_VERSION}/ext/mongoose-storage-driver-hdfs.jar; \
    chmod +x /opt/mongoose/entrypoint-storage-driver-hdfs.sh; \
    chmod +x /opt/mongoose/entrypoint-storage-driver-hdfs-debug.sh

ENTRYPOINT ["/opt/mongoose/entrypoint-storage-driver-hdfs.sh"]
