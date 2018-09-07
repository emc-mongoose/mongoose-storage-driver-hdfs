package com.emc.mongoose.storage.driver.hdfs.util.docker;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.InspectImageCmd;
import com.github.dockerjava.api.command.InspectImageResponse;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.command.PullImageResultCallback;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class HdfsNodeContainer
implements Closeable {

	public static final int PORT = 9000;
	private static final Logger LOG = Logger.getLogger(HdfsNodeContainer.class.getSimpleName());
	private static final String IMAGE_NAME = "dockerq/docker-hdfs";
	private static final DockerClient DOCKER_CLIENT = DockerClientBuilder.getInstance().build();

	private static String CONTAINER_ID = null;

	public HdfsNodeContainer()
	throws Exception {
		try {
			final InspectImageResponse result = DOCKER_CLIENT.inspectImageCmd(IMAGE_NAME).exec();
		} catch(final NotFoundException e) {
			DOCKER_CLIENT
				.pullImageCmd(IMAGE_NAME)
				.exec(new PullImageResultCallback())
				.awaitCompletion();
		}
		final CreateContainerResponse container = DOCKER_CLIENT
			.createContainerCmd(IMAGE_NAME)
			.withName("hdfs_node")
			.withNetworkMode("host")
			.withAttachStderr(true)
			.withAttachStdout(true)
			.exec();
		CONTAINER_ID = container.getId();
		LOG.info("docker start " + CONTAINER_ID + "...");
		DOCKER_CLIENT.startContainerCmd(CONTAINER_ID).exec();
		TimeUnit.SECONDS.sleep(30);

	}

	public final void close() {
		if(CONTAINER_ID != null) {
			LOG.info("docker kill " + CONTAINER_ID + "...");
			DOCKER_CLIENT.killContainerCmd(CONTAINER_ID).exec();
			LOG.info("docker rm " + CONTAINER_ID + "...");
			DOCKER_CLIENT.removeContainerCmd(CONTAINER_ID).exec();
			CONTAINER_ID = null;
		}
	}
}
