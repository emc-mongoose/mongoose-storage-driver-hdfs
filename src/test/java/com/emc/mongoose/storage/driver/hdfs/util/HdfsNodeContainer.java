package com.emc.mongoose.storage.driver.hdfs.util;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.command.PullImageResultCallback;

import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class HdfsNodeContainer {

	public static final int PORT = 9000;
	private static final Logger LOG = Logger.getLogger(HdfsNodeContainer.class.getSimpleName());
	private static final String IMAGE_NAME = "dockerq/docker-hdfs";
	private static final DockerClient DOCKER_CLIENT = DockerClientBuilder.getInstance().build();

	private static boolean NEW_IMAGE_PULLED = false;
	private static String CONTAINER_ID = null;

	public static void setUpClass()
	throws Exception {
		if(NEW_IMAGE_PULLED) {
			LOG.info("Reusing the pulled image: " + IMAGE_NAME);
		} else {
			LOG.info("docker pull " + IMAGE_NAME + "...");
			DOCKER_CLIENT
				.pullImageCmd(IMAGE_NAME)
				.exec(new PullImageResultCallback())
				.awaitSuccess();
			NEW_IMAGE_PULLED = true;
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

	public static void tearDownClass() {
		if(CONTAINER_ID != null) {
			LOG.info("docker kill " + CONTAINER_ID + "...");
			DOCKER_CLIENT.killContainerCmd(CONTAINER_ID).exec();
			LOG.info("docker rm " + CONTAINER_ID + "...");
			DOCKER_CLIENT.removeContainerCmd(CONTAINER_ID).exec();
			CONTAINER_ID = null;
		}
	}
}
