package com.emc.mongoose.storage.driver.hdfs.util.docker;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.api.model.HostConfig;
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
			DOCKER_CLIENT.inspectImageCmd(IMAGE_NAME).exec();
		} catch(final NotFoundException e) {
			DOCKER_CLIENT
				.pullImageCmd(IMAGE_NAME)
				.exec(new PullImageResultCallback())
				.awaitCompletion();
		}
		final CreateContainerResponse container = DOCKER_CLIENT
			.createContainerCmd(IMAGE_NAME)
			.withName("hdfs_node")
			.withHostConfig(HostConfig.newHostConfig().withNetworkMode("host"))
			.withExposedPorts(ExposedPort.tcp(PORT))
			.withAttachStderr(true)
			.withAttachStdout(true)
			.exec();
		CONTAINER_ID = container.getId();
		LOG.info("docker start " + CONTAINER_ID + "...");
		DOCKER_CLIENT.startContainerCmd(CONTAINER_ID).exec();
		TimeUnit.SECONDS.sleep(100);
		DOCKER_CLIENT
			.logContainerCmd(CONTAINER_ID)
			.withFollowStream(true)
			.withStdOut(true)
			.exec(
				new ResultCallback<>() {
					@Override
					public void onStart(final Closeable closeable) {
					}

					@Override
					public void onNext(final Frame object) {
						System.out.print("HDFS_NODE: " + new String(object.getPayload()));
					}

					@Override
					public void onError(final Throwable throwable) {
						throwable.printStackTrace(System.err);
					}

					@Override
					public void onComplete() {
					}

					@Override
					public void close() {
					}
				}
			);

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
