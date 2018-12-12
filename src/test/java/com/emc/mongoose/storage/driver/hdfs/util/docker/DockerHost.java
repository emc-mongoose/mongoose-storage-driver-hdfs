package com.emc.mongoose.storage.driver.hdfs.util.docker;

public interface DockerHost {

	String ENV_NAME = "DOCKER_HOST";

	String ENV_SVC_HOST = System.getenv(ENV_NAME) == null ? "127.0.0.1" : System.getenv(ENV_NAME);
}
