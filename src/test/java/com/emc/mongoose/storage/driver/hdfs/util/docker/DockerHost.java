package com.emc.mongoose.storage.driver.hdfs.util.docker;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public interface DockerHost {

	Pattern ENV_VALUE_PATTERN = Pattern.compile("[\\w\\d]+://([\\w\\d\\\\.\\-_]+):\\d{1,5}");

	String ENV_SVC_HOST = /*envDockerHost() == null ? */"127.0.0.1"/* : extractHost(envDockerHost())*/;

	static String envDockerHost() {
		return System.getenv("DOCKER_HOST");
	}

	static String extractHost(final String envDockerHost) {
		final Matcher m = ENV_VALUE_PATTERN.matcher(envDockerHost);
		if(m.find()) {
			return m.group(1);
		} else {
			throw new AssertionError("Invalid docker host env value: " + envDockerHost);
		}
	}
}
