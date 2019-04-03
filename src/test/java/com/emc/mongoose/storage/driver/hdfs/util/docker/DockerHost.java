package com.emc.mongoose.storage.driver.hdfs.util.docker;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public interface DockerHost {

	Pattern ENV_VALUE_PATTERN = Pattern.compile("[\\w\\d]+://([\\w\\d\\\\.\\-_]+):\\d{1,5}");

	String ENV_SVC_HOST = envDockerHost() == null ? "127.0.0.1" : envDockerHost();

	static String envDockerHost() {
		return System.getenv("SERVICE_HOST");
	}
}
