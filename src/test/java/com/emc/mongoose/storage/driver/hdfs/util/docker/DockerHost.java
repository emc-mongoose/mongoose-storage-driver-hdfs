package com.emc.mongoose.storage.driver.hdfs.util.docker;

public interface DockerHost {

	String ENV_SVC_HOST = /*isGitLabCiEnv() ? "storage" : */"127.0.0.1";

	static boolean isGitLabCiEnv() {
		return null != System.getenv("CI");
	}
}
