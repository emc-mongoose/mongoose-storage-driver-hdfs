package com.emc.mongoose.storage.driver.hdfs.util;

public interface HdfsNode {

	static String addr() {
		final boolean ciFlag = null == System.getenv("CI");
		if(ciFlag) {
			return "hdfsnode";
		} else {
			return "localhost";
		}
	}
}
