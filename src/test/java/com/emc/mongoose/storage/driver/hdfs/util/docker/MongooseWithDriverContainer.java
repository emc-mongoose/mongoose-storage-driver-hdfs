package com.emc.mongoose.storage.driver.hdfs.util.docker;

import com.github.dockerjava.core.command.BuildImageResultCallback;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class MongooseWithDriverContainer
	extends MongooseContainer {

	public static final String IMAGE_VERSION = MongooseContainer.IMAGE_VERSION;
	public static final String APP_VERSION = MongooseContainer.APP_VERSION;
	public static final String APP_HOME_DIR = MongooseContainer.APP_HOME_DIR;
	public static final String BASE_DIR = new File("").getAbsolutePath();
	public static final String CONTAINER_HOME_PATH = MongooseContainer.CONTAINER_HOME_PATH;
	private static String IMAGE_NAME;
	private static final String ENTRYPOINT = "/opt/mongoose/entrypoint.sh";
	private static final String ENTRYPOINT_DEBUG = "/opt/mongoose/entrypoint-debug.sh";
	private static final int PORT_DEBUG = 5005;
	private static final int PORT_JMX = 9010;
	public static final String CONTAINER_SHARE_PATH = CONTAINER_HOME_PATH + "/share";
	public static final Path HOST_SHARE_PATH = Paths.get(APP_HOME_DIR, "share");

	static {
		HOST_SHARE_PATH.toFile().mkdir();
	}

	private static final String CONTAINER_LOG_PATH = CONTAINER_HOME_PATH + "/log";
	public static final Path HOST_LOG_PATH = Paths.get(APP_HOME_DIR, "log");

	static {
		HOST_LOG_PATH.toFile().mkdir();
	}

	private static final Map<String, Path> VOLUME_BINDS = new HashMap<String, Path>() {{
		put(CONTAINER_LOG_PATH, HOST_LOG_PATH);
		put(CONTAINER_SHARE_PATH, HOST_SHARE_PATH);
	}};

	public static String systemTestContainerScenarioPath(final Class testCaseCls) {
		return MongooseContainer.systemTestContainerScenarioPath(testCaseCls);
	}

	public static String enduranceTestContainerScenarioPath(final Class testCaseCls) {
		return MongooseContainer.enduranceTestContainerScenarioPath(testCaseCls);
	}

	private final List<String> args;
	private String containerItemOutputPath = null;
	private String hostItemOutputPath = null;

	public static String getContainerItemOutputPath(final String stepId) {
		return MongooseContainer.getContainerItemOutputPath(stepId);
	}

	public static String getHostItemOutputPath(final String stepId) {
		return MongooseContainer.getHostItemOutputPath(stepId);
	}

	public MongooseWithDriverContainer(
		final String imageName, final String stepId, final String containerScenarioPath,
		final List<String> env, final List<String> args, final boolean attachOutputFlag,
		final boolean collectOutputFlag, final boolean outputMetricsTracePersistFlag
	) throws InterruptedException {
		this(imageName, IMAGE_VERSION, stepId, containerScenarioPath, env, args, attachOutputFlag, collectOutputFlag,
			outputMetricsTracePersistFlag);
	}

	public MongooseWithDriverContainer(
		final String imageName, final String version, final String stepId, final String containerScenarioPath,
		final List<String> env, final List<String> args, final boolean attachOutputFlag,
		final boolean collectOutputFlag, final boolean outputMetricsTracePersistFlag
	) throws InterruptedException {
		super(version, env, VOLUME_BINDS, attachOutputFlag, collectOutputFlag, PORT_DEBUG, PORT_JMX);
		this.args = args;
		this.args.add("--load-step-id=" + stepId);
		if(outputMetricsTracePersistFlag) {
			this.args.add("--output-metrics-trace-persist");
		}
		if(containerScenarioPath != null) {
			this.args.add("--run-scenario=" + containerScenarioPath);
		}
		buildImage(imageName);
	}

	public final void imageName(final String imageName) {
		IMAGE_NAME = imageName;
	}

	@Override
	protected final String imageName() {
		return IMAGE_NAME;
	}

	@Override
	protected final List<String> containerArgs() {
		return args;
	}

	@Override
	protected final String entrypoint() {
		return ENTRYPOINT;
	}
}
