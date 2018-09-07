package com.emc.mongoose.storage.driver.hdfs.util.docker;

import com.emc.mongoose.config.BundledDefaultsProvider;
import static com.emc.mongoose.Constants.APP_NAME;
import static com.emc.mongoose.config.CliArgUtil.ARG_PATH_SEP;

import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.api.model.Volume;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.command.BuildImageResultCallback;
import com.github.dockerjava.core.command.WaitContainerResultCallback;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class MongooseContainer
implements Runnable, Closeable {

	private static final Logger LOG = Logger.getLogger(MongooseContainer.class.getSimpleName());
	private static final String BASE_DIR = new File("").getAbsolutePath();
	private static final String APP_VERSION;
	static {
		final Config bundledDefaults;
		try {
			final Map<String, Object> schema = SchemaProvider.resolveAndReduce(
				APP_NAME, Thread.currentThread().getContextClassLoader()
			);
			bundledDefaults = new BundledDefaultsProvider().config(ARG_PATH_SEP, schema);
		} catch(final Exception e) {
			throw new IllegalStateException(
				"Failed to load the bundled default config from the resources", e
			);
		}
		APP_VERSION = bundledDefaults.stringVal("run-version");
	}

	public static final String CONTAINER_SHARE_PATH = "/root/.mongoose/" + APP_VERSION + "/share";
	public static final Path HOST_SHARE_PATH = Paths.get(BASE_DIR, "share");
	static {
		HOST_SHARE_PATH.toFile().mkdir();
	}
	private static final String CONTAINER_LOG_PATH = "/root/.mongoose/" + APP_VERSION + "/log";
	public static final Path HOST_LOG_PATH = Paths.get(BASE_DIR, "share", "log");
	static {
		HOST_LOG_PATH.toFile().mkdir();
	}

	private final List<String> configArgs = new ArrayList<>();
	private final StringBuilder stdOutBuff = new StringBuilder();
	private final StringBuilder stdErrBuff = new StringBuilder();
	private final ResultCallback<Frame> streamsCallback = new ContainerOutputCallback(
		stdOutBuff, stdErrBuff
	);

	private final DockerClient dockerClient;
	private final int durationLimitSeconds;

	private String testContainerId = null;
	private long duration = -1;
	private String stdOutput = null;
	private int containerExitCode = -1;

	public long getDuration() {
		return duration;
	}

	public String getStdOutput() {
		return stdOutput;
	}

	private int getContainerExitCode() {
		return containerExitCode;
	}

	public MongooseContainer(final List<String> configArgs, final int durationLimitSeconds) {

		this.durationLimitSeconds = durationLimitSeconds;

		this.dockerClient = DockerClientBuilder.getInstance().build();

		final File dockerBuildFile = Paths
			.get(BASE_DIR, "docker", "Dockerfile")
			.toFile();
		LOG.info("Build mongoose image w/ HDFS support using the dockerfile " + dockerBuildFile);
		final BuildImageResultCallback buildImageResultCallback = new BuildImageResultCallback();
		this.dockerClient
			.buildImageCmd()
			.withBaseDirectory(new File(BASE_DIR))
			.withDockerfile(dockerBuildFile)
			.withBuildArg("MONGOOSE_VERSION", APP_VERSION)
			.withPull(true)
			.withTags(Collections.singleton("emcmongoose/mongoose-storage-driver-hdfs:testing"))
			.exec(buildImageResultCallback);
		final String testingImageId = buildImageResultCallback.awaitImageId();
		LOG.info("Build mongoose testing image id: " + testingImageId);

		this.configArgs.add("--output-metrics-trace-persist=true");
		this.configArgs.add("--storage-net-node-port=" + HdfsNodeContainer.PORT);
		for(final String configArg: configArgs) {
			if(configArg.startsWith("--run-scenario=")) {
				final String scenarioPathStr = configArg.substring("--run-scenario=".length());
				if(scenarioPathStr.startsWith(HOST_SHARE_PATH.toString())) {
					this.configArgs.add(
						"--run-scenario=" + CONTAINER_SHARE_PATH
							+ scenarioPathStr.substring(HOST_SHARE_PATH.toString().length())
					);
				} else {
					this.configArgs.add(
						"--run-scenario=" + CONTAINER_SHARE_PATH + scenarioPathStr
					);
				}
			} else {
				this.configArgs.add(configArg);
			}
		}

		LOG.info("Mongoose test container arguments: " + Arrays.toString(configArgs.toArray()));

		final Volume volumeShare = new Volume(CONTAINER_SHARE_PATH);
		final Volume volumeLog = new Volume(CONTAINER_LOG_PATH);
		final Bind[] binds = new Bind[] {
			new Bind(HOST_SHARE_PATH.toString(), volumeShare),
			new Bind(HOST_LOG_PATH.toString(), volumeLog),
		};

		// put the environment variables into the container
		final Map<String, String> envMap = System.getenv();
		final String[] env = envMap.keySet().toArray(new String[envMap.size()]);
		for(int i = 0; i < env.length; i ++) {
			if("PATH".equals(env[i])) {
				env[i] = env[i] + "=" + envMap.get(env[i]) + ":/bin";
			} else {
				env[i] = env[i] + "=" + envMap.get(env[i]);
			}
		}

		final CreateContainerResponse container = dockerClient
			.createContainerCmd(testingImageId)
			.withName("mongoose")
			.withNetworkMode("host")
			.withExposedPorts(ExposedPort.tcp(9010), ExposedPort.tcp(5005))
			.withVolumes(volumeShare, volumeLog)
			.withBinds(binds)
			.withAttachStdout(true)
			.withAttachStderr(true)
			.withEntrypoint("/opt/mongoose/entrypoint-storage-driver-hdfs.sh")
			.withEnv(env)
			.withCmd(this.configArgs)
			.exec();

		testContainerId = container.getId();
		LOG.info("Created the mongoose test container w/ id: " + testContainerId);
	}

	public final void clearLogs(final String stepId) {
		final File logDir = Paths.get(HOST_LOG_PATH.toString(), stepId).toFile();
		final File[] logDirFiles = logDir.listFiles();
		if(logDirFiles != null) {
			for(final File logFile : logDirFiles) {
				logFile.delete();
			}
		}
		logDir.delete();
	}

	@Override
	public final void run() {
		dockerClient
			.attachContainerCmd(testContainerId)
			.withStdErr(true)
			.withStdOut(true)
			.withFollowStream(true)
			.exec(streamsCallback);
		duration = System.currentTimeMillis();
		dockerClient.startContainerCmd(testContainerId).exec();
		containerExitCode = dockerClient
			.waitContainerCmd(testContainerId)
			.exec(new WaitContainerResultCallback())
			.awaitStatusCode(durationLimitSeconds, TimeUnit.SECONDS);
		duration = System.currentTimeMillis() - duration;
		stdOutput = stdOutBuff.toString();
	}

	@Override
	public final void close()
	throws IOException {
		streamsCallback.close();
		if(testContainerId != null) {
			dockerClient
				.removeContainerCmd(testContainerId)
				.withForce(true)
				.exec();
			testContainerId = null;
		}
		dockerClient.close();
	}
}
