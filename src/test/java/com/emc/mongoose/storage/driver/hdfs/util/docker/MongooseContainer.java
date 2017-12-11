package com.emc.mongoose.storage.driver.hdfs.util.docker;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.api.model.Volume;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.command.WaitContainerResultCallback;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;

public class MongooseContainer
implements Runnable, Closeable {

	private static final String BASE_DIR = Paths.get(".").toAbsolutePath().normalize().toString();

	private static final String IMAGE_VERSION = System.getenv("IMAGE_VERSION") == null ?
		"latest" : System.getenv("IMAGE_VERSION");
	private static final String IMAGE_NAME = "emcmongoose/mongoose-storage-driver-hdfs:"
		+ IMAGE_VERSION;
	public static final String CONTAINER_SHARE_PATH = "/opt/mongoose/share";
	public static final Path HOST_SHARE_PATH = Paths.get(BASE_DIR, "share");
	static {
		HOST_SHARE_PATH.toFile().mkdir();
	}
	private static final String CONTAINER_LOG_PATH = "/opt/mongoose/log";
	private static final Path HOST_LOG_PATH = Paths.get(BASE_DIR, "share", "log");
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
		this.configArgs.add("--storage-driver-type=hdfs");
		this.configArgs.add("--output-metrics-trace-persist=true");
		this.configArgs.add("--storage-net-node-port=" + HdfsNodeContainer.PORT);
		for(final String configArg: configArgs) {
			if(configArg.startsWith("--test-scenario-file=")) {
				final String scenarioPathStr = configArg.substring(
					"--test-scenario-file=".length()
				);
				if(scenarioPathStr.startsWith(BASE_DIR)) {
					this.configArgs.add(
						"--test-scenario-file=/opt/mongoose"
							+ scenarioPathStr.substring(BASE_DIR.length())
					);
				} else {
					this.configArgs.add("--test-scenario-file=/opt/mongoose/" + scenarioPathStr);
				}
			} else {
				this.configArgs.add(configArg);
			}
		}

		final List<String> cmd = new ArrayList<>();
		cmd.add("-Xms1g");
		cmd.add("-Xmx1g");
		cmd.add("-XX:MaxDirectMemorySize=1g");
		cmd.add("-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005");
		cmd.add("-Dcom.sun.management.jmxremote=true");
		cmd.add("-Dcom.sun.management.jmxremote.port=9010");
		cmd.add("-Dcom.sun.management.jmxremote.rmi.port=9010");
		cmd.add("-Dcom.sun.management.jmxremote.local.only=false");
		cmd.add("-Dcom.sun.management.jmxremote.authenticate=false");
		cmd.add("-Dcom.sun.management.jmxremote.ssl=false");
		cmd.add("-jar");
		cmd.add("/opt/mongoose/mongoose.jar");
		cmd.addAll(this.configArgs);
		final StringJoiner cmdLine = new StringJoiner(" ");
		cmd.forEach(cmdLine::add);
		System.out.println("Container arguments: " + cmdLine.toString());

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
			.createContainerCmd(IMAGE_NAME)
			.withName("mongoose")
			.withNetworkMode("host")
			.withExposedPorts(ExposedPort.tcp(9010), ExposedPort.tcp(5005))
			.withVolumes(volumeShare, volumeLog)
			.withBinds(binds)
			.withAttachStdout(true)
			.withAttachStderr(true)
			.withEnv(env)
			.withEntrypoint("mongoose")
			.withCmd(cmd)
			.exec();

		testContainerId = container.getId();
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
