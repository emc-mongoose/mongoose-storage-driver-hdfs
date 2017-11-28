package com.emc.mongoose.storage.driver.hdfs;

import com.emc.mongoose.api.common.exception.OmgShootMyFootException;
import com.emc.mongoose.api.model.data.DataInput;
import com.emc.mongoose.api.model.io.task.data.DataIoTask;
import com.emc.mongoose.api.model.item.DataItem;
import com.emc.mongoose.api.model.storage.Credential;
import com.emc.mongoose.storage.driver.hdfs.util.HdfsNodeContainerResource;
import com.emc.mongoose.ui.config.Config;
import com.emc.mongoose.ui.config.load.LoadConfig;
import com.emc.mongoose.ui.config.load.batch.BatchConfig;
import com.emc.mongoose.ui.config.load.rate.LimitConfig;
import com.emc.mongoose.ui.config.storage.StorageConfig;
import com.emc.mongoose.ui.config.storage.auth.AuthConfig;
import com.emc.mongoose.ui.config.storage.driver.DriverConfig;
import com.emc.mongoose.ui.config.storage.driver.queue.QueueConfig;
import com.emc.mongoose.ui.config.storage.net.NetConfig;
import com.emc.mongoose.ui.config.storage.net.node.NodeConfig;
import com.github.akurilov.commons.system.SizeInBytes;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

public class DataOperationsTest
extends HdfsStorageDriver<DataItem, DataIoTask<DataItem>> {

	@ClassRule
	public static final HdfsNodeContainerResource HDFS_NODE = new HdfsNodeContainerResource();

	private static final DataInput DATA_INPUT;
	static {
		try {
			DATA_INPUT = DataInput.getInstance(null, "7a42d9c483244167", new SizeInBytes("4MB"), 16);
		} catch(final IOException e) {
			throw new AssertionError(e);
		}
	}

	private static final Credential CREDENTIAL = Credential.getInstance("root", "nope");

	private static Config getConfig() {
		try {
			final Config config = new Config();
			final LoadConfig loadConfig = new LoadConfig();
			config.setLoadConfig(loadConfig);
			final BatchConfig batchConfig = new BatchConfig();
			loadConfig.setBatchConfig(batchConfig);
			batchConfig.setSize(4096);
			final LimitConfig limitConfig = new LimitConfig();
			loadConfig.setLimitConfig(limitConfig);
			limitConfig.setConcurrency(0);
			final StorageConfig storageConfig = new StorageConfig();
			config.setStorageConfig(storageConfig);
			final NetConfig netConfig = new NetConfig();
			storageConfig.setNetConfig(netConfig);
			netConfig.setReuseAddr(true);
			netConfig.setBindBacklogSize(0);
			netConfig.setKeepAlive(true);
			netConfig.setRcvBuf(new SizeInBytes(0));
			netConfig.setSndBuf(new SizeInBytes(0));
			netConfig.setSsl(false);
			netConfig.setTcpNoDelay(false);
			netConfig.setInterestOpQueued(false);
			netConfig.setLinger(0);
			netConfig.setTimeoutMilliSec(0);
			netConfig.setIoRatio(50);
			final NodeConfig nodeConfig = new NodeConfig();
			netConfig.setNodeConfig(nodeConfig);
			nodeConfig.setAddrs(Collections.singletonList("127.0.0.1"));
			nodeConfig.setPort(9024);
			nodeConfig.setConnAttemptsLimit(0);
			final AuthConfig authConfig = new AuthConfig();
			storageConfig.setAuthConfig(authConfig);
			authConfig.setUid(CREDENTIAL.getUid());
			authConfig.setToken(null);
			authConfig.setSecret(CREDENTIAL.getSecret());
			final DriverConfig driverConfig = new DriverConfig();
			storageConfig.setDriverConfig(driverConfig);
			final QueueConfig queueConfig = new QueueConfig();
			driverConfig.setQueueConfig(queueConfig);
			queueConfig.setInput(1000000);
			queueConfig.setOutput(1000000);
			return config;
		} catch(final Throwable cause) {
			throw new RuntimeException(cause);
		}
	}

	public DataOperationsTest()
	throws OmgShootMyFootException {
		this(getConfig());
	}

	private DataOperationsTest(final Config config)
	throws OmgShootMyFootException {
		super(
			"test-data-hdfs-driver", DATA_INPUT, config.getLoadConfig(), config.getStorageConfig(),
			false
		);
	}

	@Test
	public final void testCreateFile()
	throws Exception {

	}

	@Test
	public final void testCopyFile()
	throws Exception {

	}

	@Test
	public final void testConcatFile()
	throws Exception {

	}

	@Test
	public final void testReadFullFile()
	throws Exception {

	}

	@Test
	public final void testReadFixedRangesFile()
	throws Exception {

	}

	@Test
	public final void testReadRandomRangesFile()
	throws Exception {

	}

	@Test
	public final void testOverwriteFile()
	throws Exception {

	}

	@Test
	public final void testUpdateRandomRangesFile()
	throws Exception {

	}

	@Test
	public final void testUpdateFixedRangesFile()
	throws Exception {

	}

	@Test
	public final void testDeleteFile()
	throws Exception {

	}
}
