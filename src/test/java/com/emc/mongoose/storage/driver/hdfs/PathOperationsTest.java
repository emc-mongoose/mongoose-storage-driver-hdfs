package com.emc.mongoose.storage.driver.hdfs;

import com.emc.mongoose.api.common.exception.OmgShootMyFootException;
import com.emc.mongoose.api.model.io.task.path.PathIoTask;
import com.emc.mongoose.api.model.item.ItemType;
import com.emc.mongoose.api.model.item.PathItem;
import com.emc.mongoose.api.model.storage.Credential;
import com.emc.mongoose.storage.driver.hdfs.util.HdfsNodeContainer;
import com.emc.mongoose.ui.config.Config;
import com.emc.mongoose.ui.config.item.ItemConfig;
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

import java.util.Collections;

public class PathOperationsTest
extends HdfsStorageDriver<PathItem, PathIoTask<PathItem>> {

	@ClassRule
	public static final HdfsNodeContainer HDFS_NODE = new HdfsNodeContainer();

	private static final Credential CREDENTIAL = Credential.getInstance("root", "nope");

	private static Config getConfig() {
		try {
			final Config config = new Config();
			final ItemConfig itemConfig = new ItemConfig();
			itemConfig.setType(ItemType.PATH.name().toLowerCase());
			config.setItemConfig(itemConfig);
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

	public PathOperationsTest()
	throws OmgShootMyFootException {
		this(getConfig());
	}

	private PathOperationsTest(final Config config)
	throws OmgShootMyFootException {
		super(
			"test-path-hdfs-driver", null, config.getLoadConfig(), config.getStorageConfig(),
			false
		);
	}

	@Test
	public final void testCreateDir()
	throws Exception {

	}

	@Test
	public final void testReadDir()
	throws Exception {

	}

	@Test
	public final void testDeleteDir()
	throws Exception {

	}
}
