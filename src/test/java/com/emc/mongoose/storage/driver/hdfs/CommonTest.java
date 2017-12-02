package com.emc.mongoose.storage.driver.hdfs;

import com.emc.mongoose.api.common.exception.OmgShootMyFootException;
import com.emc.mongoose.api.model.data.DataInput;
import com.emc.mongoose.api.model.item.BasicDataItemFactory;
import com.emc.mongoose.api.model.item.DataItem;
import com.emc.mongoose.api.model.item.Item;
import com.emc.mongoose.api.model.item.ItemFactory;
import com.emc.mongoose.api.model.storage.Credential;
import com.emc.mongoose.storage.driver.hdfs.util.HdfsNodeContainer;
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

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CommonTest
extends HdfsStorageDriver {

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
			nodeConfig.setPort(HdfsNodeContainer.PORT);
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

	public CommonTest()
	throws OmgShootMyFootException {
		this(getConfig());
	}

	private CommonTest(final Config config)
	throws OmgShootMyFootException {
		super(
			"test-common-hdfs-driver", DATA_INPUT, config.getLoadConfig(),
			config.getStorageConfig(), false
		);
	}

	@BeforeClass
	public static void setUpClass()
	throws Exception {
		HdfsNodeContainer.setUpClass();
	}

	@AfterClass
	public static void tearDownClass()
	throws Exception {
		HdfsNodeContainer.tearDownClass();
	}

	@Test
	public final void testGetEndpoint()
	throws Exception {
		final FileSystem fs = getEndpoint("127.0.0.1");
		assertNotNull(fs);
		fs.close();
	}

	@Test
	public final void testDirectoryListing()
	throws Exception {

		final FileSystem fs = (FileSystem) endpoints.values().iterator().next();
		final String parentDirPath = "/default";
		final int fileCount = 4321;
		final int fileSize = 0x10_00_00;
		final byte[] fileContent = new byte[fileSize];
		for(int i = 0; i < fileSize; i ++) {
			fileContent[i] = (byte) System.nanoTime();
		}

		long n = IntStream
			.range(0, fileCount)
			.parallel()
			.mapToObj(Integer::toString)
			.peek(
				fileName -> {
					final Path dstFilePath = new Path(parentDirPath, fileName);
					try(
						final FSDataOutputStream dstFileOutput = fs.create(
							dstFilePath, defaultFsPerm, false, BUFF_SIZE_MIN, (short) 1, fileSize,
							null
						)
					) {
						dstFileOutput.write(fileContent);
					} catch(final IOException e) {
						System.err.println(
							"Failure while writing the file \"" + dstFilePath + "\": "
								+ e.getMessage()
						);
					}
				}
			)
			.count();

		final ItemFactory<DataItem> dataItemFactory = new BasicDataItemFactory<>();
		final List<DataItem> listedItems = list(
			dataItemFactory, parentDirPath, null, 10, null, fileCount
		);

		assertEquals(fileCount, listedItems.size());
		n = listedItems
			.parallelStream()
			.peek(
				dataItem -> {
					try {
						assertEquals(fileSize, dataItem.size());
					} catch(final IOException ignored) {
					}
				}
			)
			.map(Item::getName)
			.peek(
				dataItemName ->
					assertTrue(
						Integer.parseInt(dataItemName.substring(dataItemName.lastIndexOf('/') + 1))
							< fileCount
					)
			)
			.count();
	}
}
