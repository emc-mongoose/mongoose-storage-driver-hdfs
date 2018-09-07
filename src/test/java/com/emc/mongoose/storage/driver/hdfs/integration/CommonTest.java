package com.emc.mongoose.storage.driver.hdfs.integration;

import com.emc.mongoose.data.DataInput;
import com.emc.mongoose.env.Extension;
import com.emc.mongoose.exception.OmgShootMyFootException;
import com.emc.mongoose.item.DataItem;
import com.emc.mongoose.item.DataItemFactoryImpl;
import com.emc.mongoose.item.Item;
import com.emc.mongoose.item.ItemFactory;
import com.emc.mongoose.storage.Credential;
import com.emc.mongoose.storage.driver.hdfs.HdfsStorageDriver;
import com.emc.mongoose.storage.driver.hdfs.util.docker.HdfsNodeContainer;
import com.github.akurilov.commons.collection.TreeUtil;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import com.github.akurilov.confuse.impl.BasicConfig;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.emc.mongoose.Constants.APP_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CommonTest
extends HdfsStorageDriver {

	private static final DataInput DATA_INPUT;
	static {
		try {
			DATA_INPUT = DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("4MB"), 16);
		} catch(final IOException e) {
			throw new AssertionError(e);
		}
	}

	private static final Credential CREDENTIAL = Credential.getInstance("root", "nope");
	private static HdfsNodeContainer HDFS_NODE_CONTAINER;

	private static Config getConfig() {
		try {
			final List<Map<String, Object>> configSchemas = Extension
				.load(Thread.currentThread().getContextClassLoader())
				.stream()
				.map(Extension::schemaProvider)
				.filter(Objects::nonNull)
				.map(
					schemaProvider -> {
						try {
							return schemaProvider.schema();
						} catch(final Exception e) {
							fail(e.getMessage());
						}
						return null;
					}
				)
				.filter(Objects::nonNull)
				.collect(Collectors.toList());
			SchemaProvider
				.resolve(APP_NAME, Thread.currentThread().getContextClassLoader())
				.stream()
				.findFirst()
				.ifPresent(configSchemas::add);
			final Map<String, Object> configSchema = TreeUtil.reduceForest(configSchemas);
			final Config config = new BasicConfig("-", configSchema);
			config.val("load-batch-size", 4096);
			config.val("load-step-limit-concurrency", 0);
			config.val("storage-net-reuseAddr", true);
			config.val("storage-net-bindBacklogSize", 0);
			config.val("storage-net-keepAlive", true);
			config.val("storage-net-rcvBuf", 0);
			config.val("storage-net-sndBuf", 0);
			config.val("storage-net-ssl", false);
			config.val("storage-net-tcpNoDelay", false);
			config.val("storage-net-interestOpQueued", false);
			config.val("storage-net-linger", 0);
			config.val("storage-net-timeoutMilliSec", 0);
			config.val("storage-net-node-addrs", Collections.singletonList("127.0.0.1"));
			config.val("storage-net-node-port", HdfsNodeContainer.PORT);
			config.val("storage-net-node-connAttemptsLimit", 0);
			config.val("storage-auth-uid", CREDENTIAL.getUid());
			config.val("storage-auth-token", null);
			config.val("storage-auth-secret", CREDENTIAL.getSecret());
			config.val("storage-driver-threads", 0);
			config.val("storage-driver-queue-input", 1_000_000);
			config.val("storage-driver-queue-output", 1_000_000);
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
			"hdfs", "test-common-hdfs-driver", DATA_INPUT,
			config.configVal("storage"), false, config.configVal("load").intVal("batch-size")
		);
	}

	@BeforeClass
	public static void setUpClass()
	throws Exception {
		try {
			HDFS_NODE_CONTAINER = new HdfsNodeContainer();
		} catch(final Exception e) {
			throw new AssertionError(e);
		}
	}

	@AfterClass
	public static void tearDownClass()
	throws Exception {
		HDFS_NODE_CONTAINER.close();
	}

	@Test
	public final void testGetEndpoint()
	throws Exception {
		final FileSystem fs = getEndpoint(endpointAddrs[0]);
		assertNotNull(fs);
		fs.close();
	}

	@Test
	public final void testDirectoryListing()
	throws Exception {

		final FileSystem fs = getEndpoint(endpointAddrs[0]);
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

		final ItemFactory<DataItem> dataItemFactory = new DataItemFactoryImpl<>();
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
			.map(Item::name)
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
