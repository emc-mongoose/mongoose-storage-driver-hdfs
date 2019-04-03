package com.emc.mongoose.storage.driver.hdfs.integration;

import com.emc.mongoose.base.data.DataInput;
import com.emc.mongoose.base.env.Extension;
import com.emc.mongoose.base.item.DataItem;
import com.emc.mongoose.base.item.DataItemFactoryImpl;
import com.emc.mongoose.base.item.Item;
import com.emc.mongoose.base.item.ItemFactory;
import com.emc.mongoose.base.storage.Credential;
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
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.emc.mongoose.base.Constants.APP_NAME;
import static com.emc.mongoose.storage.driver.hdfs.util.docker.DockerHost.ENV_SVC_HOST;
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
			config.val("load-batch-size", 128);
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
			config.val("storage-net-node-addrs", Collections.singletonList(ENV_SVC_HOST));
			config.val("storage-net-node-port", HdfsNodeContainer.PORT);
			config.val("storage-net-node-connAttemptsLimit", 0);
			config.val("storage-auth-uid", CREDENTIAL.getUid());
			config.val("storage-auth-token", null);
			config.val("storage-auth-secret", CREDENTIAL.getSecret());
			config.val("storage-driver-threads", 0);
			config.val("storage-driver-limit-queue-input", 1_000_000);
			config.val("storage-driver-limit-queue-output", 1_000_000);
			config.val("storage-driver-limit-concurrency", 0);
			return config;
		} catch(final Throwable cause) {
			throw new RuntimeException(cause);
		}
	}

	public CommonTest()
	throws Exception {
		this(getConfig());
	}

	private CommonTest(final Config config)
	throws Exception {
		super(
			"hdfs", "test-common-hdfs-driver", DATA_INPUT,
			config.configVal("storage"), false, config.configVal("load").intVal("batch-size")
		);
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
		final String parentDirPath = "/testDirListing";
		final int fileCount = 321;
		final int fileSize = 0x10_00_00;
		final byte[] fileContent = new byte[fileSize];
		for(int i = 0; i < fileSize; i ++) {
			fileContent[i] = (byte) System.nanoTime();
		}

		IntStream
			.range(0, fileCount)
			.parallel()
			.mapToObj(Integer::toString)
			.forEach(
				fileName -> {
					final Path dstFilePath = new Path(parentDirPath, fileName);
					try(
						final FSDataOutputStream dstFileOutput = fs.create(
							dstFilePath, defaultFsPerm, false, BUFF_SIZE_MIN, (short) 1, fileSize, null
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
			);

		final ItemFactory<DataItem> dataItemFactory = new DataItemFactoryImpl<>();
		final List<DataItem> listedItems = list(dataItemFactory, parentDirPath, null, 10, null, fileCount);

		assertEquals(fileCount, listedItems.size());
		listedItems
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
			.forEach(
				dataItemName ->
					assertTrue(
						Integer.parseInt(dataItemName.substring(dataItemName.lastIndexOf('/') + 1))
							< fileCount
					)
			);
	}
}
