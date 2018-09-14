package com.emc.mongoose.storage.driver.hdfs.integration;

import com.emc.mongoose.env.Extension;
import com.emc.mongoose.exception.OmgShootMyFootException;
import com.emc.mongoose.item.ItemType;
import com.emc.mongoose.item.PathItem;
import com.emc.mongoose.item.op.path.PathOperation;
import com.emc.mongoose.storage.Credential;
import com.emc.mongoose.storage.driver.hdfs.HdfsStorageDriver;
import com.emc.mongoose.storage.driver.hdfs.util.docker.StorageNodeContainer;
import com.github.akurilov.commons.collection.TreeUtil;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import com.github.akurilov.confuse.impl.BasicConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.emc.mongoose.Constants.APP_NAME;
import static org.junit.Assert.fail;

public class PathOperationsTest
extends HdfsStorageDriver<PathItem, PathOperation<PathItem>> {

	private static final Credential CREDENTIAL = Credential.getInstance("root", "nope");
	private static StorageNodeContainer HDFS_NODE_CONTAINER;

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
			config.val("item-type", ItemType.PATH.name().toLowerCase());
			config.val("load-batch-size", 4096);
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
			config.val("storage-net-node-port", StorageNodeContainer.PORT);
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

	public PathOperationsTest()
	throws OmgShootMyFootException {
		this(getConfig());
	}

	private PathOperationsTest(final Config config)
	throws OmgShootMyFootException {
		super(
			"hdfs", "test-path-hdfs-driver", null,
			config.configVal("storage"), false, config.configVal("load").intVal("batch-size")
		);
	}

	@BeforeClass
	public static void setUpClass()
	throws Exception {
		try {
			HDFS_NODE_CONTAINER = new StorageNodeContainer();
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
