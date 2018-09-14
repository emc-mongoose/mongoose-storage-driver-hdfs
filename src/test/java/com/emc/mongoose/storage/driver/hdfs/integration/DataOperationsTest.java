package com.emc.mongoose.storage.driver.hdfs.integration;

import com.emc.mongoose.data.DataInput;
import com.emc.mongoose.env.Extension;
import com.emc.mongoose.exception.OmgShootMyFootException;
import com.emc.mongoose.item.DataItem;
import com.emc.mongoose.item.DataItemImpl;
import com.emc.mongoose.item.op.OpType;
import com.emc.mongoose.item.op.Operation;
import com.emc.mongoose.item.op.data.DataOperation;
import com.emc.mongoose.item.op.data.DataOperationImpl;
import com.emc.mongoose.storage.Credential;
import com.emc.mongoose.storage.driver.hdfs.HdfsStorageDriver;
import com.emc.mongoose.storage.driver.hdfs.util.docker.StorageNodeContainer;
import com.github.akurilov.commons.collection.Range;
import com.github.akurilov.commons.collection.TreeUtil;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import com.github.akurilov.confuse.impl.BasicConfig;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.emc.mongoose.Constants.APP_NAME;
import static com.emc.mongoose.Constants.MIB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DataOperationsTest
extends HdfsStorageDriver<DataItem, DataOperation<DataItem>> {

	private static final DataInput DATA_INPUT;
	static {
		try {
			DATA_INPUT = DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("4MB"), 16);
		} catch(final IOException e) {
			throw new AssertionError(e);
		}
	}

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

	public DataOperationsTest()
	throws OmgShootMyFootException {
		this(getConfig());
	}

	private DataOperationsTest(final Config config)
	throws OmgShootMyFootException {
		super(
			"hdfs", "test-data-hdfs-driver", DATA_INPUT,
			config.configVal("storage"), true, config.configVal("load").intVal("batch-size")
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
	public final void testCreateFile()
	throws Exception {

		final DataItem dataItem = new DataItemImpl(0, MIB, 0);
		dataItem.name("0000");
		dataItem.dataInput(DATA_INPUT);
		final DataOperation<DataItem> createTask = new DataOperationImpl<>(
			0, OpType.CREATE, dataItem, null, "/default", CREDENTIAL, null, 0, null
		);
		prepareOperation(createTask);
		createTask.status(Operation.Status.ACTIVE);
		while(Operation.Status.ACTIVE.equals(createTask.status())) {
			invokeNio(createTask);
		}
		assertEquals(Operation.Status.SUCC, createTask.status());
		assertEquals(dataItem.size(), createTask.countBytesDone());

		final FileSystem endpoint = getEndpoint("127.0.0.1");
		final FileStatus fileStatus = endpoint.getFileStatus(
			new Path("/default", dataItem.name())
		);
		assertTrue(fileStatus.isFile());
		assertEquals(dataItem.size(), fileStatus.getLen());
	}

	@Test
	public final void testCopyFile()
	throws Exception {

		final DataItem dataItem = new DataItemImpl(0, MIB, 0);
		dataItem.name("1111");
		dataItem.dataInput(DATA_INPUT);
		final DataOperation<DataItem> createTask = new DataOperationImpl<>(
			0, OpType.CREATE, dataItem, null, "/default", CREDENTIAL, null, 0, null
		);
		prepareOperation(createTask);
		createTask.status(Operation.Status.ACTIVE);
		while(Operation.Status.ACTIVE.equals(createTask.status())) {
			invokeNio(createTask);
		}
		assertEquals(Operation.Status.SUCC, createTask.status());

		final DataOperation<DataItem> copyTask = new DataOperationImpl<>(
			0, OpType.CREATE, dataItem, createTask.dstPath(), "/copies", CREDENTIAL,
			null, 0, null
		);
		copyTask.nodeAddr(endpointAddrs[0]);
		copyTask.status(Operation.Status.ACTIVE);
		while(Operation.Status.ACTIVE.equals(copyTask.status())) {
			invokeNio(copyTask);
		}
		assertEquals(Operation.Status.SUCC, copyTask.status());

		final FileSystem endpoint = getEndpoint(endpointAddrs[0]);
		final FileStatus fileStatus = endpoint.getFileStatus(
			new Path("/copies", dataItem.name())
		);
		assertTrue(fileStatus.isFile());
		assertEquals(dataItem.size(), fileStatus.getLen());
	}

	@Test @Ignore
	public final void testConcatFile()
	throws Exception {

		final List<DataItem> srcItems = IntStream
			.range(0, 10)
			.parallel()
			.mapToObj(Integer::toString)
			.map(
				fileName -> {
					final DataItem dataItem = new DataItemImpl(0, MIB, 0);
					dataItem.name(fileName);
					return dataItem;
				}
			)
			.peek(
				dataItem -> {
					dataItem.dataInput(DATA_INPUT);
					final DataOperation<DataItem> createTask = new DataOperationImpl<>(
						0, OpType.CREATE, dataItem, null, "/default", CREDENTIAL, null, 0, null
					);
					try {
						prepareOperation(createTask);
						createTask.status(Operation.Status.ACTIVE);
						while(Operation.Status.ACTIVE.equals(createTask.status())) {
							invokeNio(createTask);
						}
					} catch(final Throwable cause) {
						cause.printStackTrace(System.err);
					}
				}
			)
			.collect(Collectors.toList());

		final DataItem dstItem = new DataItemImpl(0, 0, 0);
		dstItem.name("0010");
		dstItem.dataInput(DATA_INPUT);
		final DataOperation<DataItem> concatTask = new DataOperationImpl<>(
			0, OpType.CREATE, dstItem, null, "/default", CREDENTIAL, null, 0, srcItems
		);
		prepareOperation(concatTask);
		concatTask.status(Operation.Status.ACTIVE);
		while(Operation.Status.ACTIVE.equals(concatTask.status())) {
			invokeNio(concatTask);
		}
		assertEquals(Operation.Status.SUCC, concatTask.status());

		final FileSystem endpoint = getEndpoint(endpointAddrs[0]);
		final FileStatus fileStatus = endpoint.getFileStatus(
			new Path("/default", dstItem.name())
		);
		assertTrue(fileStatus.isFile());
		assertEquals(srcItems.size() * MIB, fileStatus.getLen());
	}

	@Test
	public final void testReadFullFile()
	throws Exception {

		final DataItem dataItem = new DataItemImpl(0, MIB, 0);
		dataItem.name("2222");
		dataItem.dataInput(DATA_INPUT);
		final DataOperation<DataItem> createTask = new DataOperationImpl<>(
			0, OpType.CREATE, dataItem, null, "/default", CREDENTIAL, null, 0, null
		);
		prepareOperation(createTask);
		createTask.status(Operation.Status.ACTIVE);
		while(Operation.Status.ACTIVE.equals(createTask.status())) {
			invokeNio(createTask);
		}
		assertEquals(Operation.Status.SUCC, createTask.status());

		final DataOperation<DataItem> readTask = new DataOperationImpl<>(
			0, OpType.READ, dataItem, createTask.dstPath(), null, CREDENTIAL,
			null, 0, null
		);
		readTask.nodeAddr(endpointAddrs[0]);
		readTask.status(Operation.Status.ACTIVE);
		while(Operation.Status.ACTIVE.equals(readTask.status())) {
			invokeNio(readTask);
		}
		assertEquals(Operation.Status.SUCC, readTask.status());
		assertEquals(dataItem.size(), readTask.countBytesDone());
	}

	@Test
	public final void testReadFixedRangesFile()
	throws Exception {

		final DataItem dataItem = new DataItemImpl(0, MIB, 0);
		dataItem.name("3333");
		dataItem.dataInput(DATA_INPUT);
		final DataOperation<DataItem> createTask = new DataOperationImpl<>(
			0, OpType.CREATE, dataItem, null, "/default", CREDENTIAL, null, 0, null
		);
		prepareOperation(createTask);
		createTask.status(Operation.Status.ACTIVE);
		while(Operation.Status.ACTIVE.equals(createTask.status())) {
			invokeNio(createTask);
		}
		assertEquals(Operation.Status.SUCC, createTask.status());

		final List<Range> fixedRanges = new ArrayList<>();
		fixedRanges.add(new Range(123, 456, -1));
		fixedRanges.add(new Range(789, 1234, -1));
		final DataOperation<DataItem> readTask = new DataOperationImpl<>(
			0, OpType.READ, dataItem, createTask.dstPath(), null, CREDENTIAL,
			fixedRanges, 0, null
		);
		readTask.nodeAddr(endpointAddrs[0]);
		readTask.status(Operation.Status.ACTIVE);
		while(Operation.Status.ACTIVE.equals(readTask.status())) {
			invokeNio(readTask);
		}
		assertEquals(Operation.Status.SUCC, readTask.status());
		assertEquals(456 - 123 + 1 + 1234 - 789 + 1, readTask.countBytesDone());
	}

	@Test
	public final void testReadRandomRangesFile()
	throws Exception {

		final DataItem dataItem = new DataItemImpl(0, MIB, 0);
		dataItem.name("4444");
		dataItem.dataInput(DATA_INPUT);
		final DataOperation<DataItem> createTask = new DataOperationImpl<>(
			0, OpType.CREATE, dataItem, null, "/default", CREDENTIAL, null, 0, null
		);
		prepareOperation(createTask);
		createTask.status(Operation.Status.ACTIVE);
		while(Operation.Status.ACTIVE.equals(createTask.status())) {
			invokeNio(createTask);
		}
		assertEquals(Operation.Status.SUCC, createTask.status());

		final DataOperation<DataItem> readTask = new DataOperationImpl<>(
			0, OpType.READ, dataItem, createTask.dstPath(), null, CREDENTIAL,
			null, 10, null
		);
		readTask.nodeAddr(endpointAddrs[0]);
		readTask.status(Operation.Status.ACTIVE);
		while(Operation.Status.ACTIVE.equals(readTask.status())) {
			invokeNio(readTask);
		}
		assertEquals(Operation.Status.SUCC, readTask.status());
		assertTrue(1023 < readTask.countBytesDone());
		assertTrue(readTask.countBytesDone() < MIB);
	}

	@Test
	public final void testOverwriteFile()
	throws Exception {

		final DataItem dataItem = new DataItemImpl(0, MIB, 0);
		dataItem.name("5555");
		dataItem.dataInput(DATA_INPUT);
		final DataOperation<DataItem> createTask = new DataOperationImpl<>(
			0, OpType.CREATE, dataItem, null, "/default", CREDENTIAL, null, 0, null
		);
		prepareOperation(createTask);
		createTask.status(Operation.Status.ACTIVE);
		while(Operation.Status.ACTIVE.equals(createTask.status())) {
			invokeNio(createTask);
		}
		assertEquals(Operation.Status.SUCC, createTask.status());

		final DataOperation<DataItem> overwriteTask = new DataOperationImpl<>(
			0, OpType.UPDATE, dataItem, createTask.dstPath(), null, CREDENTIAL,
			null, 0, null
		);
		overwriteTask.nodeAddr(endpointAddrs[0]);
		overwriteTask.status(Operation.Status.ACTIVE);
		while(Operation.Status.ACTIVE.equals(overwriteTask.status())) {
			invokeNio(overwriteTask);
		}
		assertEquals(Operation.Status.SUCC, overwriteTask.status());
		assertEquals(MIB, overwriteTask.countBytesDone());
		final FileSystem endpoint = getEndpoint(endpointAddrs[0]);
		final FileStatus fileStatus = endpoint.getFileStatus(
			new Path("/default", dataItem.name())
		);
		assertTrue(fileStatus.isFile());
		assertEquals(MIB, fileStatus.getLen());
	}

	@Test
	public final void testAppendFile()
	throws Exception {

		final DataItem dataItem = new DataItemImpl(0, MIB, 0);
		dataItem.name("6666");
		dataItem.dataInput(DATA_INPUT);
		final DataOperation<DataItem> createTask = new DataOperationImpl<>(
			0, OpType.CREATE, dataItem, null, "/default", CREDENTIAL, null, 0, null
		);
		prepareOperation(createTask);
		createTask.status(Operation.Status.ACTIVE);
		while(Operation.Status.ACTIVE.equals(createTask.status())) {
			invokeNio(createTask);
		}
		assertEquals(Operation.Status.SUCC, createTask.status());

		final DataOperation<DataItem> appendTask = new DataOperationImpl<>(
			0, OpType.UPDATE, dataItem, createTask.dstPath(), null, CREDENTIAL,
			Collections.singletonList(new Range(-1, -1, MIB)), 0, null
		);
		appendTask.nodeAddr(endpointAddrs[0]);
		appendTask.status(Operation.Status.ACTIVE);
		while(Operation.Status.ACTIVE.equals(appendTask.status())) {
			invokeNio(appendTask);
		}
		assertEquals(Operation.Status.SUCC, appendTask.status());
		assertEquals(MIB, appendTask.countBytesDone());
		final FileSystem endpoint = getEndpoint(endpointAddrs[0]);
		final FileStatus fileStatus = endpoint.getFileStatus(
			new Path("/default", dataItem.name())
		);
		assertTrue(fileStatus.isFile());
		assertEquals(2 * MIB, fileStatus.getLen());

	}

	@Test
	public final void testDeleteFile()
	throws Exception {

		final FileSystem endpoint = getEndpoint(endpointAddrs[0]);

		final DataItem dataItem = new DataItemImpl(0, MIB, 0);
		dataItem.name("7777");
		dataItem.dataInput(DATA_INPUT);
		final DataOperation<DataItem> createTask = new DataOperationImpl<>(
			0, OpType.CREATE, dataItem, null, "/default", CREDENTIAL, null, 0, null
		);
		prepareOperation(createTask);
		createTask.status(Operation.Status.ACTIVE);
		while(Operation.Status.ACTIVE.equals(createTask.status())) {
			invokeNio(createTask);
		}
		assertEquals(Operation.Status.SUCC, createTask.status());
		assertTrue(endpoint.exists(new Path("/default", dataItem.name())));

		final DataOperation<DataItem> deleteTask = new DataOperationImpl<>(
			0, OpType.DELETE, dataItem, null, "/default", CREDENTIAL, null, 0, null
		);
		prepareOperation(deleteTask);
		deleteTask.status(Operation.Status.ACTIVE);
		while(Operation.Status.ACTIVE.equals(deleteTask.status())) {
			invokeNio(deleteTask);
		}
		assertEquals(Operation.Status.SUCC, deleteTask.status());
		assertFalse(endpoint.exists(new Path("/default", dataItem.name())));
	}
}
