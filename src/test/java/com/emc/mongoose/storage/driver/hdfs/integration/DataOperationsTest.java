package com.emc.mongoose.storage.driver.hdfs.integration;

import com.emc.mongoose.data.DataInput;
import com.emc.mongoose.env.Extension;
import com.emc.mongoose.exception.OmgShootMyFootException;
import com.emc.mongoose.item.BasicDataItem;
import com.emc.mongoose.item.DataItem;
import com.emc.mongoose.item.io.IoType;
import com.emc.mongoose.item.io.task.IoTask;
import com.emc.mongoose.item.io.task.data.BasicDataIoTask;
import com.emc.mongoose.item.io.task.data.DataIoTask;
import com.emc.mongoose.storage.Credential;
import com.emc.mongoose.storage.driver.hdfs.HdfsStorageDriver;
import com.emc.mongoose.storage.driver.hdfs.util.docker.HdfsNodeContainer;

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
extends HdfsStorageDriver<DataItem, DataIoTask<DataItem>> {

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

	public DataOperationsTest()
	throws OmgShootMyFootException {
		this(getConfig());
	}

	private DataOperationsTest(final Config config)
	throws OmgShootMyFootException {
		super(
			"hdfs", "test-data-hdfs-driver", DATA_INPUT, config.configVal("load"),
			config.configVal("storage"), true
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
	public final void testCreateFile()
	throws Exception {

		final DataItem dataItem = new BasicDataItem(0, MIB, 0);
		dataItem.setName("0000");
		dataItem.dataInput(DATA_INPUT);
		final DataIoTask<DataItem> createTask = new BasicDataIoTask<>(
			0, IoType.CREATE, dataItem, null, "/default", CREDENTIAL, null, 0, null
		);
		prepareIoTask(createTask);
		createTask.status(IoTask.Status.ACTIVE);
		while(IoTask.Status.ACTIVE.equals(createTask.status())) {
			invokeNio(createTask);
		}
		assertEquals(IoTask.Status.SUCC, createTask.status());
		assertEquals(dataItem.size(), createTask.countBytesDone());

		final FileSystem endpoint = getEndpoint("127.0.0.1");
		final FileStatus fileStatus = endpoint.getFileStatus(
			new Path("/default", dataItem.getName())
		);
		assertTrue(fileStatus.isFile());
		assertEquals(dataItem.size(), fileStatus.getLen());
	}

	@Test
	public final void testCopyFile()
	throws Exception {

		final DataItem dataItem = new BasicDataItem(0, MIB, 0);
		dataItem.setName("1111");
		dataItem.dataInput(DATA_INPUT);
		final DataIoTask<DataItem> createTask = new BasicDataIoTask<>(
			0, IoType.CREATE, dataItem, null, "/default", CREDENTIAL, null, 0, null
		);
		prepareIoTask(createTask);
		createTask.status(IoTask.Status.ACTIVE);
		while(IoTask.Status.ACTIVE.equals(createTask.status())) {
			invokeNio(createTask);
		}
		assertEquals(IoTask.Status.SUCC, createTask.status());

		final DataIoTask<DataItem> copyTask = new BasicDataIoTask<>(
			0, IoType.CREATE, dataItem, createTask.dstPath(), "/copies", CREDENTIAL,
			null, 0, null
		);
		copyTask.nodeAddr(endpointAddrs[0]);
		copyTask.status(IoTask.Status.ACTIVE);
		while(IoTask.Status.ACTIVE.equals(copyTask.status())) {
			invokeNio(copyTask);
		}
		assertEquals(IoTask.Status.SUCC, copyTask.status());

		final FileSystem endpoint = getEndpoint(endpointAddrs[0]);
		final FileStatus fileStatus = endpoint.getFileStatus(
			new Path("/copies", dataItem.getName())
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
					final DataItem dataItem = new BasicDataItem(0, MIB, 0);
					dataItem.setName(fileName);
					return dataItem;
				}
			)
			.peek(
				dataItem -> {
					dataItem.dataInput(DATA_INPUT);
					final DataIoTask<DataItem> createTask = new BasicDataIoTask<>(
						0, IoType.CREATE, dataItem, null, "/default", CREDENTIAL, null, 0, null
					);
					try {
						prepareIoTask(createTask);
						createTask.status(IoTask.Status.ACTIVE);
						while(IoTask.Status.ACTIVE.equals(createTask.status())) {
							invokeNio(createTask);
						}
					} catch(final Throwable cause) {
						cause.printStackTrace(System.err);
					}
				}
			)
			.collect(Collectors.toList());

		final DataItem dstItem = new BasicDataItem(0, 0, 0);
		dstItem.setName("0010");
		dstItem.dataInput(DATA_INPUT);
		final DataIoTask<DataItem> concatTask = new BasicDataIoTask<>(
			0, IoType.CREATE, dstItem, null, "/default", CREDENTIAL, null, 0, srcItems
		);
		prepareIoTask(concatTask);
		concatTask.status(IoTask.Status.ACTIVE);
		while(IoTask.Status.ACTIVE.equals(concatTask.status())) {
			invokeNio(concatTask);
		}
		assertEquals(IoTask.Status.SUCC, concatTask.status());

		final FileSystem endpoint = getEndpoint(endpointAddrs[0]);
		final FileStatus fileStatus = endpoint.getFileStatus(
			new Path("/default", dstItem.getName())
		);
		assertTrue(fileStatus.isFile());
		assertEquals(srcItems.size() * MIB, fileStatus.getLen());
	}

	@Test
	public final void testReadFullFile()
	throws Exception {

		final DataItem dataItem = new BasicDataItem(0, MIB, 0);
		dataItem.setName("2222");
		dataItem.dataInput(DATA_INPUT);
		final DataIoTask<DataItem> createTask = new BasicDataIoTask<>(
			0, IoType.CREATE, dataItem, null, "/default", CREDENTIAL, null, 0, null
		);
		prepareIoTask(createTask);
		createTask.status(IoTask.Status.ACTIVE);
		while(IoTask.Status.ACTIVE.equals(createTask.status())) {
			invokeNio(createTask);
		}
		assertEquals(IoTask.Status.SUCC, createTask.status());

		final DataIoTask<DataItem> readTask = new BasicDataIoTask<>(
			0, IoType.READ, dataItem, createTask.dstPath(), null, CREDENTIAL,
			null, 0, null
		);
		readTask.nodeAddr(endpointAddrs[0]);
		readTask.status(IoTask.Status.ACTIVE);
		while(IoTask.Status.ACTIVE.equals(readTask.status())) {
			invokeNio(readTask);
		}
		assertEquals(IoTask.Status.SUCC, readTask.status());
		assertEquals(dataItem.size(), readTask.countBytesDone());
	}

	@Test
	public final void testReadFixedRangesFile()
	throws Exception {

		final DataItem dataItem = new BasicDataItem(0, MIB, 0);
		dataItem.setName("3333");
		dataItem.dataInput(DATA_INPUT);
		final DataIoTask<DataItem> createTask = new BasicDataIoTask<>(
			0, IoType.CREATE, dataItem, null, "/default", CREDENTIAL, null, 0, null
		);
		prepareIoTask(createTask);
		createTask.status(IoTask.Status.ACTIVE);
		while(IoTask.Status.ACTIVE.equals(createTask.status())) {
			invokeNio(createTask);
		}
		assertEquals(IoTask.Status.SUCC, createTask.status());

		final List<Range> fixedRanges = new ArrayList<>();
		fixedRanges.add(new Range(123, 456, -1));
		fixedRanges.add(new Range(789, 1234, -1));
		final DataIoTask<DataItem> readTask = new BasicDataIoTask<>(
			0, IoType.READ, dataItem, createTask.dstPath(), null, CREDENTIAL,
			fixedRanges, 0, null
		);
		readTask.nodeAddr(endpointAddrs[0]);
		readTask.status(IoTask.Status.ACTIVE);
		while(IoTask.Status.ACTIVE.equals(readTask.status())) {
			invokeNio(readTask);
		}
		assertEquals(IoTask.Status.SUCC, readTask.status());
		assertEquals(456 - 123 + 1 + 1234 - 789 + 1, readTask.countBytesDone());
	}

	@Test
	public final void testReadRandomRangesFile()
	throws Exception {

		final DataItem dataItem = new BasicDataItem(0, MIB, 0);
		dataItem.setName("4444");
		dataItem.dataInput(DATA_INPUT);
		final DataIoTask<DataItem> createTask = new BasicDataIoTask<>(
			0, IoType.CREATE, dataItem, null, "/default", CREDENTIAL, null, 0, null
		);
		prepareIoTask(createTask);
		createTask.status(IoTask.Status.ACTIVE);
		while(IoTask.Status.ACTIVE.equals(createTask.status())) {
			invokeNio(createTask);
		}
		assertEquals(IoTask.Status.SUCC, createTask.status());

		final DataIoTask<DataItem> readTask = new BasicDataIoTask<>(
			0, IoType.READ, dataItem, createTask.dstPath(), null, CREDENTIAL,
			null, 10, null
		);
		readTask.nodeAddr(endpointAddrs[0]);
		readTask.status(IoTask.Status.ACTIVE);
		while(IoTask.Status.ACTIVE.equals(readTask.status())) {
			invokeNio(readTask);
		}
		assertEquals(IoTask.Status.SUCC, readTask.status());
		assertTrue(1023 < readTask.countBytesDone());
		assertTrue(readTask.countBytesDone() < MIB);
	}

	@Test
	public final void testOverwriteFile()
	throws Exception {

		final DataItem dataItem = new BasicDataItem(0, MIB, 0);
		dataItem.setName("5555");
		dataItem.dataInput(DATA_INPUT);
		final DataIoTask<DataItem> createTask = new BasicDataIoTask<>(
			0, IoType.CREATE, dataItem, null, "/default", CREDENTIAL, null, 0, null
		);
		prepareIoTask(createTask);
		createTask.status(IoTask.Status.ACTIVE);
		while(IoTask.Status.ACTIVE.equals(createTask.status())) {
			invokeNio(createTask);
		}
		assertEquals(IoTask.Status.SUCC, createTask.status());

		final DataIoTask<DataItem> overwriteTask = new BasicDataIoTask<>(
			0, IoType.UPDATE, dataItem, createTask.dstPath(), null, CREDENTIAL,
			null, 0, null
		);
		overwriteTask.nodeAddr(endpointAddrs[0]);
		overwriteTask.status(IoTask.Status.ACTIVE);
		while(IoTask.Status.ACTIVE.equals(overwriteTask.status())) {
			invokeNio(overwriteTask);
		}
		assertEquals(IoTask.Status.SUCC, overwriteTask.status());
		assertEquals(MIB, overwriteTask.countBytesDone());
		final FileSystem endpoint = getEndpoint(endpointAddrs[0]);
		final FileStatus fileStatus = endpoint.getFileStatus(
			new Path("/default", dataItem.getName())
		);
		assertTrue(fileStatus.isFile());
		assertEquals(MIB, fileStatus.getLen());
	}

	@Test
	public final void testAppendFile()
	throws Exception {

		final DataItem dataItem = new BasicDataItem(0, MIB, 0);
		dataItem.setName("6666");
		dataItem.dataInput(DATA_INPUT);
		final DataIoTask<DataItem> createTask = new BasicDataIoTask<>(
			0, IoType.CREATE, dataItem, null, "/default", CREDENTIAL, null, 0, null
		);
		prepareIoTask(createTask);
		createTask.status(IoTask.Status.ACTIVE);
		while(IoTask.Status.ACTIVE.equals(createTask.status())) {
			invokeNio(createTask);
		}
		assertEquals(IoTask.Status.SUCC, createTask.status());

		final DataIoTask<DataItem> appendTask = new BasicDataIoTask<>(
			0, IoType.UPDATE, dataItem, createTask.dstPath(), null, CREDENTIAL,
			Collections.singletonList(new Range(-1, -1, MIB)), 0, null
		);
		appendTask.nodeAddr(endpointAddrs[0]);
		appendTask.status(IoTask.Status.ACTIVE);
		while(IoTask.Status.ACTIVE.equals(appendTask.status())) {
			invokeNio(appendTask);
		}
		assertEquals(IoTask.Status.SUCC, appendTask.status());
		assertEquals(MIB, appendTask.countBytesDone());
		final FileSystem endpoint = getEndpoint(endpointAddrs[0]);
		final FileStatus fileStatus = endpoint.getFileStatus(
			new Path("/default", dataItem.getName())
		);
		assertTrue(fileStatus.isFile());
		assertEquals(2 * MIB, fileStatus.getLen());

	}

	@Test
	public final void testDeleteFile()
	throws Exception {

		final FileSystem endpoint = getEndpoint(endpointAddrs[0]);

		final DataItem dataItem = new BasicDataItem(0, MIB, 0);
		dataItem.setName("7777");
		dataItem.dataInput(DATA_INPUT);
		final DataIoTask<DataItem> createTask = new BasicDataIoTask<>(
			0, IoType.CREATE, dataItem, null, "/default", CREDENTIAL, null, 0, null
		);
		prepareIoTask(createTask);
		createTask.status(IoTask.Status.ACTIVE);
		while(IoTask.Status.ACTIVE.equals(createTask.status())) {
			invokeNio(createTask);
		}
		assertEquals(IoTask.Status.SUCC, createTask.status());
		assertTrue(endpoint.exists(new Path("/default", dataItem.getName())));

		final DataIoTask<DataItem> deleteTask = new BasicDataIoTask<>(
			0, IoType.DELETE, dataItem, null, "/default", CREDENTIAL, null, 0, null
		);
		prepareIoTask(deleteTask);
		deleteTask.status(IoTask.Status.ACTIVE);
		while(IoTask.Status.ACTIVE.equals(deleteTask.status())) {
			invokeNio(deleteTask);
		}
		assertEquals(IoTask.Status.SUCC, deleteTask.status());
		assertFalse(endpoint.exists(new Path("/default", dataItem.getName())));
	}
}
