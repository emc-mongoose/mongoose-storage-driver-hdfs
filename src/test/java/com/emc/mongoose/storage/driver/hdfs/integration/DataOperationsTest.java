package com.emc.mongoose.storage.driver.hdfs.integration;

import com.emc.mongoose.api.common.exception.OmgShootMyFootException;
import com.emc.mongoose.api.model.data.DataInput;
import com.emc.mongoose.api.model.io.IoType;
import com.emc.mongoose.api.model.io.task.IoTask;
import com.emc.mongoose.api.model.io.task.data.BasicDataIoTask;
import com.emc.mongoose.api.model.io.task.data.DataIoTask;
import com.emc.mongoose.api.model.item.BasicDataItem;
import com.emc.mongoose.api.model.item.DataItem;
import com.emc.mongoose.api.model.storage.Credential;
import com.emc.mongoose.storage.driver.hdfs.HdfsStorageDriver;
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

import com.github.akurilov.commons.collection.Range;
import com.github.akurilov.commons.system.SizeInBytes;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.emc.mongoose.api.common.Constants.MIB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DataOperationsTest
extends HdfsStorageDriver<DataItem, DataIoTask<DataItem>> {

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

	public DataOperationsTest()
	throws OmgShootMyFootException {
		this(getConfig());
	}

	private DataOperationsTest(final Config config)
	throws OmgShootMyFootException {
		super(
			"test-data-hdfs-driver", DATA_INPUT, config.getLoadConfig(), config.getStorageConfig(),
			true
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
	public final void testCreateFile()
	throws Exception {

		final DataItem dataItem = new BasicDataItem(0, MIB, 0);
		dataItem.setName("0000");
		dataItem.setDataInput(DATA_INPUT);
		final DataIoTask<DataItem> createTask = new BasicDataIoTask<>(
			0, IoType.CREATE, dataItem, null, "/default", CREDENTIAL, null, 0, null
		);
		prepareIoTask(createTask);
		createTask.setStatus(IoTask.Status.ACTIVE);
		while(IoTask.Status.ACTIVE.equals(createTask.getStatus())) {
			invokeNio(createTask);
		}
		assertEquals(IoTask.Status.SUCC, createTask.getStatus());
		assertEquals(dataItem.size(), createTask.getCountBytesDone());

		final FileSystem endpoint = endpoints.values().iterator().next();
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
		dataItem.setDataInput(DATA_INPUT);
		final DataIoTask<DataItem> createTask = new BasicDataIoTask<>(
			0, IoType.CREATE, dataItem, null, "/default", CREDENTIAL, null, 0, null
		);
		prepareIoTask(createTask);
		createTask.setStatus(IoTask.Status.ACTIVE);
		while(IoTask.Status.ACTIVE.equals(createTask.getStatus())) {
			invokeNio(createTask);
		}
		assertEquals(IoTask.Status.SUCC, createTask.getStatus());

		final DataIoTask<DataItem> copyTask = new BasicDataIoTask<>(
			0, IoType.CREATE, dataItem, createTask.getDstPath(), "/copies", CREDENTIAL,
			null, 0, null
		);
		copyTask.setNodeAddr(endpoints.keySet().iterator().next());
		copyTask.setStatus(IoTask.Status.ACTIVE);
		while(IoTask.Status.ACTIVE.equals(copyTask.getStatus())) {
			invokeNio(copyTask);
		}
		assertEquals(IoTask.Status.SUCC, copyTask.getStatus());

		final FileSystem endpoint = endpoints.values().iterator().next();
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
					dataItem.setDataInput(DATA_INPUT);
					final DataIoTask<DataItem> createTask = new BasicDataIoTask<>(
						0, IoType.CREATE, dataItem, null, "/default", CREDENTIAL, null, 0, null
					);
					try {
						prepareIoTask(createTask);
						createTask.setStatus(IoTask.Status.ACTIVE);
						while(IoTask.Status.ACTIVE.equals(createTask.getStatus())) {
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
		dstItem.setDataInput(DATA_INPUT);
		final DataIoTask<DataItem> concatTask = new BasicDataIoTask<>(
			0, IoType.CREATE, dstItem, null, "/default", CREDENTIAL, null, 0, srcItems
		);
		prepareIoTask(concatTask);
		concatTask.setStatus(IoTask.Status.ACTIVE);
		while(IoTask.Status.ACTIVE.equals(concatTask.getStatus())) {
			invokeNio(concatTask);
		}
		assertEquals(IoTask.Status.SUCC, concatTask.getStatus());

		final FileSystem endpoint = endpoints.values().iterator().next();
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
		dataItem.setDataInput(DATA_INPUT);
		final DataIoTask<DataItem> createTask = new BasicDataIoTask<>(
			0, IoType.CREATE, dataItem, null, "/default", CREDENTIAL, null, 0, null
		);
		prepareIoTask(createTask);
		createTask.setStatus(IoTask.Status.ACTIVE);
		while(IoTask.Status.ACTIVE.equals(createTask.getStatus())) {
			invokeNio(createTask);
		}
		assertEquals(IoTask.Status.SUCC, createTask.getStatus());

		final DataIoTask<DataItem> readTask = new BasicDataIoTask<>(
			0, IoType.READ, dataItem, createTask.getDstPath(), null, CREDENTIAL,
			null, 0, null
		);
		readTask.setNodeAddr(endpoints.keySet().iterator().next());
		readTask.setStatus(IoTask.Status.ACTIVE);
		while(IoTask.Status.ACTIVE.equals(readTask.getStatus())) {
			invokeNio(readTask);
		}
		assertEquals(IoTask.Status.SUCC, readTask.getStatus());
		assertEquals(dataItem.size(), readTask.getCountBytesDone());
	}

	@Test
	public final void testReadFixedRangesFile()
	throws Exception {

		final DataItem dataItem = new BasicDataItem(0, MIB, 0);
		dataItem.setName("3333");
		dataItem.setDataInput(DATA_INPUT);
		final DataIoTask<DataItem> createTask = new BasicDataIoTask<>(
			0, IoType.CREATE, dataItem, null, "/default", CREDENTIAL, null, 0, null
		);
		prepareIoTask(createTask);
		createTask.setStatus(IoTask.Status.ACTIVE);
		while(IoTask.Status.ACTIVE.equals(createTask.getStatus())) {
			invokeNio(createTask);
		}
		assertEquals(IoTask.Status.SUCC, createTask.getStatus());

		final List<Range> fixedRanges = new ArrayList<>();
		fixedRanges.add(new Range(123, 456, -1));
		fixedRanges.add(new Range(789, 1234, -1));
		final DataIoTask<DataItem> readTask = new BasicDataIoTask<>(
			0, IoType.READ, dataItem, createTask.getDstPath(), null, CREDENTIAL,
			fixedRanges, 0, null
		);
		readTask.setNodeAddr(endpoints.keySet().iterator().next());
		readTask.setStatus(IoTask.Status.ACTIVE);
		while(IoTask.Status.ACTIVE.equals(readTask.getStatus())) {
			invokeNio(readTask);
		}
		assertEquals(IoTask.Status.SUCC, readTask.getStatus());
		assertEquals(456 - 123 + 1 + 1234 - 789 + 1, readTask.getCountBytesDone());
	}

	@Test
	public final void testReadRandomRangesFile()
	throws Exception {

		final DataItem dataItem = new BasicDataItem(0, MIB, 0);
		dataItem.setName("4444");
		dataItem.setDataInput(DATA_INPUT);
		final DataIoTask<DataItem> createTask = new BasicDataIoTask<>(
			0, IoType.CREATE, dataItem, null, "/default", CREDENTIAL, null, 0, null
		);
		prepareIoTask(createTask);
		createTask.setStatus(IoTask.Status.ACTIVE);
		while(IoTask.Status.ACTIVE.equals(createTask.getStatus())) {
			invokeNio(createTask);
		}
		assertEquals(IoTask.Status.SUCC, createTask.getStatus());

		final DataIoTask<DataItem> readTask = new BasicDataIoTask<>(
			0, IoType.READ, dataItem, createTask.getDstPath(), null, CREDENTIAL,
			null, 10, null
		);
		readTask.setNodeAddr(endpoints.keySet().iterator().next());
		readTask.setStatus(IoTask.Status.ACTIVE);
		while(IoTask.Status.ACTIVE.equals(readTask.getStatus())) {
			invokeNio(readTask);
		}
		assertEquals(IoTask.Status.SUCC, readTask.getStatus());
		assertTrue(1023 < readTask.getCountBytesDone());
		assertTrue(readTask.getCountBytesDone() < MIB);
	}

	@Test
	public final void testOverwriteFile()
	throws Exception {

		final DataItem dataItem = new BasicDataItem(0, MIB, 0);
		dataItem.setName("5555");
		dataItem.setDataInput(DATA_INPUT);
		final DataIoTask<DataItem> createTask = new BasicDataIoTask<>(
			0, IoType.CREATE, dataItem, null, "/default", CREDENTIAL, null, 0, null
		);
		prepareIoTask(createTask);
		createTask.setStatus(IoTask.Status.ACTIVE);
		while(IoTask.Status.ACTIVE.equals(createTask.getStatus())) {
			invokeNio(createTask);
		}
		assertEquals(IoTask.Status.SUCC, createTask.getStatus());

		final DataIoTask<DataItem> overwriteTask = new BasicDataIoTask<>(
			0, IoType.UPDATE, dataItem, createTask.getDstPath(), null, CREDENTIAL,
			null, 0, null
		);
		overwriteTask.setNodeAddr(endpoints.keySet().iterator().next());
		overwriteTask.setStatus(IoTask.Status.ACTIVE);
		while(IoTask.Status.ACTIVE.equals(overwriteTask.getStatus())) {
			invokeNio(overwriteTask);
		}
		assertEquals(IoTask.Status.SUCC, overwriteTask.getStatus());
		assertEquals(MIB, overwriteTask.getCountBytesDone());
		final FileSystem endpoint = endpoints.values().iterator().next();
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
		dataItem.setDataInput(DATA_INPUT);
		final DataIoTask<DataItem> createTask = new BasicDataIoTask<>(
			0, IoType.CREATE, dataItem, null, "/default", CREDENTIAL, null, 0, null
		);
		prepareIoTask(createTask);
		createTask.setStatus(IoTask.Status.ACTIVE);
		while(IoTask.Status.ACTIVE.equals(createTask.getStatus())) {
			invokeNio(createTask);
		}
		assertEquals(IoTask.Status.SUCC, createTask.getStatus());

		final DataIoTask<DataItem> appendTask = new BasicDataIoTask<>(
			0, IoType.UPDATE, dataItem, createTask.getDstPath(), null, CREDENTIAL,
			Collections.singletonList(new Range(-1, -1, MIB)), 0, null
		);
		appendTask.setNodeAddr(endpoints.keySet().iterator().next());
		appendTask.setStatus(IoTask.Status.ACTIVE);
		while(IoTask.Status.ACTIVE.equals(appendTask.getStatus())) {
			invokeNio(appendTask);
		}
		assertEquals(IoTask.Status.SUCC, appendTask.getStatus());
		assertEquals(MIB, appendTask.getCountBytesDone());
		final FileSystem endpoint = endpoints.values().iterator().next();
		final FileStatus fileStatus = endpoint.getFileStatus(
			new Path("/default", dataItem.getName())
		);
		assertTrue(fileStatus.isFile());
		assertEquals(2 * MIB, fileStatus.getLen());

	}

	@Test
	public final void testDeleteFile()
	throws Exception {

		final FileSystem endpoint = endpoints.values().iterator().next();

		final DataItem dataItem = new BasicDataItem(0, MIB, 0);
		dataItem.setName("7777");
		dataItem.setDataInput(DATA_INPUT);
		final DataIoTask<DataItem> createTask = new BasicDataIoTask<>(
			0, IoType.CREATE, dataItem, null, "/default", CREDENTIAL, null, 0, null
		);
		prepareIoTask(createTask);
		createTask.setStatus(IoTask.Status.ACTIVE);
		while(IoTask.Status.ACTIVE.equals(createTask.getStatus())) {
			invokeNio(createTask);
		}
		assertEquals(IoTask.Status.SUCC, createTask.getStatus());
		assertTrue(endpoint.exists(new Path("/default", dataItem.getName())));

		final DataIoTask<DataItem> deleteTask = new BasicDataIoTask<>(
			0, IoType.DELETE, dataItem, null, "/default", CREDENTIAL, null, 0, null
		);
		prepareIoTask(deleteTask);
		deleteTask.setStatus(IoTask.Status.ACTIVE);
		while(IoTask.Status.ACTIVE.equals(deleteTask.getStatus())) {
			invokeNio(deleteTask);
		}
		assertEquals(IoTask.Status.SUCC, deleteTask.getStatus());
		assertFalse(endpoint.exists(new Path("/default", dataItem.getName())));
	}
}
