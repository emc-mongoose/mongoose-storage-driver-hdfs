package com.emc.mongoose.storage.driver.hdfs.system;

import com.emc.mongoose.item.op.OpType;
import com.emc.mongoose.storage.driver.hdfs.util.EnvUtil;
import com.emc.mongoose.storage.driver.hdfs.util.LogAnalyzer;
import com.emc.mongoose.storage.driver.hdfs.util.docker.HdfsNodeContainer;
import com.emc.mongoose.storage.driver.hdfs.util.docker.MongooseContainer;
import com.github.akurilov.commons.system.SizeInBytes;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.math3.stat.Frequency;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

import static com.emc.mongoose.Constants.MIB;
import static com.emc.mongoose.storage.driver.hdfs.util.docker.MongooseContainer.CONTAINER_SHARE_PATH;
import static com.emc.mongoose.storage.driver.hdfs.util.docker.MongooseContainer.HOST_SHARE_PATH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CircularAppendTest {

	private static final String SCENARIO_FILE = "scenario" + "/" + "circular_append.js";
	private static final String ITEM_LIST_FILE_0 = CONTAINER_SHARE_PATH + "/" + "circular_append_items_0.csv";
	private static final String ITEM_LIST_FILE_1 = CONTAINER_SHARE_PATH + "/" + "circular_append_items_1.csv";
	private static final String ITEM_OUTPUT_PATH = "/" + CircularAppendTest.class.getSimpleName();
	private static final String STEP_ID = CircularAppendTest.class.getSimpleName();
	private static final int BASE_ITEMS_COUNT = 10;
	private static final int APPEND_COUNT = 20;
	private static final SizeInBytes ITEM_DATA_SIZE = new SizeInBytes(16 * MIB);
	private static final int CONCURRENCY = 10;

	private static HdfsNodeContainer HDFS_NODE_CONTAINER;
	private static MongooseContainer MONGOOSE_CONTAINER;
	private static String STD_OUTPUT;

	@BeforeClass
	public static void setUpClass()
	throws Exception {

		final String resourceScenarioPath = ReadUsingVariablePathTest.class
			.getClassLoader()
			.getResource(SCENARIO_FILE)
			.getPath();
		final Path hostScenarioPath = Paths.get(HOST_SHARE_PATH.toString(), SCENARIO_FILE);
		Files.createDirectories(hostScenarioPath.getParent());
		if(Files.exists(hostScenarioPath)) {
			Files.delete(hostScenarioPath);
		}
		Files.copy(Paths.get(resourceScenarioPath), hostScenarioPath);
		final List<String> args = new ArrayList<>();
		args.add("--item-output-path=" + ITEM_OUTPUT_PATH);
		args.add("--load-step-id=" + STEP_ID);
		args.add("--storage-driver-limit-concurrency=" + CONCURRENCY);
		args.add("--run-scenario=" + hostScenarioPath);
		EnvUtil.set("BASE_ITEMS_COUNT", Integer.toString(BASE_ITEMS_COUNT));
		EnvUtil.set("APPEND_COUNT", Integer.toString(APPEND_COUNT));
		EnvUtil.set("ITEM_DATA_SIZE", ITEM_DATA_SIZE.toString());
		EnvUtil.set("ITEM_LIST_FILE_0", ITEM_LIST_FILE_0);
		EnvUtil.set("ITEM_LIST_FILE_1", ITEM_LIST_FILE_1);

		try {
			HDFS_NODE_CONTAINER = new HdfsNodeContainer();
			MONGOOSE_CONTAINER = new MongooseContainer(args, 1000);
		} catch(final Exception e) {
			throw new AssertionError(e);
		}
		MONGOOSE_CONTAINER.clearLogs(STEP_ID);
		MONGOOSE_CONTAINER.run();
		STD_OUTPUT = MONGOOSE_CONTAINER.getStdOutput();
	}

	@AfterClass
	public static void tearDownClass()
	throws Exception {
		HDFS_NODE_CONTAINER.close();
		MONGOOSE_CONTAINER.close();
	}

	@Test
	public void testMetricsLogRecords()
	throws Exception {
		final List<CSVRecord> metricsLogRecords = LogAnalyzer.getMetricsLogRecords(STEP_ID);
		assertTrue(
			"There should be more than 0 metrics records in the log file",
			metricsLogRecords.size() > 0
		);
		LogAnalyzer.testMetricsLogRecords(
			metricsLogRecords, OpType.UPDATE, CONCURRENCY, 1, ITEM_DATA_SIZE,
			BASE_ITEMS_COUNT * APPEND_COUNT, 0, 10
		);
	}

	@Test
	public void testTotalMetricsLogRecords()
	throws Exception {
		final List<CSVRecord> totalMetrcisLogRecords = LogAnalyzer.getMetricsTotalLogRecords(
			STEP_ID
		);
		assertEquals(
			"There should be 1 total metrics records in the log file", 1,
			totalMetrcisLogRecords.size()
		);
		LogAnalyzer.testTotalMetricsLogRecord(
			totalMetrcisLogRecords.get(0), OpType.UPDATE, CONCURRENCY, 1, ITEM_DATA_SIZE, 0, 0
		);
	}

	@Test
	public void testSingleMetricsStdout()
	throws Exception {
		LogAnalyzer.testSingleMetricsStdout(
			STD_OUTPUT.replaceAll("[\r\n]+", " "), OpType.UPDATE, CONCURRENCY, 1, ITEM_DATA_SIZE, 10
		);
	}

	@Test
	public void testOpTraceLogRecords()
	throws Exception {
		final LongAdder opTraceRecCount = new LongAdder();
		final Consumer<CSVRecord> ioTraceReqTestFunc = ioTraceRec -> {
			LogAnalyzer.testOpTraceRecord(ioTraceRec, OpType.UPDATE.ordinal(), ITEM_DATA_SIZE);
			opTraceRecCount.increment();
		};
		LogAnalyzer.testOpTraceLogRecords(STEP_ID, ioTraceReqTestFunc);
		assertTrue(
			"There should be more than " + BASE_ITEMS_COUNT +
				" records in the I/O trace log file, but got: " + opTraceRecCount.sum(),
			BASE_ITEMS_COUNT < opTraceRecCount.sum()
		);
	}

	@Test
	public void testItemOutputFile()
	throws Exception {
		final List<CSVRecord> items = new ArrayList<>();
		final String hostItemListFile = ITEM_LIST_FILE_1.replace(
			CONTAINER_SHARE_PATH, HOST_SHARE_PATH.toString()
		);
		try(final BufferedReader br = new BufferedReader(new FileReader(hostItemListFile))) {
			final CSVParser csvParser = CSVFormat.RFC4180.parse(br);
			for(final CSVRecord csvRecord : csvParser) {
				items.add(csvRecord);
			}
		}
		final int itemIdRadix = Character.MAX_RADIX;
		final Frequency freq = new Frequency();
		String itemPath, itemId;
		long itemOffset;
		long size;
		final SizeInBytes expectedFinalSize = new SizeInBytes(
			(APPEND_COUNT + 1) * ITEM_DATA_SIZE.get() / 3,
			3 * (APPEND_COUNT + 1) * ITEM_DATA_SIZE.get(),
			1
		);
		final int n = items.size();
		CSVRecord itemRec;
		for(int i = 0; i < n; i ++) {
			itemRec = items.get(i);
			itemPath = itemRec.get(0);
			for(int j = i; j < n; j ++) {
				if(i != j) {
					assertFalse(itemPath.equals(items.get(j).get(0)));
				}
			}
			itemId = itemPath.substring(itemPath.lastIndexOf('/') + 1);

			// NOTE: won't work for Atmos storage
			itemOffset = Long.parseLong(itemRec.get(1), 0x10);
			assertEquals(Long.parseLong(itemId, itemIdRadix), itemOffset);
			freq.addValue(itemOffset);

			size = Long.parseLong(itemRec.get(2));
			assertTrue(
				"Expected size: " + expectedFinalSize.toString() + ", actual: " + size,
				expectedFinalSize.getMin() <= size && size <= expectedFinalSize.getMax()
			);
			assertEquals("0/0", itemRec.get(3));
		}

		// NOTE: won't work for Atmos storage
		assertEquals(BASE_ITEMS_COUNT, freq.getUniqueCount(), BASE_ITEMS_COUNT / 20);
	}
}
