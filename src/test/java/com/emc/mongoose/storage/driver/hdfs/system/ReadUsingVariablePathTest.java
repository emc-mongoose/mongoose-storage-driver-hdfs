package com.emc.mongoose.storage.driver.hdfs.system;

import com.emc.mongoose.api.model.io.IoType;
import com.emc.mongoose.storage.driver.hdfs.util.EnvUtil;
import com.emc.mongoose.storage.driver.hdfs.util.LogAnalyzer;
import com.emc.mongoose.storage.driver.hdfs.util.docker.HdfsNodeContainer;
import com.emc.mongoose.storage.driver.hdfs.util.docker.MongooseContainer;
import static com.emc.mongoose.api.common.Constants.MIB;
import static com.emc.mongoose.storage.driver.hdfs.util.docker.MongooseContainer.CONTAINER_SHARE_PATH;
import static com.emc.mongoose.storage.driver.hdfs.util.docker.MongooseContainer.HOST_SHARE_PATH;

import com.github.akurilov.commons.system.SizeInBytes;

import org.apache.commons.csv.CSVRecord;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ReadUsingVariablePathTest {

	private static final String SCENARIO_FILE = "scenario" + File.separator
		+ "read_using_variable_path.js";
	private static final String ITEM_LIST_FILE = CONTAINER_SHARE_PATH + File.separator
		+ "read_using_variable_path_items.csv";
	private static final String ITEM_OUTPUT_PATH = "/"
		+ ReadUsingVariablePathTest.class.getSimpleName();
	private static final String STEP_ID = ReadUsingVariablePathTest.class.getSimpleName();
	private static final int STEP_LIMIT_COUNT = 10000;
	private static final SizeInBytes ITEM_DATA_SIZE = new SizeInBytes(MIB);
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
		args.add("--test-step-id=" + STEP_ID);
		args.add("--test-scenario-file=" + hostScenarioPath);
		args.add("--item-naming-radix=16");
		args.add("--item-naming-length=16");
		args.add("--load-limit-concurrency=" + CONCURRENCY);
		EnvUtil.set("ITEM_LIST_FILE", ITEM_LIST_FILE);
		EnvUtil.set("ITEM_DATA_SIZE", ITEM_DATA_SIZE.toString());
		EnvUtil.set("ITEM_OUTPUT_PATH", ITEM_OUTPUT_PATH);
		EnvUtil.set("STEP_LIMIT_COUNT", Integer.toString(STEP_LIMIT_COUNT));

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
	public void testIoTraceLogRecords()
	throws Exception {
		final LongAdder ioTraceRecCount = new LongAdder();
		final int baseOutputPathLen = ITEM_OUTPUT_PATH.length();
		// Item path should look like:
		// ${FILE_OUTPUT_PATH}/1/b/0123456789abcdef
		// ${FILE_OUTPUT_PATH}/b/fedcba9876543210
		final Pattern subPathPtrn = Pattern.compile("(/[0-9a-f]){1,2}/[0-9a-f]{16}");
		final Consumer<CSVRecord> ioTraceReqTestFunc = ioTraceRec -> {
			LogAnalyzer.testIoTraceRecord(ioTraceRec, IoType.READ.ordinal(), ITEM_DATA_SIZE);
			String nextFilePath = ioTraceRec.get("ItemPath");
			assertTrue(nextFilePath.startsWith(ITEM_OUTPUT_PATH));
			nextFilePath = nextFilePath.substring(baseOutputPathLen);
			final Matcher m = subPathPtrn.matcher(nextFilePath);
			assertTrue("\"" + nextFilePath + "\" doesn't match the pattern" , m.matches());
			ioTraceRecCount.increment();
		};
		LogAnalyzer.testIoTraceLogRecords(STEP_ID, ioTraceReqTestFunc);
		assertEquals(
			"There should be more than 1 record in the I/O trace log file",
			STEP_LIMIT_COUNT, ioTraceRecCount.sum()
		);
	}

	@Test
	public void testMetricsLogRecords()
	throws Exception {
		LogAnalyzer.testMetricsLogRecords(
			LogAnalyzer.getMetricsLogRecords(STEP_ID), IoType.READ, CONCURRENCY, 1, ITEM_DATA_SIZE,
			STEP_LIMIT_COUNT, 0, 10
		);
	}

	@Test
	public void testTotalMetricsLogRecord()
	throws Exception {
		LogAnalyzer.testTotalMetricsLogRecord(
			LogAnalyzer.getMetricsTotalLogRecords(STEP_ID).get(0), IoType.READ, CONCURRENCY, 1,
			ITEM_DATA_SIZE, STEP_LIMIT_COUNT, 0
		);
	}

	@Test
	public void testMetricsStdout()
	throws Exception {
		LogAnalyzer.testSingleMetricsStdout(
			STD_OUTPUT.replaceAll("[\r\n]+", " "), IoType.READ, CONCURRENCY, 1, ITEM_DATA_SIZE, 10
		);
	}

	@Test
	public void testFinalMetricsTableRowStdout()
	throws Exception {
		LogAnalyzer.testFinalMetricsTableRowStdout(
			STD_OUTPUT, STEP_ID, IoType.CREATE, 1, CONCURRENCY, STEP_LIMIT_COUNT, 0, ITEM_DATA_SIZE
		);
	}
}