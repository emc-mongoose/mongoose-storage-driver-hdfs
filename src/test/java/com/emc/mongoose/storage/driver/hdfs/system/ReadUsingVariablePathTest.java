package com.emc.mongoose.storage.driver.hdfs.system;

import com.emc.mongoose.item.op.OpType;
import com.emc.mongoose.storage.driver.hdfs.util.EnvUtil;
import com.emc.mongoose.storage.driver.hdfs.util.LogAnalyzer;
import com.emc.mongoose.storage.driver.hdfs.util.docker.StorageNodeContainer;
import com.emc.mongoose.storage.driver.hdfs.util.docker.MongooseContainer;
import com.github.akurilov.commons.system.SizeInBytes;
import org.apache.commons.csv.CSVRecord;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

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

import static com.emc.mongoose.Constants.MIB;
import static com.emc.mongoose.storage.driver.hdfs.util.docker.MongooseContainer.CONTAINER_SHARE_PATH;
import static com.emc.mongoose.storage.driver.hdfs.util.docker.MongooseContainer.HOST_SHARE_PATH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ReadUsingVariablePathTest {

	private static final String SCENARIO_FILE = "scenario" + File.separator
		+ "read_using_variable_path.js";
	private static final String ITEM_LIST_FILE = CONTAINER_SHARE_PATH + File.separator
		+ "read_using_variable_path_items.csv";
	private static final String ITEM_OUTPUT_PATH = "/"
		+ ReadUsingVariablePathTest.class.getSimpleName();
	private static final String STEP_ID = ReadUsingVariablePathTest.class.getSimpleName();
	private static final int STEP_LIMIT_COUNT = 1000;
	private static final SizeInBytes ITEM_DATA_SIZE = new SizeInBytes(MIB);
	private static final int CONCURRENCY = 10;

	private static StorageNodeContainer HDFS_NODE_CONTAINER;
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
		args.add("--load-step-id=" + STEP_ID);
		args.add("--run-scenario=" + hostScenarioPath);
		args.add("--item-naming-radix=16");
		args.add("--item-naming-length=16");
		args.add("--storage-driver-limit-concurrency=" + CONCURRENCY);
		EnvUtil.set("ITEM_LIST_FILE", ITEM_LIST_FILE);
		EnvUtil.set("ITEM_DATA_SIZE", ITEM_DATA_SIZE.toString());
		EnvUtil.set("ITEM_OUTPUT_PATH", ITEM_OUTPUT_PATH);
		EnvUtil.set("STEP_LIMIT_COUNT", Integer.toString(STEP_LIMIT_COUNT));

		try {
			HDFS_NODE_CONTAINER = new StorageNodeContainer();
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
	public void testOpTraceLogRecords()
	throws Exception {
		final LongAdder opTraceRecCount = new LongAdder();
		final int baseOutputPathLen = ITEM_OUTPUT_PATH.length();
		// Item path should look like:
		// ${FILE_OUTPUT_PATH}/1/b/0123456789abcdef
		// ${FILE_OUTPUT_PATH}/b/fedcba9876543210
		final Pattern subPathPtrn = Pattern.compile("(/[0-9a-f]){1,2}/[0-9a-f]{16}");
		final Consumer<CSVRecord> opTraceReqTestFunc = opTraceRec -> {
			LogAnalyzer.testOpTraceRecord(opTraceRec, OpType.READ.ordinal(), ITEM_DATA_SIZE);
			String nextFilePath = opTraceRec.get(1);
			assertTrue(nextFilePath.startsWith(ITEM_OUTPUT_PATH));
			nextFilePath = nextFilePath.substring(baseOutputPathLen);
			final Matcher m = subPathPtrn.matcher(nextFilePath);
			assertTrue("\"" + nextFilePath + "\" doesn't match the pattern" , m.matches());
			opTraceRecCount.increment();
		};
		LogAnalyzer.testOpTraceLogRecords(STEP_ID, opTraceReqTestFunc);
		assertEquals(
			"There should be more than 1 record in the I/O trace log file",
			STEP_LIMIT_COUNT, opTraceRecCount.sum()
		);
	}

	@Test
	public void testMetricsLogRecords()
	throws Exception {
		LogAnalyzer.testMetricsLogRecords(
			LogAnalyzer.getMetricsLogRecords(STEP_ID), OpType.READ, CONCURRENCY, 1, ITEM_DATA_SIZE,
			STEP_LIMIT_COUNT, 0, 10
		);
	}

	@Test
	public void testTotalMetricsLogRecord()
	throws Exception {
		LogAnalyzer.testTotalMetricsLogRecord(
			LogAnalyzer.getMetricsTotalLogRecords(STEP_ID).get(0), OpType.READ, CONCURRENCY, 1,
			ITEM_DATA_SIZE, STEP_LIMIT_COUNT, 0
		);
	}

	@Test
	public void testMetricsStdout()
	throws Exception {
		LogAnalyzer.testSingleMetricsStdout(
			STD_OUTPUT.replaceAll("[\r\n]+", " "), OpType.READ, CONCURRENCY, 1, ITEM_DATA_SIZE, 10
		);
	}

	@Test
	public void testFinalMetricsTableRowStdout()
	throws Exception {
		LogAnalyzer.testFinalMetricsTableRowStdout(
			STD_OUTPUT, STEP_ID, OpType.READ, 1, CONCURRENCY, STEP_LIMIT_COUNT, 0, ITEM_DATA_SIZE
		);
	}
}
