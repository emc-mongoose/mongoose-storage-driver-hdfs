package com.emc.mongoose.storage.driver.hdfs.system;

import com.emc.mongoose.api.common.env.Extensions;
import com.emc.mongoose.api.model.io.IoType;
import com.emc.mongoose.storage.driver.hdfs.util.EnvUtil;
import com.emc.mongoose.storage.driver.hdfs.util.LogAnalyzer;
import com.emc.mongoose.storage.driver.hdfs.util.docker.HdfsNodeContainer;
import com.emc.mongoose.storage.driver.hdfs.util.docker.MongooseContainer;
import static com.emc.mongoose.api.common.Constants.MIB;
import static com.emc.mongoose.storage.driver.hdfs.util.docker.MongooseContainer.HOST_SHARE_PATH;

import com.github.akurilov.commons.system.SizeInBytes;

import org.apache.commons.csv.CSVRecord;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

public class CopyUsingInputPathTest {

	private static final String SCENARIO_FILE = "scenario" + File.separator
		+ "copy_using_input_path.js";
	private static final String STEP_ID = CopyUsingInputPathTest.class.getSimpleName();
	private static final String ITEM_PATH_0 = "/" + STEP_ID + "/source";
	private static final String ITEM_PATH_1 = "/" + STEP_ID + "/destination";
	private static final int TEST_STEP_LIMIT_COUNT = 1000;
	private static final SizeInBytes ITEM_DATA_SIZE = new SizeInBytes(MIB);
	private static final int CONCURRENCY = 100;

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
		args.add("--load-limit-concurrency=" + CONCURRENCY);
		EnvUtil.set("TEST_STEP_LIMIT_COUNT", Integer.toString(TEST_STEP_LIMIT_COUNT));
		EnvUtil.set("ITEM_DATA_SIZE", ITEM_DATA_SIZE.toString());
		EnvUtil.set("ITEM_PATH_0", ITEM_PATH_0);
		EnvUtil.set("ITEM_PATH_1", ITEM_PATH_1);

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
	public void testIoTraceRecords()
	throws Exception {
		final LongAdder ioTraceRecCount = new LongAdder();
		final URI endpointUri = new URI("hdfs", null, "127.0.0.1", 9000, "/", null, null);
		final Configuration hadoopConfig = new Configuration();
		hadoopConfig.setClassLoader(Extensions.CLS_LOADER);
		final FileSystem endpoint = FileSystem.get(endpointUri, hadoopConfig);
		final Consumer<CSVRecord> ioTraceRecTestFunc = ioTraceRecord -> {
			final String nextItemPath = ioTraceRecord.get("ItemPath");
			try {
				final FileStatus dstFileStatus = endpoint.getFileStatus(
					new org.apache.hadoop.fs.Path(nextItemPath)
				);
				final FileStatus srcFileStatus = endpoint.getFileStatus(
					new org.apache.hadoop.fs.Path(nextItemPath.replace(ITEM_PATH_1, ITEM_PATH_0))
				);
				assertEquals(srcFileStatus.getLen(), dstFileStatus.getLen());
				LogAnalyzer.testIoTraceRecord(
					ioTraceRecord, IoType.CREATE.ordinal(), new SizeInBytes(srcFileStatus.getLen())
				);
				ioTraceRecCount.increment();
			} catch(final IOException e) {
				fail(e.getMessage());
			}
		};
		LogAnalyzer.testContainerIoTraceLogRecords(STEP_ID, ioTraceRecTestFunc);
		assertEquals(
			"There should be " + TEST_STEP_LIMIT_COUNT + " records in the I/O trace log file",
			TEST_STEP_LIMIT_COUNT, ioTraceRecCount.sum()
		);
	}

	@Test
	public void testTotalMetricsLogRecords()
	throws Exception {
		final List<CSVRecord> totalMetricsLogRecords = LogAnalyzer
			.getContainerMetricsTotalLogRecords(STEP_ID);
		assertEquals(
			"There should be 1 total metrics records in the log file", 1,
			totalMetricsLogRecords.size()
		);
		LogAnalyzer.testTotalMetricsLogRecord(
			totalMetricsLogRecords.get(0), IoType.CREATE, CONCURRENCY, 1, ITEM_DATA_SIZE, 0, 0
		);
	}

	@Test
	public void testMetricsLogRecords()
	throws Exception {
		final List<CSVRecord> metricsLogRecords = LogAnalyzer.getContainerMetricsLogRecords(
			STEP_ID
		);
		assertTrue(
			"There should be more than 0 metrics records in the log file",
			metricsLogRecords.size() > 0
		);
		LogAnalyzer.testMetricsLogRecords(
			metricsLogRecords, IoType.CREATE, CONCURRENCY, 1, ITEM_DATA_SIZE, 0, 0, 10
		);
	}

	@Test
	public void testSingleMetricsStdout()
	throws Exception {
		LogAnalyzer.testSingleMetricsStdout(
			STD_OUTPUT.replaceAll("[\r\n]+", " "), IoType.CREATE, CONCURRENCY, 1, ITEM_DATA_SIZE, 10
		);
	}

	@Test
	public void testFinalMetricsTableRowStdout()
	throws Exception {
		LogAnalyzer.testFinalMetricsTableRowStdout(
			STD_OUTPUT, STEP_ID, IoType.CREATE, 1, CONCURRENCY, 0, 0, ITEM_DATA_SIZE
		);
	}
}
