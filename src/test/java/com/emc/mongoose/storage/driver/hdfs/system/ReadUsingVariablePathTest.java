package com.emc.mongoose.storage.driver.hdfs.system;

import com.emc.mongoose.storage.driver.hdfs.util.EnvUtil;
import com.emc.mongoose.storage.driver.hdfs.util.docker.HdfsNodeContainer;
import com.emc.mongoose.storage.driver.hdfs.util.docker.MongooseContainer;
import static com.emc.mongoose.storage.driver.hdfs.util.docker.MongooseContainer.CONTAINER_SHARE_PATH;
import static com.emc.mongoose.storage.driver.hdfs.util.docker.MongooseContainer.HOST_SHARE_PATH;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class ReadUsingVariablePathTest {

	private static final String SCENARIO_FILE = "scenario" + File.separator
		+ "read_using_variable_path.js";
	private static final String ITEM_LIST_FILE = CONTAINER_SHARE_PATH + File.separator
		+ "items.csv";
	private static final String ITEM_OUTPUT_PATH = "/"
		+ ReadUsingVariablePathTest.class.getSimpleName();

	private static HdfsNodeContainer HDFS_NODE_CONTAINER;
	private static MongooseContainer MONGOOSE_CONTAINER;

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
		args.add("--test-scenario-file=" + hostScenarioPath.toString());
		EnvUtil.set("ITEM_LIST_FILE", ITEM_LIST_FILE);
		EnvUtil.set("ITEM_OUTPUT_PATH", ITEM_OUTPUT_PATH);

		try {
			HDFS_NODE_CONTAINER = new HdfsNodeContainer();
			MONGOOSE_CONTAINER = new MongooseContainer(args, 1000);
		} catch(final Exception e) {
			throw new AssertionError(e);
		}
		MONGOOSE_CONTAINER.run();
	}

	@AfterClass
	public static void tearDownClass()
	throws Exception {
		HDFS_NODE_CONTAINER.close();
		MONGOOSE_CONTAINER.close();
	}

	@Test
	public void test()
	throws Exception {
		assertTrue(true);
	}
}
