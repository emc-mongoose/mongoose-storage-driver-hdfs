package com.emc.mongoose.storage.driver.hdfs;

import com.emc.mongoose.api.model.io.task.IoTask;
import com.emc.mongoose.api.model.io.task.data.DataIoTask;
import com.emc.mongoose.api.model.item.DataItem;
import com.emc.mongoose.ui.log.LogUtil;
import com.emc.mongoose.ui.log.Loggers;
import static com.emc.mongoose.api.model.io.task.IoTask.Status.FAIL_IO;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.logging.log4j.Level;

import java.io.IOException;

public interface DeleteHelper {

	static void invokeFileDelete(
		final DataIoTask<? extends DataItem> fileIoTask, final FileSystem endpoint,
		final HdfsStorageDriver driver
	) {
		final String dstPath = fileIoTask.getDstPath();
		final DataItem fileItem = fileIoTask.getItem();
		final String itemName = fileItem.getName();
		final Path filePath;
		if(dstPath == null || dstPath.isEmpty() || itemName.startsWith(dstPath)) {
			filePath = new Path(itemName);
		} else {
			filePath = new Path(dstPath, itemName);
		}
		try {
			if(endpoint.delete(filePath, false)) {
				driver.finishFileIoTask(fileIoTask);
			} else {
				Loggers.ERR.debug(
					"Failed to delete the file {} @ {}", filePath,
					endpoint.getCanonicalServiceName()
				);
				fileIoTask.startResponse();
				fileIoTask.finishResponse();
				fileIoTask.setStatus(IoTask.Status.RESP_FAIL_UNKNOWN);
			}
		} catch(final IOException e) {
			LogUtil.exception(
				Level.DEBUG, e, "Failed to delete the file {} @ {}", filePath,
				endpoint.getCanonicalServiceName()
			);
			fileIoTask.startResponse();
			fileIoTask.finishResponse();
			fileIoTask.setStatus(FAIL_IO);
		}
	}
}
