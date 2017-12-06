package com.emc.mongoose.storage.driver.hdfs;

import com.emc.mongoose.api.model.io.task.IoTask;

public interface IoTaskCallback {

	void notifyIoTaskFinish(final IoTask ioTask);
}
