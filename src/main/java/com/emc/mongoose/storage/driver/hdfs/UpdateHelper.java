package com.emc.mongoose.storage.driver.hdfs;

import com.emc.mongoose.api.model.io.task.data.DataIoTask;
import com.emc.mongoose.api.model.item.DataItem;

import com.github.akurilov.commons.collection.Range;
import com.github.akurilov.commons.io.util.OutputStreamWrapperChannel;

import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

interface UpdateHelper {

	static void invokeFileAppend(
		final DataIoTask<? extends DataItem> ioTask, final DataItem fileItem,
		final FSDataOutputStream outputStream, final Range appendRange,
		final HdfsStorageDriver driver
	) throws IOException {
		final long countBytesDone = ioTask.getCountBytesDone();
		final long appendSize = appendRange.getSize();
		final long remainingSize = appendSize - countBytesDone;
		long n;
		if(remainingSize > 0) {
			final WritableByteChannel outputChan = OutputStreamWrapperChannel
				.getThreadLocalInstance(outputStream, remainingSize);
			n = fileItem.writeToSocketChannel(outputChan, remainingSize);
			ioTask.setCountBytesDone(countBytesDone + n);
			fileItem.size(fileItem.size() + n);
		} else {
			driver.notifyIoTaskFinish(ioTask);
		}
	}
}
