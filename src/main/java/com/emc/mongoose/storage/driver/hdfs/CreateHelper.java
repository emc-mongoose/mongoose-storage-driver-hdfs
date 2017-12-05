package com.emc.mongoose.storage.driver.hdfs;

import com.emc.mongoose.api.model.io.task.IoTask;
import com.emc.mongoose.api.model.io.task.data.DataIoTask;
import com.emc.mongoose.api.model.item.DataItem;
import com.emc.mongoose.ui.log.LogUtil;
import static com.emc.mongoose.api.model.io.task.IoTask.Status.ACTIVE;
import static com.emc.mongoose.api.model.io.task.IoTask.Status.FAIL_IO;

import com.github.akurilov.commons.io.util.OutputStreamWrapperChannel;
import static com.github.akurilov.commons.system.DirectMemUtil.REUSABLE_BUFF_SIZE_MAX;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import org.apache.logging.log4j.Level;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.List;

interface CreateHelper {

	static void invokeFileCreate(
		final DataIoTask<? extends DataItem> fileIoTask, final DataItem fileItem,
		final FSDataOutputStream outputStream, final HdfsStorageDriver driver
	) {
		final long fileSize;
		try {
			fileSize = fileItem.size();
		} catch(final IOException e) {
			throw new AssertionError(e);
		}
		long countBytesDone = fileIoTask.getCountBytesDone();
		try {
			long remainingBytes = fileSize - countBytesDone;
			if(remainingBytes > 0) {
				final WritableByteChannel outputChan = OutputStreamWrapperChannel
					.getThreadLocalInstance(outputStream, remainingBytes);
				countBytesDone += fileItem.writeToSocketChannel(outputChan, remainingBytes);
				fileIoTask.setCountBytesDone(countBytesDone);
			} else {
				driver.finishFileIoTask(fileIoTask);
			}
		} catch(final IOException e) {
			LogUtil.exception(
				Level.DEBUG, e, "Failed to write to the file: {}" + fileItem.getName()
			);
			driver.finishFileIoTask(fileIoTask);
			fileIoTask.setStatus(FAIL_IO);
		}
	}

	static void invokeFileCopy(
		final DataIoTask<? extends DataItem> fileIoTask, final DataItem fileItem,
		final FSDataInputStream inputStream, final FSDataOutputStream outputStream,
		final HdfsStorageDriver driver
	) {
		long countBytesDone = fileIoTask.getCountBytesDone();
		final long fileSize;
		try {
			fileSize = fileItem.size();
		} catch(final IOException e) {
			throw new AssertionError(e);
		}
		final long remainingSize = fileSize - countBytesDone;
		if(remainingSize > 0 && ACTIVE.equals(fileIoTask.getStatus())) {
			final byte[] buff = new byte[
				remainingSize > REUSABLE_BUFF_SIZE_MAX ?
					REUSABLE_BUFF_SIZE_MAX : (int) remainingSize
				];
			try {
				final int n = inputStream.read(buff, 0, buff.length);
				outputStream.write(buff, 0, n);
				countBytesDone += n;
				fileIoTask.setCountBytesDone(countBytesDone);
			} catch(final IOException e) {
				LogUtil.exception(
					Level.DEBUG, e, "Failed to copy the file: {}" + fileIoTask.getItem().getName()
				);
				driver.finishFileIoTask(fileIoTask);
				fileIoTask.setStatus(FAIL_IO);
			}
		}
		if(countBytesDone == fileSize) {
			driver.finishFileIoTask(fileIoTask);
		}
	}

	static void invokeFileConcat(
		final DataIoTask<? extends DataItem> fileIoTask, final DataItem fileItem,
		final List<? extends DataItem> srcItems, final FileSystem endpoint,
		final HdfsStorageDriver driver, final FsPermission fsPerm
		) {

		final String dstPath = fileIoTask.getDstPath();
		final int srcItemsCount = srcItems.size();
		final Path[] srcPaths = new Path[srcItems.size()];
		final String fileName = fileItem.getName();
		final Path dstFilePath = driver.getFilePath(dstPath, fileName);
		DataItem srcItem;
		long dstItemSize = 0;

		try {
			for(int i = 0; i < srcItemsCount; i ++) {
				srcItem = srcItems.get(i);
				srcPaths[i] = driver.getFilePath(dstPath, srcItem.getName());
				dstItemSize += srcItem.size();
			}
			endpoint
				.create(
					dstFilePath, fsPerm, false, 0, endpoint.getDefaultReplication(dstFilePath),
					dstItemSize, null
				)
				.close();
			endpoint.concat(dstFilePath, srcPaths);
			driver.finishFileIoTask(fileIoTask);
		} catch(final IOException e) {
			fileIoTask.startResponse();
			fileIoTask.finishResponse();
			fileIoTask.setStatus(IoTask.Status.FAIL_IO);
			LogUtil.exception(Level.DEBUG, e, "I/O task \"{}\" failure", fileIoTask);
		} catch(final Throwable cause) {
			fileIoTask.startResponse();
			fileIoTask.finishResponse();
			fileIoTask.setStatus(IoTask.Status.FAIL_UNKNOWN);
			LogUtil.exception(Level.DEBUG, cause, "I/O task \"{}\" failure", fileIoTask);
		}
	}
}
