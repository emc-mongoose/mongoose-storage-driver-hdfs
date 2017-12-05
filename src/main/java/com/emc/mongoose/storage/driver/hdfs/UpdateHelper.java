package com.emc.mongoose.storage.driver.hdfs;

import com.emc.mongoose.api.model.io.task.data.DataIoTask;
import com.emc.mongoose.api.model.item.DataItem;
import com.emc.mongoose.ui.log.Loggers;
import static com.emc.mongoose.api.model.item.DataItem.getRangeCount;
import static com.emc.mongoose.api.model.item.DataItem.getRangeOffset;

import com.github.akurilov.commons.collection.Range;

import org.apache.hadoop.fs.FSDataOutputStream;

import java.util.List;

interface UpdateHelper {

	static void invokeFileRandomRangesUpdate(
		final DataIoTask<? extends DataItem> ioTask, final DataItem fileItem,
		final FSDataOutputStream outputStream, final HdfsStorageDriver driver
	) {

		long countBytesDone = ioTask.getCountBytesDone();
		final long updatingRangesSize = ioTask.getMarkedRangesSize();

		if(updatingRangesSize > 0 && updatingRangesSize > countBytesDone) {

			DataItem updatingRange;
			int currRangeIdx;
			while(true) {
				currRangeIdx = ioTask.getCurrRangeIdx();
				if(currRangeIdx < getRangeCount(fileItem.size())) {
					updatingRange = ioTask.getCurrRangeUpdate();
					if(updatingRange == null) {
						ioTask.setCurrRangeIdx(++ currRangeIdx);
					} else {
						break;
					}
				} else {
					ioTask.setCountBytesDone(updatingRangesSize);
					return;
				}
			}

			final long updatingRangeSize = updatingRange.size();
			if(Loggers.MSG.isTraceEnabled()) {
				Loggers.MSG.trace(
					"{}: set the file position = {} + {}", fileItem.getName(),
					getRangeOffset(currRangeIdx), countBytesDone
				);
			}
			outputStream.position(getRangeOffset(currRangeIdx) + countBytesDone);
			countBytesDone += updatingRange.writeToFileChannel(
				outputStream, updatingRangeSize - countBytesDone
			);
			if(Loggers.MSG.isTraceEnabled()) {
				Loggers.MSG.trace(
					"{}: {} bytes written totally", fileItem.getName(), countBytesDone
				);
			}
			if(countBytesDone == updatingRangeSize) {
				ioTask.setCurrRangeIdx(currRangeIdx + 1);
				ioTask.setCountBytesDone(0);
			} else {
				ioTask.setCountBytesDone(countBytesDone);
			}
		} else {
			if(Loggers.MSG.isTraceEnabled()) {
				Loggers.MSG.trace("{}: {} bytes updated", fileItem.getName(), updatingRangesSize);
			}
			driver.finishFileIoTask(ioTask);
			fileItem.commitUpdatedRanges(ioTask.getMarkedRangesMaskPair());
		}
	}

	static void invokeFileFixedRangesUpdate(
		final DataIoTask<? extends DataItem> ioTask, final DataItem fileItem,
		final FSDataOutputStream outputStream, final List<Range> byteRanges,
		final HdfsStorageDriver driver
	) {

		long countBytesDone = ioTask.getCountBytesDone();
		final long baseItemSize = fileItem.size();
		final long updatingRangesSize = ioTask.getMarkedRangesSize();

		if(updatingRangesSize > 0 && updatingRangesSize > countBytesDone) {

			Range byteRange;
			DataItem updatingRange;
			int currRangeIdx = ioTask.getCurrRangeIdx();
			long rangeBeg;
			long rangeEnd;
			long rangeSize;

			if(currRangeIdx < byteRanges.size()) {
				byteRange = byteRanges.get(currRangeIdx);
				rangeBeg = byteRange.getBeg();
				rangeEnd = byteRange.getEnd();
				rangeSize = byteRange.getSize();
				if(rangeSize == -1) {
					if(rangeBeg == -1) {
						// last "rangeEnd" bytes
						rangeBeg = baseItemSize - rangeEnd;
						rangeSize = rangeEnd;
					} else if(rangeEnd == -1) {
						// start @ offset equal to "rangeBeg"
						rangeSize = baseItemSize - rangeBeg;
					} else {
						rangeSize = rangeEnd - rangeBeg + 1;
					}
				} else {
					// append
					rangeBeg = baseItemSize;
					// note down the new size
					fileItem.size(baseItemSize + updatingRangesSize);
				}
				updatingRange = fileItem.slice(rangeBeg, rangeSize);
				updatingRange.position(countBytesDone);
				outputStream.position(rangeBeg + countBytesDone);
				countBytesDone += updatingRange.writeToFileChannel(outputStream, rangeSize - countBytesDone);

				if(countBytesDone == rangeSize) {
					ioTask.setCurrRangeIdx(currRangeIdx + 1);
					ioTask.setCountBytesDone(0);
				} else {
					ioTask.setCountBytesDone(countBytesDone);
				}

			} else {
				ioTask.setCountBytesDone(updatingRangesSize);
			}
		} else {
			driver.finishFileIoTask(ioTask);
		}
	}

	static void invokeFileOverwrite(
		final DataIoTask<? extends DataItem> ioTask, final DataItem fileItem,
		final FSDataOutputStream outputStream, final HdfsStorageDriver driver
	) {
		throw new AssertionError("Not implemented yet");
	}
}
