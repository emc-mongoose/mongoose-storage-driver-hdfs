package com.emc.mongoose.storage.driver.hdfs;

import com.emc.mongoose.api.model.data.DataCorruptionException;
import com.emc.mongoose.api.model.data.DataSizeException;
import com.emc.mongoose.api.model.io.task.IoTask;
import com.emc.mongoose.api.model.io.task.data.DataIoTask;
import com.emc.mongoose.api.model.item.DataItem;
import com.emc.mongoose.ui.log.Loggers;
import static com.emc.mongoose.api.model.item.DataItem.getRangeCount;
import static com.emc.mongoose.api.model.item.DataItem.getRangeOffset;

import com.github.akurilov.commons.collection.Range;
import com.github.akurilov.commons.system.DirectMemUtil;

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.List;


interface ReadHelper {

	static void invokeFileReadAndVerify(
		final DataIoTask<? extends DataItem> ioTask, final DataItem fileItem,
		final FSDataInputStream inputStream, final HdfsStorageDriver driver
	) throws DataSizeException, IOException {
		long countBytesDone = ioTask.getCountBytesDone();
		final long contentSize = fileItem.size();
		if(countBytesDone < contentSize) {
			try {
				if(fileItem.isUpdated()) {
					final DataItem currRange = ioTask.getCurrRange();
					final int nextRangeIdx = ioTask.getCurrRangeIdx() + 1;
					final long nextRangeOffset = getRangeOffset(nextRangeIdx);
					if(currRange != null) {
						final ByteBuffer inBuff = DirectMemUtil.getThreadLocalReusableBuff(
							nextRangeOffset - countBytesDone
						);
						final int n = inputStream.read(inBuff);
						if(n < 0) {
							throw new DataSizeException(contentSize, countBytesDone);
						} else {
							inBuff.flip();
							currRange.verify(inBuff);
							currRange.position(currRange.position() + n);
							countBytesDone += n;
							if(countBytesDone == nextRangeOffset) {
								ioTask.setCurrRangeIdx(nextRangeIdx);
							}
						}
					} else {
						throw new AssertionError("Null data range");
					}
				} else {
					final ByteBuffer inBuff = DirectMemUtil.getThreadLocalReusableBuff(
						contentSize - countBytesDone
					);
					final int n = inputStream.read(inBuff);
					if(n < 0) {
						throw new DataSizeException(contentSize, countBytesDone);
					} else {
						inBuff.flip();
						fileItem.verify(inBuff);
						fileItem.position(fileItem.position() + n);
						countBytesDone += n;
					}
				}
			} catch(final DataCorruptionException e) {
				ioTask.setStatus(IoTask.Status.RESP_FAIL_CORRUPT);
				countBytesDone += e.getOffset();
				ioTask.setCountBytesDone(countBytesDone);
				Loggers.MSG.debug(
					"{}: content mismatch @ offset {}, expected: {}, actual: {} ",
					fileItem.getName(), countBytesDone,
					String.format("\"0x%X\"", (int) (e.expected & 0xFF)),
					String.format("\"0x%X\"", (int) (e.actual & 0xFF))
				);
			}
			ioTask.setCountBytesDone(countBytesDone);
		} else {
			driver.notifyIoTaskFinish(ioTask);
		}
	}

	static void invokeFileReadAndVerifyRandomRanges(
		final DataIoTask<? extends DataItem> ioTask, final DataItem fileItem,
		final FSDataInputStream inputStream, final BitSet maskRangesPair[],
		final HdfsStorageDriver driver
	) throws DataSizeException, DataCorruptionException, IOException {

		long countBytesDone = ioTask.getCountBytesDone();
		final long rangesSizeSum = ioTask.getMarkedRangesSize();

		if(rangesSizeSum > 0 && rangesSizeSum > countBytesDone) {

			DataItem range2read;
			int currRangeIdx;
			while(true) {
				currRangeIdx = ioTask.getCurrRangeIdx();
				if(currRangeIdx < getRangeCount(fileItem.size())) {
					if(maskRangesPair[0].get(currRangeIdx) || maskRangesPair[1].get(currRangeIdx)) {
						range2read = ioTask.getCurrRange();
						if(Loggers.MSG.isTraceEnabled()) {
							Loggers.MSG.trace(
								"I/O task: {}, Range index: {}, size: {}, internal position: {}, " +
									"Done byte count: {}",
								ioTask.toString(), currRangeIdx, range2read.size(),
								range2read.position(), countBytesDone
							);
						}
						break;
					} else {
						ioTask.setCurrRangeIdx(++ currRangeIdx);
					}
				} else {
					ioTask.setCountBytesDone(rangesSizeSum);
					return;
				}
			}

			final long currRangeSize = range2read.size();
			final long currPos = getRangeOffset(currRangeIdx) + countBytesDone;
			inputStream.seek(currPos);
			final ByteBuffer inBuff = DirectMemUtil.getThreadLocalReusableBuff(
				currRangeSize - countBytesDone
			);
			final int n = inputStream.read(inBuff);
			if(n < 0) {
				throw new DataSizeException(rangesSizeSum, countBytesDone);
			} else {
				inBuff.flip();
				try {
					range2read.verify(inBuff);
					range2read.position(range2read.position() + n);
					countBytesDone += n;
				} catch(final DataCorruptionException e) {
					throw new DataCorruptionException(
						currPos + e.getOffset() - countBytesDone, e.expected, e.actual
					);
				}
			}

			if(Loggers.MSG.isTraceEnabled()) {
				Loggers.MSG.trace(
					"I/O task: {}, Done bytes count: {}, Curr range size: {}",
					ioTask.toString(), countBytesDone, range2read.size()
				);
			}

			if(countBytesDone == currRangeSize) {
				ioTask.setCurrRangeIdx(currRangeIdx + 1);
				ioTask.setCountBytesDone(0);
			} else {
				ioTask.setCountBytesDone(countBytesDone);
			}
		} else {
			driver.notifyIoTaskFinish(ioTask);
		}
	}

	static void invokeFileReadAndVerifyFixedRanges(
		final DataIoTask<? extends DataItem> ioTask, final DataItem fileItem,
		final FSDataInputStream inputStream, final List<Range> fixedRanges,
		final HdfsStorageDriver driver
	) throws DataSizeException, DataCorruptionException, IOException {

		final long baseItemSize = fileItem.size();
		final long fixedRangesSizeSum = ioTask.getMarkedRangesSize();

		long countBytesDone = ioTask.getCountBytesDone();
		// "countBytesDone" is the current range done bytes counter here
		long rangeBytesDone = countBytesDone;
		long currOffset;
		long cellOffset;
		long cellEnd;
		int n;

		if(fixedRangesSizeSum > 0 && fixedRangesSizeSum > countBytesDone) {

			Range fixedRange;
			DataItem currRange;
			int currFixedRangeIdx = ioTask.getCurrRangeIdx();
			long fixedRangeEnd;
			long fixedRangeSize;

			if(currFixedRangeIdx < fixedRanges.size()) {
				fixedRange = fixedRanges.get(currFixedRangeIdx);
				currOffset = fixedRange.getBeg();
				fixedRangeEnd = fixedRange.getEnd();
				if(currOffset == -1) {
					// last "rangeEnd" bytes
					currOffset = baseItemSize - fixedRangeEnd;
					fixedRangeSize = fixedRangeEnd;
				} else if(fixedRangeEnd == -1) {
					// start @ offset equal to "rangeBeg"
					fixedRangeSize = baseItemSize - currOffset;
				} else {
					fixedRangeSize = fixedRangeEnd - currOffset + 1;
				}

				// let (current offset = rangeBeg + rangeBytesDone)
				currOffset += rangeBytesDone;
				// find the internal data item's cell index which has:
				// (cell's offset <= current offset) && (cell's end > current offset)
				n = getRangeCount(currOffset + 1) - 1;
				cellOffset = getRangeOffset(n);
				cellEnd = Math.min(baseItemSize, getRangeOffset(n + 1));
				// get the found cell data item (updated or not)
				currRange = fileItem.slice(cellOffset, cellEnd - cellOffset);
				if(fileItem.isRangeUpdated(n)) {
					currRange.layer(fileItem.layer() + 1);
				}
				// set the cell data item internal position to (current offset - cell's offset)
				currRange.position(currOffset - cellOffset);
				inputStream.seek(currOffset);

				final ByteBuffer inBuff = DirectMemUtil.getThreadLocalReusableBuff(
					Math.min(
						fixedRangeSize - countBytesDone, currRange.size() - currRange.position()
					)
				);
				final int m = inputStream.read(inBuff);
				if(m < 0) {
					throw new DataSizeException(fixedRangesSizeSum, countBytesDone);
				} else {
					inBuff.flip();
					try {
						currRange.verify(inBuff);
						currRange.position(currRange.position() + m);
						rangeBytesDone += m;
					} catch(final DataCorruptionException e) {
						throw new DataCorruptionException(
							currOffset + e.getOffset() - countBytesDone, e.expected, e.actual
						);
					}
				}

				if(rangeBytesDone == fixedRangeSize) {
					// current byte range verification is finished
					if(currFixedRangeIdx == fixedRanges.size() - 1) {
						// current byte range was last in the list
						ioTask.setCountBytesDone(fixedRangesSizeSum);
						driver.notifyIoTaskFinish(ioTask);
						return;
					} else {
						ioTask.setCurrRangeIdx(currFixedRangeIdx + 1);
						rangeBytesDone = 0;
					}
				}
				ioTask.setCountBytesDone(rangeBytesDone);
			} else {
				ioTask.setCountBytesDone(fixedRangesSizeSum);
			}
		} else {
			driver.notifyIoTaskFinish(ioTask);
		}
	}

	static void invokeFileRead(
		final DataIoTask<? extends DataItem> ioTask, final DataItem fileItem,
		final FSDataInputStream inputStream, final HdfsStorageDriver driver
	) throws IOException {
		long countBytesDone = ioTask.getCountBytesDone();
		final long contentSize = fileItem.size();
		int n;
		if(countBytesDone < contentSize) {
			n = inputStream.read(
				DirectMemUtil.getThreadLocalReusableBuff(contentSize - countBytesDone)
			);
			if(n < 0) {
				driver.notifyIoTaskFinish(ioTask);
				ioTask.setCountBytesDone(countBytesDone);
				fileItem.size(countBytesDone);
			} else {
				countBytesDone += n;
				ioTask.setCountBytesDone(countBytesDone);
			}
		}
		if(countBytesDone == contentSize) {
			driver.notifyIoTaskFinish(ioTask);
		}
	}

	static void invokeFileReadRandomRanges(
		final DataIoTask<? extends DataItem> ioTask, final DataItem fileItem,
		final FSDataInputStream inputStream, final BitSet maskRangesPair[],
		final HdfsStorageDriver driver
	) throws IOException {

		int n;
		long countBytesDone = ioTask.getCountBytesDone();
		final long rangesSizeSum = ioTask.getMarkedRangesSize();

		if(rangesSizeSum > 0 && rangesSizeSum > countBytesDone) {

			DataItem range2read;
			int currRangeIdx;
			while(true) {
				currRangeIdx = ioTask.getCurrRangeIdx();
				if(currRangeIdx < getRangeCount(fileItem.size())) {
					if(maskRangesPair[0].get(currRangeIdx) || maskRangesPair[1].get(currRangeIdx)) {
						range2read = ioTask.getCurrRange();
						break;
					} else {
						ioTask.setCurrRangeIdx(++ currRangeIdx);
					}
				} else {
					ioTask.setCountBytesDone(rangesSizeSum);
					return;
				}
			}

			final long currRangeSize = range2read.size();
			inputStream.seek(getRangeOffset(currRangeIdx) + countBytesDone);
			n = inputStream.read(
				DirectMemUtil.getThreadLocalReusableBuff(currRangeSize - countBytesDone)
			);
			if(n < 0) {
				driver.notifyIoTaskFinish(ioTask);
				ioTask.setCountBytesDone(countBytesDone);
				return;
			}
			countBytesDone += n;

			if(countBytesDone == currRangeSize) {
				ioTask.setCurrRangeIdx(currRangeIdx + 1);
				ioTask.setCountBytesDone(0);
			}
		} else {
			driver.notifyIoTaskFinish(ioTask);
		}
	}

	static void invokeFileReadFixedRanges(
		final DataIoTask<? extends DataItem> ioTask, final DataItem fileItem,
		final FSDataInputStream inputStream, final List<Range> byteRanges,
		final HdfsStorageDriver driver
	) throws IOException {

		int n;
		long countBytesDone = ioTask.getCountBytesDone();
		final long baseItemSize = fileItem.size();
		final long rangesSizeSum = ioTask.getMarkedRangesSize();

		if(rangesSizeSum > 0 && rangesSizeSum > countBytesDone) {

			Range byteRange;
			int currRangeIdx = ioTask.getCurrRangeIdx();
			long rangeBeg;
			long rangeEnd;
			long rangeSize;

			if(currRangeIdx < byteRanges.size()) {
				byteRange = byteRanges.get(currRangeIdx);
				rangeBeg = byteRange.getBeg();
				rangeEnd = byteRange.getEnd();
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
				inputStream.seek(rangeBeg + countBytesDone);
				n = inputStream.read(
					DirectMemUtil.getThreadLocalReusableBuff(rangeSize - countBytesDone)
				);
				if(n < 0) {
					driver.notifyIoTaskFinish(ioTask);
					ioTask.setCountBytesDone(countBytesDone);
					return;
				}
				countBytesDone += n;

				if(countBytesDone == rangeSize) {
					ioTask.setCurrRangeIdx(currRangeIdx + 1);
					ioTask.setCountBytesDone(0);
				} else {
					ioTask.setCountBytesDone(countBytesDone);
				}
			} else {
				ioTask.setCountBytesDone(rangesSizeSum);
			}
		} else {
			driver.notifyIoTaskFinish(ioTask);
		}
	}
}
