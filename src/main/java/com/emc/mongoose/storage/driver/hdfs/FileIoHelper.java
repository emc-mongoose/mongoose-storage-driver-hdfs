package com.emc.mongoose.storage.driver.hdfs;

import com.emc.mongoose.api.model.data.DataCorruptionException;
import com.emc.mongoose.api.model.data.DataSizeException;
import com.emc.mongoose.api.model.io.task.IoTask;
import com.emc.mongoose.api.model.io.task.data.DataIoTask;
import com.emc.mongoose.api.model.item.DataItem;
import com.emc.mongoose.ui.log.Loggers;
import static com.emc.mongoose.api.model.io.task.IoTask.Status.ACTIVE;
import static com.emc.mongoose.api.model.item.DataItem.getRangeCount;
import static com.emc.mongoose.api.model.item.DataItem.getRangeOffset;

import com.github.akurilov.commons.collection.Range;
import com.github.akurilov.commons.io.util.OutputStreamWrapperChannel;
import com.github.akurilov.commons.system.DirectMemUtil;
import static com.github.akurilov.commons.system.DirectMemUtil.REUSABLE_BUFF_SIZE_MAX;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.BitSet;
import java.util.List;

public interface FileIoHelper {

	static boolean invokeFileCreate(
		final DataIoTask<? extends DataItem> fileIoTask, final DataItem fileItem,
		final FSDataOutputStream outputStream
	) throws IOException {

		final long fileSize;
		try {
			fileSize = fileItem.size();
		} catch(final IOException e) {
			throw new AssertionError(e);
		}
		long countBytesDone = fileIoTask.getCountBytesDone();
		final long remainingBytes = fileSize - countBytesDone;

		if(remainingBytes > 0) {
			final WritableByteChannel outputChan = OutputStreamWrapperChannel
				.getThreadLocalInstance(outputStream, remainingBytes);
			countBytesDone += fileItem.writeToSocketChannel(outputChan, remainingBytes);
			fileIoTask.setCountBytesDone(countBytesDone);
		}

		return remainingBytes <= 0;
	}

	static boolean invokeFileCopy(
		final DataIoTask<? extends DataItem> fileIoTask, final DataItem fileItem,
		final FSDataInputStream inputStream, final FSDataOutputStream outputStream
	) throws IOException {

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
			final int n = inputStream.read(buff, 0, buff.length);
			outputStream.write(buff, 0, n);
			countBytesDone += n;
			fileIoTask.setCountBytesDone(countBytesDone);
		}

		return countBytesDone >= fileSize;
	}

	/*static boolean invokeFileConcat(
		final DataIoTask<? extends DataItem> fileIoTask, final DataItem fileItem,
		final List<? extends DataItem> srcItems, final FileSystem endpoint,
		final FsPermission fsPerm
	) throws IOException {

		final String dstPath = fileIoTask.getDstPath();
		final int srcItemsCount = srcItems.size();
		final Path[] srcPaths = new Path[srcItems.size()];
		final String fileName = fileItem.getName();
		final Path dstFilePath = driver.getFilePath(dstPath, fileName);
		DataItem srcItem;
		long dstItemSize = 0;

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

		return true;
	}*/

	static boolean invokeFileReadAndVerify(
		final DataIoTask<? extends DataItem> ioTask, final DataItem fileItem,
		final FSDataInputStream inputStream

	) throws DataSizeException, DataCorruptionException, IOException {
		long countBytesDone = ioTask.getCountBytesDone();
		final long contentSize = fileItem.size();

		if(countBytesDone < contentSize) {
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
			ioTask.setCountBytesDone(countBytesDone);
		}

		return countBytesDone >= contentSize;
	}

	static boolean invokeFileReadAndVerifyRandomRanges(
		final DataIoTask<? extends DataItem> ioTask, final DataItem fileItem,
		final FSDataInputStream inputStream, final BitSet maskRangesPair[]
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
					return true;
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
		}

		return rangesSizeSum <= 0 || rangesSizeSum <= countBytesDone;
	}

	static boolean invokeFileReadAndVerifyFixedRanges(
		final DataIoTask<? extends DataItem> ioTask, final DataItem fileItem,
		final FSDataInputStream inputStream, final List<Range> fixedRanges
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
						return true;
					} else {
						ioTask.setCurrRangeIdx(currFixedRangeIdx + 1);
						rangeBytesDone = 0;
					}
				}
				ioTask.setCountBytesDone(rangeBytesDone);
			} else {
				ioTask.setCountBytesDone(fixedRangesSizeSum);
			}
		}

		return fixedRangesSizeSum <= 0 || fixedRangesSizeSum <= countBytesDone;
	}

	static boolean invokeFileRead(
		final DataIoTask<? extends DataItem> ioTask, final DataItem fileItem,
		final FSDataInputStream inputStream
	) throws IOException {
		long countBytesDone = ioTask.getCountBytesDone();
		final long contentSize = fileItem.size();
		int n;
		if(countBytesDone < contentSize) {
			n = inputStream.read(
				DirectMemUtil.getThreadLocalReusableBuff(contentSize - countBytesDone)
			);
			if(n < 0) {
				ioTask.setCountBytesDone(countBytesDone);
				fileItem.size(countBytesDone);
				return true;
			} else {
				countBytesDone += n;
				ioTask.setCountBytesDone(countBytesDone);
			}
		}

		return countBytesDone >= contentSize;
	}

	static boolean invokeFileReadRandomRanges(
		final DataIoTask<? extends DataItem> ioTask, final DataItem fileItem,
		final FSDataInputStream inputStream, final BitSet maskRangesPair[]
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
					return true;
				}
			}

			final long currRangeSize = range2read.size();
			inputStream.seek(getRangeOffset(currRangeIdx) + countBytesDone);
			n = inputStream.read(
				DirectMemUtil.getThreadLocalReusableBuff(currRangeSize - countBytesDone)
			);
			if(n < 0) {
				ioTask.setCountBytesDone(countBytesDone);
				return true;
			}
			countBytesDone += n;

			if(countBytesDone == currRangeSize) {
				ioTask.setCurrRangeIdx(currRangeIdx + 1);
				ioTask.setCountBytesDone(0);
			} else {
				ioTask.setCountBytesDone(countBytesDone);
			}
		}

		return rangesSizeSum <= 0 || rangesSizeSum <= countBytesDone;
	}

	static boolean invokeFileReadFixedRanges(
		final DataIoTask<? extends DataItem> ioTask, final DataItem fileItem,
		final FSDataInputStream inputStream, final List<Range> byteRanges
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
					ioTask.setCountBytesDone(countBytesDone);
					return true;
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
		}

		return rangesSizeSum <= 0 || rangesSizeSum <= countBytesDone;
	}

	static boolean invokeFileAppend(
		final DataIoTask<? extends DataItem> ioTask, final DataItem fileItem,
		final FSDataOutputStream outputStream, final Range appendRange
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
		}

		return remainingSize <= 0;
	}

	static boolean invokeFileDelete(
		final DataIoTask<? extends DataItem> fileIoTask, final FileSystem endpoint
	) throws IOException {

		final String dstPath = fileIoTask.getDstPath();
		final DataItem fileItem = fileIoTask.getItem();
		final String itemName = fileItem.getName();
		final Path filePath;
		if(dstPath == null || dstPath.isEmpty() || itemName.startsWith(dstPath)) {
			filePath = new Path(itemName);
		} else {
			filePath = new Path(dstPath, itemName);
		}

		if(!endpoint.delete(filePath, false)) {
			Loggers.ERR.debug(
				"Failed to delete the file {} @ {}", filePath,
				endpoint.getCanonicalServiceName()
			);
			fileIoTask.startResponse();
			fileIoTask.finishResponse();
			fileIoTask.setStatus(IoTask.Status.RESP_FAIL_UNKNOWN);
		}

		return true;
	}
}
