package com.emc.mongoose.storage.driver.hdfs;

import com.emc.mongoose.api.common.env.Extensions;
import com.emc.mongoose.api.common.exception.OmgShootMyFootException;
import com.emc.mongoose.api.model.data.DataCorruptionException;
import com.emc.mongoose.api.model.data.DataInput;
import com.emc.mongoose.api.model.data.DataSizeException;
import com.emc.mongoose.api.model.io.IoType;
import com.emc.mongoose.api.model.io.task.IoTask;
import com.emc.mongoose.api.model.io.task.data.DataIoTask;
import com.emc.mongoose.api.model.io.task.path.PathIoTask;
import com.emc.mongoose.api.model.item.DataItem;
import com.emc.mongoose.api.model.item.Item;
import com.emc.mongoose.api.model.item.ItemFactory;
import com.emc.mongoose.api.model.item.PathItem;
import com.emc.mongoose.api.model.storage.Credential;
import com.emc.mongoose.storage.driver.nio.base.NioStorageDriverBase;
import com.emc.mongoose.ui.config.load.LoadConfig;
import com.emc.mongoose.ui.config.storage.StorageConfig;
import com.emc.mongoose.ui.config.storage.net.node.NodeConfig;
import com.emc.mongoose.ui.log.LogUtil;
import com.emc.mongoose.ui.log.Loggers;
import static com.emc.mongoose.api.model.io.task.IoTask.Status.ACTIVE;
import static com.emc.mongoose.api.model.io.task.IoTask.Status.FAIL_IO;
import static com.emc.mongoose.api.model.item.DataItem.getRangeCount;
import static com.emc.mongoose.api.model.item.DataItem.getRangeOffset;

import static com.github.akurilov.commons.system.DirectMemUtil.REUSABLE_BUFF_SIZE_MAX;
import com.github.akurilov.commons.collection.Range;
import com.github.akurilov.commons.io.util.OutputStreamWrapperChannel;
import com.github.akurilov.commons.system.DirectMemUtil;
import com.github.akurilov.commons.system.SizeInBytes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;

import org.apache.logging.log4j.Level;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.rmi.RemoteException;
import java.rmi.ServerException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class HdfsStorageDriver<I extends Item, O extends IoTask<I>>
extends NioStorageDriverBase<I, O> {

	public static final String DEFAULT_URI_SCHEMA = "hdfs";

	protected final ConcurrentMap<String, FileSystem> endpoints = new ConcurrentHashMap<>();
	private final String[] endpointAddrs;
	private final AtomicInteger rrc = new AtomicInteger(0);
	private final ConcurrentMap<DataIoTask<? extends DataItem>, FSDataInputStream>
		fileInputStreams = new ConcurrentHashMap<>();
	private final ConcurrentMap<DataIoTask<? extends DataItem>, FSDataOutputStream>
		fileOutputStreams = new ConcurrentHashMap<>();

	private final Configuration hadoopConfig;
	protected final FsPermission defaultFsPerm;

	private int nodePort = -1;
	private int inBuffSize = BUFF_SIZE_MIN;
	private int outBuffSize = BUFF_SIZE_MAX;

	public HdfsStorageDriver(
		final String testStepId, final DataInput dataInput, final LoadConfig loadConfig,
		final StorageConfig storageConfig, final boolean verifyFlag
	) throws OmgShootMyFootException {

		super(testStepId, dataInput, loadConfig, storageConfig, verifyFlag);

		hadoopConfig = new Configuration();
		hadoopConfig.setClassLoader(Extensions.CLS_LOADER);
		defaultFsPerm = FsPermission
			.getDefault()
			.applyUMask(FsPermission.getUMask(hadoopConfig));

		final String uid = credential == null ? null : credential.getUid();
		if(uid != null && !uid.isEmpty()) {
			UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(uid));
		}

		final NodeConfig nodeConfig = storageConfig.getNetConfig().getNodeConfig();
		nodePort = storageConfig.getNetConfig().getNodeConfig().getPort();
		final List<String> endpointAddrList = nodeConfig.getAddrs();
		endpointAddrs = endpointAddrList.toArray(new String[endpointAddrList.size()]);
		for(final String nodeAddr: endpointAddrs) {
			try {
				endpoints.computeIfAbsent(nodeAddr, this::getEndpoint);
			} catch(final NumberFormatException e) {
				LogUtil.exception(Level.ERROR, e, "Invalid port value?");
			} catch(final RuntimeException e) {
				final Throwable cause = e.getCause();
				if(cause != null) {
					LogUtil.exception(Level.ERROR, cause, "Failed to connect to HDFS endpoint");
				} else {
					LogUtil.exception(Level.ERROR, e, "Unexpected failure");
				}
			}
		}

		requestAuthTokenFunc = null; // do not use
		requestNewPathFunc = null; // do not use
	}

	protected String getNextEndpointAddr() {
		return endpointAddrs[rrc.getAndIncrement() % endpointAddrs.length];
	}

	protected FileSystem getEndpoint(final String nodeAddr) {
		try {
			final String addr;
			final int port;
			int portSepPos = nodeAddr.lastIndexOf(':');
			if(portSepPos > 0) {
				addr = nodeAddr.substring(0, portSepPos);
				port = Integer.parseInt(nodeAddr.substring(portSepPos + 1));
			} else {
				addr = nodeAddr;
				port = nodePort;
			}
			final String uid = credential == null ? null : credential.getUid();
			final URI endpointUri = new URI(DEFAULT_URI_SCHEMA, uid, addr, port, "/", null, null);
			return FileSystem.get(endpointUri, hadoopConfig);
		} catch(final URISyntaxException | IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	protected void prepareIoTask(final O ioTask)
	throws ServerException {
		super.prepareIoTask(ioTask);
		String endpointAddr = ioTask.getNodeAddr();
		if(endpointAddr == null) {
			endpointAddr = getNextEndpointAddr();
			ioTask.setNodeAddr(endpointAddr);
		}
	}

	private Path getFilePath(final String basePath, final String fileName) {
		if(basePath == null || basePath.isEmpty() || fileName.startsWith(basePath)) {
			return new Path(fileName);
		} else {
			return new Path(basePath, fileName);
		}
	}

	protected FSDataOutputStream getCreateFileStream(
		final DataIoTask<? extends DataItem> createFileTask
	) {
		final String endpointAddr = createFileTask.getNodeAddr();
		final FileSystem endpoint = endpoints.get(endpointAddr);
		final String dstPath = createFileTask.getDstPath();
		final DataItem fileItem = createFileTask.getItem();
		final String fileName = fileItem.getName();
		final Path filePath = getFilePath(dstPath, fileName);
		try {
			return endpoint.create(
				filePath, defaultFsPerm, false, outBuffSize,
				endpoint.getDefaultReplication(filePath), fileItem.size(), null
			);
		} catch(final IOException e) {
			createFileTask.setStatus(FAIL_IO);
			throw new RuntimeException(e);
		}
	}

	protected FSDataInputStream getReadFileStream(
		final DataIoTask<? extends DataItem> fileIoTask
	) {
		final String endpointAddr = fileIoTask.getNodeAddr();
		final FileSystem endpoint = endpoints.get(endpointAddr);
		final String srcPath = fileIoTask.getSrcPath();
		if(srcPath == null || srcPath.isEmpty()) {
			return null;
		}
		final DataItem fileItem = fileIoTask.getItem();
		final String fileName = fileItem.getName();
		final Path filePath = getFilePath(srcPath, fileName);
		try {
			return endpoint.open(filePath, inBuffSize);
		} catch(final IOException e) {
			fileIoTask.setStatus(FAIL_IO);
			throw new RuntimeException(e);
		}
	}

	@Override
	protected void invokeNio(final O ioTask) {
		if(ioTask instanceof DataIoTask) {
			invokeFileNio((DataIoTask<? extends DataItem>) ioTask);
		} else if(ioTask instanceof PathIoTask) {
			invokeDirectoryNio((PathIoTask<? extends PathItem>) ioTask);
		} else {
			throw new AssertionError("Not implemented");
		}
	}

	private void invokeFileNio(final DataIoTask<? extends DataItem> fileIoTask) {

		final IoType ioType = fileIoTask.getIoType();
		final DataItem fileItem = fileIoTask.getItem();

		FSDataInputStream input = null;
		FSDataOutputStream output = null;

		try {
			switch(ioType) {
				case NOOP:
					invokeNoop((O) fileIoTask);
					break;
				case CREATE:
					final List<? extends DataItem> srcItems = fileIoTask.getSrcItemsToConcat();
					if(srcItems != null) {
						invokeFileConcat(
							fileIoTask, fileItem, srcItems, endpoints.get(getNextEndpointAddr())
						);
					} else {
						input = fileInputStreams.computeIfAbsent(
							fileIoTask, this::getReadFileStream
						);
						output = fileOutputStreams.computeIfAbsent(
							fileIoTask, this::getCreateFileStream
						);
						if(input != null) {
							invokeFileCopy(fileIoTask, fileItem, input, output);
						} else {
							invokeFileCreate(fileIoTask, fileItem, output);
						}
					}
					break;
				case READ:
					input  = fileInputStreams.computeIfAbsent(fileIoTask, this::getReadFileStream);
					final List<Range> fixedRangesToRead = fileIoTask.getFixedRanges();
					if(verifyFlag) {
						try {
							if(fixedRangesToRead == null || fixedRangesToRead.isEmpty()) {
								if(fileIoTask.hasMarkedRanges()) {
									invokeFileReadAndVerifyRandomRanges(
										fileIoTask, fileItem, input,
										fileIoTask.getMarkedRangesMaskPair()
									);
								} else {
									invokeFileReadAndVerify(fileIoTask, fileItem, input);
								}
							} else {
								invokeFileReadAndVerifyFixedRanges(
									fileIoTask, fileItem, input, fixedRangesToRead
								);
							}
						} catch(final DataSizeException e) {
							fileIoTask.setStatus(IoTask.Status.RESP_FAIL_CORRUPT);
							final long
								countBytesDone = fileIoTask.getCountBytesDone() + e.getOffset();
							fileIoTask.setCountBytesDone(countBytesDone);
							Loggers.MSG.debug(
								"{}: content size mismatch, expected: {}, actual: {}",
								fileItem.getName(), fileItem.size(), countBytesDone
							);
						} catch(final DataCorruptionException e) {
							fileIoTask.setStatus(IoTask.Status.RESP_FAIL_CORRUPT);
							final long
								countBytesDone = fileIoTask.getCountBytesDone() + e.getOffset();
							fileIoTask.setCountBytesDone(countBytesDone);
							Loggers.MSG.debug(
								"{}: content mismatch @ offset {}, expected: {}, actual: {} ",
								fileItem.getName(), countBytesDone,
								String.format("\"0x%X\"", (int) (e.expected & 0xFF)),
								String.format("\"0x%X\"", (int) (e.actual & 0xFF))
							);
						}
					} else {
						if(fixedRangesToRead == null || fixedRangesToRead.isEmpty()) {
							if(fileIoTask.hasMarkedRanges()) {
								invokeFileReadRandomRanges(
									fileIoTask, fileItem, input,
									fileIoTask.getMarkedRangesMaskPair()
								);
							} else {
								invokeFileRead(fileIoTask, fileItem, input);
							}
						} else {
							invokeFileReadFixedRanges(
								fileIoTask, fileItem, input, fixedRangesToRead
							);
						}
					}
					break;
				case UPDATE:
					output = fileOutputStreams.computeIfAbsent(
						fileIoTask, this::getCreateFileStream
					);
					if(output == null) {
						break;
					}
					final List<Range> fixedRangesToUpdate = fileIoTask.getFixedRanges();
					if(fixedRangesToUpdate == null || fixedRangesToUpdate.isEmpty()) {
						if(fileIoTask.hasMarkedRanges()) {
							invokeFileRandomRangesUpdate(fileIoTask, fileItem, output);
						} else {
							throw new AssertionError("Not implemented");
						}
					} else {
						invokeFileFixedRangesUpdate(
							fileIoTask, fileItem, output, fixedRangesToUpdate
						);
					}
					break;
				case DELETE:
					invokeFileDelete(fileIoTask);
					break;
				case LIST:
					throw new AssertionError("Not implemented");
					break;
				default:
					throw new AssertionError("Not implemented");
			}
		} catch(final RuntimeException e) {
			final Throwable cause = e.getCause();
			final long countBytesDone = fileIoTask.getCountBytesDone();
			if(cause instanceof AccessControlException) {
				LogUtil.exception(
					Level.DEBUG, cause, "Access to the file is forbidden: {}", fileItem.getName()
				);
				fileItem.size(countBytesDone);
				finishIoTask((O) fileIoTask);
				fileIoTask.setStatus(IoTask.Status.RESP_FAIL_AUTH);
			} else if(cause instanceof IOException) {
				LogUtil.exception(
					Level.DEBUG, cause, "Failed open the file: {}", fileItem.getName()
				);
				fileItem.size(countBytesDone);
				finishIoTask((O) fileIoTask);
				fileIoTask.setStatus(FAIL_IO);
			} else if(cause instanceof URISyntaxException) {
				LogUtil.exception(Level.DEBUG, cause, "Failed to calculate the HDFS service URI");
				fileItem.size(countBytesDone);
				finishIoTask((O) fileIoTask);
				fileIoTask.setStatus(IoTask.Status.RESP_FAIL_CLIENT);
			} else if(cause != null) {
				LogUtil.exception(Level.DEBUG, cause, "Unexpected failure");
				fileItem.size(countBytesDone);
				finishIoTask((O) fileIoTask);
				fileIoTask.setStatus(IoTask.Status.FAIL_UNKNOWN);
			} else {
				LogUtil.exception(Level.DEBUG, e, "Unexpected failure");
				fileItem.size(countBytesDone);
				finishIoTask((O) fileIoTask);
				fileIoTask.setStatus(IoTask.Status.FAIL_UNKNOWN);
			}
		} finally {
			
			if(!ACTIVE.equals(fileIoTask.getStatus())) {

				if(input != null) {
					fileInputStreams.remove(fileIoTask);
					try {
						input.close();
					} catch(final IOException e) {
						Loggers.ERR.warn("Failed to close the source I/O channel");
					}
				}

				if(output != null) {
					fileOutputStreams.remove(fileIoTask);
					try {
						output.close();
					} catch(final IOException e) {
						Loggers.ERR.warn("Failed to close the destination I/O channel");
					}
				}
			}
		}
	}

	protected void invokeNoop(final O ioTask) {
		finishIoTask(ioTask);
	}

	protected void invokeFileCreate(
		final DataIoTask<? extends DataItem> fileIoTask, final DataItem fileItem,
		final FSDataOutputStream outputStream
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
				finishIoTask((O) fileIoTask);
			}
		} catch(final IOException e) {
			LogUtil.exception(
				Level.DEBUG, e, "Failed to write to the file: {}" + fileItem.getName()
			);
			finishIoTask((O) fileIoTask);
			fileIoTask.setStatus(FAIL_IO);
		}
	}

	protected void invokeFileCopy(
		final DataIoTask<? extends DataItem> fileIoTask, final DataItem fileItem,
		final FSDataInputStream inputStream, final FSDataOutputStream outputStream
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
				finishIoTask((O) fileIoTask);
				fileIoTask.setStatus(FAIL_IO);
			}
		}
		if(countBytesDone == fileSize) {
			finishIoTask((O) fileIoTask);
		}
	}

	protected void invokeFileConcat(
		final DataIoTask<? extends DataItem> fileIoTask, final DataItem fileItem,
		final List<? extends DataItem> srcItems, final FileSystem endpoint
	) {

		final String dstPath = fileIoTask.getDstPath();
		final int srcItemsCount = srcItems.size();
		final Path[] srcPaths = new Path[srcItems.size()];
		final String fileName = fileItem.getName();
		final Path dstFilePath = getFilePath(dstPath, fileName);
		DataItem srcItem;
		long dstItemSize = 0;

		try {
			for(int i = 0; i < srcItemsCount; i ++) {
				srcItem = srcItems.get(i);
				srcPaths[i] = getFilePath(dstPath, srcItem.getName());
				dstItemSize += srcItem.size();
			}
			endpoint
				.create(
					dstFilePath, defaultFsPerm, false, 0,
					endpoint.getDefaultReplication(dstFilePath), dstItemSize, null
				)
				.close();
			endpoint.concat(dstFilePath, srcPaths);
			finishIoTask((O) fileIoTask);
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

	private void invokeFileReadAndVerify(
		final DataIoTask<? extends DataItem> ioTask, final DataItem fileItem,
		final FSDataInputStream inputStream
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
			finishIoTask((O) ioTask);
		}
	}

	private void invokeFileReadAndVerifyRandomRanges(
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
			finishIoTask((O) ioTask);
		}
	}

	private void invokeFileReadAndVerifyFixedRanges(
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
						finishIoTask((O) ioTask);
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
			finishIoTask((O) ioTask);
		}
	}

	private void invokeFileRandomRangesUpdate(
		final DataIoTask<? extends DataItem> ioTask, final DataItem fileItem,
		final FSDataOutputStream outputStream
	) throws IOException {

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
			finishIoTask(ioTask);
			fileItem.commitUpdatedRanges(ioTask.getMarkedRangesMaskPair());
		}
	}

	private void invokeFixedRangesUpdate(
		final DataIoTask<? extends DataItem> ioTask, final DataItem fileItem,
		final FSDataOutputStream outputStream, final List<Range> byteRanges
	) throws IOException {

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
			finishIoTask(ioTask);
		}
	}

	protected void invokeFileDelete(final DataIoTask<? extends DataItem> fileIoTask) {
		final String endpointAddr = fileIoTask.getNodeAddr();
		final FileSystem endpoint = endpoints.get(endpointAddr);
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
				finishIoTask((O) fileIoTask);
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

	private void invokeDirectoryNio(final PathIoTask<? extends PathItem> dirIoTask) {

	}

	@Override
	protected String requestNewPath(final String path) {
		throw new AssertionError("Should not be invoked");
	}

	@Override
	protected String requestNewAuthToken(final Credential credential) {
		throw new AssertionError("Should not be invoked");
	}

	@Override
	public List<I> list(
		final ItemFactory<I> itemFactory, final String path, final String prefix, final int idRadix,
		final I lastPrevItem, final int count
	) throws IOException {

		final FileSystem endpoint = endpoints.values().iterator().next();
		final RemoteIterator<LocatedFileStatus> it = endpoint.listFiles(new Path(path), false);
		final List<I> items = new ArrayList<>(count);
		final int prefixLen = prefix == null ? 0 : prefix.length();

		final String lastPrevItemName;
		boolean lastPrevItemNameFound;
		if(lastPrevItem == null) {
			lastPrevItemName = null;
			lastPrevItemNameFound = true;
		} else {
			lastPrevItemName = lastPrevItem.getName();
			lastPrevItemNameFound = false;
		}

		long listedCount = 0;

		LocatedFileStatus lfs;
		Path nextPath;
		String nextPathStr;
		String nextName;
		long nextSize;
		long nextId;
		I nextFile;

		while(it.hasNext() && listedCount < count) {

			lfs = it.next();
			// skip directory entries
			if(lfs.isDirectory()) {
				continue;
			}
			nextPath = lfs.getPath();
			nextPathStr = nextPath.toUri().getPath();
			nextName = nextPath.getName();

			if(!lastPrevItemNameFound) {
				lastPrevItemNameFound = nextPathStr.equals(lastPrevItemName);
				continue;
			}

			try {
				if(prefixLen > 0) {
					// skip all files which not start with the given prefix
					if(!nextName.startsWith(prefix)) {
						continue;
					}
					nextId = Long.parseLong(nextName.substring(prefixLen), idRadix);
				} else {
					nextId = Long.parseLong(nextName, idRadix);
				}
			} catch(final NumberFormatException e) {
				// this allows to not to fail the listing even if it contains a file with incompatible name
				nextId = 0; // fallback value
			}
			nextSize = lfs.getLen();
			nextFile = itemFactory.getItem(nextPathStr, nextId, nextSize);
			items.add(nextFile);
			listedCount ++;
		}

		return items;
	}

	@Override
	public void adjustIoBuffers(final long avgTransferSize, final IoType ioType)
	throws RemoteException {
		int size;
		if(avgTransferSize < BUFF_SIZE_MIN) {
			size = BUFF_SIZE_MIN;
		} else if(BUFF_SIZE_MAX < avgTransferSize) {
			size = BUFF_SIZE_MAX;
		} else {
			size = (int) avgTransferSize;
		}
		if(IoType.CREATE.equals(ioType)) {
			Loggers.MSG.info("Adjust output buffer size: {}", SizeInBytes.formatFixedSize(size));
			outBuffSize = size;
		} else if(IoType.READ.equals(ioType)) {
			Loggers.MSG.info("Adjust input buffer size: {}", SizeInBytes.formatFixedSize(size));
			inBuffSize = size;
		}
	}

	@Override
	protected void doClose()
	throws IOException {

		super.doClose();

		hadoopConfig.clear();
		for(final FSDataInputStream input: fileInputStreams.values()) {
			input.close();
		}
		fileInputStreams.clear();
		for(final FSDataOutputStream output: fileOutputStreams.values()) {
			output.close();
		}
		fileOutputStreams.clear();
		for(final FileSystem endpoint: endpoints.values()) {
			endpoint.close();
		}
		endpoints.clear();
		for(int i = 0; i < endpointAddrs.length; i ++) {
			endpointAddrs[i] = null;
		}
	}
}
