package com.emc.mongoose.storage.driver.hdfs;

import static com.emc.mongoose.item.DataItem.rangeCount;
import static com.emc.mongoose.item.DataItem.rangeOffset;
import static com.emc.mongoose.item.io.task.IoTask.Status.ACTIVE;
import static com.emc.mongoose.item.io.task.IoTask.Status.FAIL_IO;
import com.emc.mongoose.data.DataCorruptionException;
import com.emc.mongoose.data.DataInput;
import com.emc.mongoose.data.DataSizeException;
import com.emc.mongoose.exception.OmgShootMyFootException;
import com.emc.mongoose.item.DataItem;
import com.emc.mongoose.item.Item;
import com.emc.mongoose.item.ItemFactory;
import com.emc.mongoose.item.PathItem;
import com.emc.mongoose.item.io.IoType;
import com.emc.mongoose.item.io.task.IoTask;
import com.emc.mongoose.item.io.task.data.DataIoTask;
import com.emc.mongoose.item.io.task.path.PathIoTask;
import com.emc.mongoose.logging.LogUtil;
import com.emc.mongoose.logging.Loggers;
import com.emc.mongoose.storage.Credential;
import com.emc.mongoose.storage.driver.coop.nio.NioStorageDriverBase;

import static com.github.akurilov.commons.system.DirectMemUtil.REUSABLE_BUFF_SIZE_MAX;
import com.github.akurilov.commons.collection.Range;
import com.github.akurilov.commons.io.util.OutputStreamWrapperChannel;
import com.github.akurilov.commons.system.DirectMemUtil;
import com.github.akurilov.commons.system.SizeInBytes;

import com.github.akurilov.confuse.Config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;

import org.apache.logging.log4j.Level;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class HdfsStorageDriver<I extends Item, O extends IoTask<I>>
extends NioStorageDriverBase<I, O> {

	protected final String uriSchema;
	protected final Configuration hadoopConfig;
	protected final FsPermission defaultFsPerm;
	protected final String[] endpointAddrs;
	protected final int nodePort;
	private final AtomicInteger rrc = new AtomicInteger(0);
	private final ConcurrentMap<DataIoTask<? extends DataItem>, FSDataInputStream>
		fileInputStreams = new ConcurrentHashMap<>();
	private final ConcurrentMap<DataIoTask<? extends DataItem>, FSDataOutputStream>
		fileOutputStreams = new ConcurrentHashMap<>();
	private final UserGroupInformation ugi;

	protected int inBuffSize = BUFF_SIZE_MIN;
	protected int outBuffSize = BUFF_SIZE_MAX;

	public HdfsStorageDriver(
		final String uriSchema, final String testStepId, final DataInput dataInput,
		final Config loadConfig, final Config storageConfig, final boolean verifyFlag
	) throws OmgShootMyFootException {

		super(testStepId, dataInput, loadConfig, storageConfig, verifyFlag);

		this.uriSchema = uriSchema;
		hadoopConfig = new Configuration();
		hadoopConfig.setClassLoader(getClass().getClassLoader());
		defaultFsPerm = FsPermission
			.getDefault()
			.applyUMask(FsPermission.getUMask(hadoopConfig));

		final String uid = credential == null ? null : credential.getUid();
		if(uid != null && !uid.isEmpty()) {
			ugi = UserGroupInformation.createRemoteUser(uid);
			UserGroupInformation.setLoginUser(ugi);
		} else {
			ugi = null;
		}

		final Config nodeConfig = storageConfig.configVal("net-node");
		nodePort = storageConfig.intVal("net-node-port");
		final List<String> endpointAddrList = nodeConfig.listVal("addrs");
		endpointAddrs = endpointAddrList.toArray(new String[endpointAddrList.size()]);
		requestAuthTokenFunc = null; // do not use
		requestNewPathFunc = null; // do not use
	}

	protected final String getNextEndpointAddr() {
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
			final URI endpointUri = new URI(uriSchema, uid, addr, port, "/", null, null);
			// set the temporary thread's context classloader
			Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
			return FileSystem.get(endpointUri, hadoopConfig);
		} catch(final URISyntaxException | IOException e) {
			throw new RuntimeException(e);
		} finally {
			// set the thread's context classloader back
			Thread.currentThread().setContextClassLoader(ClassLoader.getSystemClassLoader());
		}
	}

	@Override
	protected void prepareIoTask(final O ioTask) {
		super.prepareIoTask(ioTask);
		String endpointAddr = ioTask.nodeAddr();
		if(endpointAddr == null) {
			endpointAddr = getNextEndpointAddr();
			ioTask.nodeAddr(endpointAddr);
		}
	}

	protected static Path getFilePath(final String basePath, final String fileName) {
		if(basePath == null || basePath.isEmpty() || fileName.startsWith(basePath)) {
			return new Path(fileName);
		} else {
			return new Path(basePath, fileName);
		}
	}

	protected FSDataOutputStream getCreateFileStream(
		final DataIoTask<? extends DataItem> createFileTask
	) {
		final String dstPath = createFileTask.dstPath();
		final DataItem fileItem = createFileTask.item();
		final String fileName = fileItem.getName();
		final Path filePath = getFilePath(dstPath, fileName);
		final FileSystem endpoint = getEndpoint(createFileTask.nodeAddr());
		try {
			return endpoint.create(
				filePath, defaultFsPerm, false, outBuffSize,
				endpoint.getDefaultReplication(filePath), fileItem.size(), null
			);
		} catch(final IOException e) {
			createFileTask.status(FAIL_IO);
			throw new RuntimeException(e);
		}
	}

	protected FSDataInputStream getReadFileStream(
		final DataIoTask<? extends DataItem> readFileTask
	) {
		final String srcPath = readFileTask.srcPath();
		if(srcPath == null || srcPath.isEmpty()) {
			return null;
		}
		final DataItem fileItem = readFileTask.item();
		final String fileName = fileItem.getName();
		final Path filePath = getFilePath(srcPath, fileName);
		final FileSystem endpoint = getEndpoint(readFileTask.nodeAddr());
		try {
			return endpoint.open(filePath, inBuffSize);
		} catch(final IOException e) {
			readFileTask.status(FAIL_IO);
			throw new RuntimeException(e);
		}
	}

	protected FSDataOutputStream getUpdateFileStream(
		final DataIoTask<? extends DataItem> updateFileTask
	) {
		final String dstPath = updateFileTask.dstPath();
		final DataItem fileItem = updateFileTask.item();
		final String fileName = fileItem.getName();
		final Path filePath = getFilePath(dstPath, fileName);
		final FileSystem endpoint = getEndpoint(updateFileTask.nodeAddr());
		try {
			return endpoint.create(
				filePath, defaultFsPerm, true, outBuffSize,
				endpoint.getDefaultReplication(filePath), fileItem.size(), null
			);
		} catch(final IOException e) {
			updateFileTask.status(FAIL_IO);
			throw new RuntimeException(e);
		}
	}

	protected FSDataOutputStream getAppendFileStream(
		final DataIoTask<? extends DataItem> appendFileTask
	) {
		final String dstPath = appendFileTask.dstPath();
		final DataItem fileItem = appendFileTask.item();
		final String fileName = fileItem.getName();
		final Path filePath = getFilePath(dstPath, fileName);
		final FileSystem endpoint = getEndpoint(appendFileTask.nodeAddr());
		try {
			return endpoint.append(filePath, outBuffSize);
		} catch(final IOException e) {
			appendFileTask.status(FAIL_IO);
			throw new RuntimeException(e);
		}
	}

	@Override
	protected final void invokeNio(final O ioTask) {
		if(ioTask instanceof DataIoTask) {
			invokeFileNio((DataIoTask<? extends DataItem>) ioTask);
		} else if(ioTask instanceof PathIoTask) {
			invokeDirectoryNio((PathIoTask<? extends PathItem>) ioTask);
		} else {
			throw new AssertionError("Not implemented");
		}
	}

	private void invokeFileNio(final DataIoTask<? extends DataItem> fileIoTask) {

		final IoType ioType = fileIoTask.ioType();
		final DataItem fileItem = fileIoTask.item();

		FSDataInputStream input = null;
		FSDataOutputStream output = null;

		try {
			switch(ioType) {

				case NOOP:
					finishIoTask((O) fileIoTask);
					break;

				case CREATE:
					final List<? extends DataItem> srcItems = fileIoTask.srcItemsToConcat();
					if(srcItems != null) {
						throw new AssertionError("Files concatenation support is not implemented");
					} else {
						input = fileInputStreams.computeIfAbsent(
							fileIoTask, this::getReadFileStream
						);
						output = fileOutputStreams.computeIfAbsent(
							fileIoTask, this::getCreateFileStream
						);
						if(input != null) {
							if(invokeFileCopy(fileIoTask, fileItem, input, output)) {
								finishIoTask((O) fileIoTask);
							}
						} else {
							if(invokeFileCreate(fileIoTask, fileItem, output)) {
								finishIoTask((O) fileIoTask);
							}
						}
					}
					break;

				case READ:
					input = fileInputStreams.computeIfAbsent(fileIoTask, this::getReadFileStream);
					final List<Range> fixedRangesToRead = fileIoTask.fixedRanges();
					if(verifyFlag) {
						try {
							if(fixedRangesToRead == null || fixedRangesToRead.isEmpty()) {
								if(fileIoTask.hasMarkedRanges()) {
									if(
										invokeFileReadAndVerifyRandomRanges(
											fileIoTask, fileItem, input,
											fileIoTask.markedRangesMaskPair()
										)
									) {
										finishIoTask((O) fileIoTask);
									}
								} else {
									if(invokeFileReadAndVerify(fileIoTask, fileItem, input)) {
										finishIoTask((O) fileIoTask);
									}
								}
							} else {
								if(
									invokeFileReadAndVerifyFixedRanges(
										fileIoTask, fileItem, input, fixedRangesToRead
									)
								) {
									finishIoTask((O) fileIoTask);
								}
							}
						} catch(final DataSizeException e) {
							fileIoTask.status(IoTask.Status.RESP_FAIL_CORRUPT);
							final long countBytesDone = fileIoTask.countBytesDone()
								+ e.getOffset();
							fileIoTask.countBytesDone(countBytesDone);
							try {
								Loggers.MSG.debug(
									"{}: content size mismatch, expected: {}, actual: {}",
									fileItem.getName(), fileItem.size(), countBytesDone
								);
							} catch(final IOException ignored) {
							}
						} catch(final DataCorruptionException e) {
							fileIoTask.status(IoTask.Status.RESP_FAIL_CORRUPT);
							final long countBytesDone = fileIoTask.countBytesDone() + e.getOffset();
							fileIoTask.countBytesDone(countBytesDone);
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
								if(
									invokeFileReadRandomRanges(
										fileIoTask, fileItem, input,
										fileIoTask.markedRangesMaskPair()
									)
								) {
									fileIoTask.countBytesDone(fileIoTask.markedRangesSize());
									finishIoTask((O) fileIoTask);
								}
							} else {
								if(invokeFileRead(fileIoTask, fileItem, input)) {
									finishIoTask((O) fileIoTask);
								}
							}
						} else {
							if(
								invokeFileReadFixedRanges(
									fileIoTask, fileItem, input, fixedRangesToRead
								)
							) {
								finishIoTask((O) fileIoTask);
							}
						}
					}
					break;

				case UPDATE:
					final List<Range> fixedRangesToUpdate = fileIoTask.fixedRanges();
					if(fixedRangesToUpdate == null || fixedRangesToUpdate.isEmpty()) {
						if(fileIoTask.hasMarkedRanges()) {
							throw new AssertionError("Random byte ranges update isn't implemented");
						} else {
							// overwrite the file
							output = fileOutputStreams.computeIfAbsent(
								fileIoTask, this::getUpdateFileStream
							);
							if(invokeFileCreate(fileIoTask, fileItem, output)) {
								finishIoTask((O) fileIoTask);
							}
						}
					} else {
						if(fixedRangesToUpdate.size() == 1) {
							final Range range = fixedRangesToUpdate.get(0);
							if(range.getBeg() == 0 && range.getEnd() == fileItem.size() - 1) {
								// overwrite the file
								output = fileOutputStreams.computeIfAbsent(
									fileIoTask, this::getUpdateFileStream
								);
								if(invokeFileCreate(fileIoTask, fileItem, output)) {
									finishIoTask((O) fileIoTask);
								}
							} else if(range.getSize() > 0) {
								// append
								output = fileOutputStreams.computeIfAbsent(
									fileIoTask, this::getAppendFileStream
								);
								if(invokeFileAppend(fileIoTask, fileItem, output, range)) {
									finishIoTask((O) fileIoTask);
								}
							} else {
								throw new AssertionError(
									"Custom fixed byte ranges update isn't implemented"
								);
							}
						} else {
							throw new AssertionError(
								"Multiple fixed byte ranges update isn't implemented"
							);
						}
					}
					break;

				case DELETE:
					if(invokeFileDelete(fileIoTask)) {
						finishIoTask((O) fileIoTask);
					}
					break;

				case LIST:
				default:
					throw new AssertionError("\"" + ioType + "\" operation isn't implemented");
			}
		} catch(final IOException e) {
			LogUtil.exception(
				Level.DEBUG, e, "I/O failure, operation: {}, file: {}", ioType, fileItem.getName()
			);
			finishIoTask((O) fileIoTask);
			fileIoTask.status(FAIL_IO);
		} catch(final RuntimeException e) {
			final Throwable cause = e.getCause();
			final long countBytesDone = fileIoTask.countBytesDone();
			if(cause instanceof AccessControlException) {
				LogUtil.exception(
					Level.DEBUG, cause, "Access to the file is forbidden: {}", fileItem.getName()
				);
				fileItem.size(countBytesDone);
				finishIoTask((O) fileIoTask);
				fileIoTask.status(IoTask.Status.RESP_FAIL_AUTH);
			} else if(cause instanceof IOException) {
				LogUtil.exception(
					Level.DEBUG, cause, "Failed open the file: {}", fileItem.getName()
				);
				fileItem.size(countBytesDone);
				finishIoTask((O) fileIoTask);
				fileIoTask.status(FAIL_IO);
			} else if(cause instanceof URISyntaxException) {
				LogUtil.exception(Level.DEBUG, cause, "Failed to calculate the HDFS service URI");
				fileItem.size(countBytesDone);
				finishIoTask((O) fileIoTask);
				fileIoTask.status(IoTask.Status.RESP_FAIL_CLIENT);
			} else if(cause != null) {
				LogUtil.exception(Level.DEBUG, cause, "Unexpected failure");
				fileItem.size(countBytesDone);
				finishIoTask((O) fileIoTask);
				fileIoTask.status(IoTask.Status.FAIL_UNKNOWN);
			} else {
				LogUtil.exception(Level.DEBUG, e, "Unexpected failure");
				fileItem.size(countBytesDone);
				finishIoTask((O) fileIoTask);
				fileIoTask.status(IoTask.Status.FAIL_UNKNOWN);
			}
		} finally {
			if(!ACTIVE.equals(fileIoTask.status())) {
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

	private void invokeDirectoryNio(final PathIoTask<? extends PathItem> dirIoTask) {
		throw new AssertionError("Not implemented yet");
	}

	@Override
	protected String requestNewPath(final String path) {
		throw new AssertionError("Should not be invoked");
	}

	@Override
	protected String requestNewAuthToken(final Credential credential) {
		throw new AssertionError("Should not be invoked");
	}

	protected boolean invokeFileCreate(
		final DataIoTask<? extends DataItem> fileIoTask, final DataItem fileItem,
		final FSDataOutputStream outputStream
	) throws IOException {

		final long fileSize;
		try {
			fileSize = fileItem.size();
		} catch(final IOException e) {
			throw new AssertionError(e);
		}
		long countBytesDone = fileIoTask.countBytesDone();
		final long remainingBytes = fileSize - countBytesDone;

		if(remainingBytes > 0) {
			final WritableByteChannel outputChan = OutputStreamWrapperChannel
				.getThreadLocalInstance(outputStream, remainingBytes);
			countBytesDone += fileItem.writeToSocketChannel(outputChan, remainingBytes);
			outputStream.hflush();
			fileIoTask.countBytesDone(countBytesDone);
		}

		return remainingBytes <= 0;
	}

	protected boolean invokeFileCopy(
		final DataIoTask<? extends DataItem> fileIoTask, final DataItem fileItem,
		final FSDataInputStream inputStream, final FSDataOutputStream outputStream
	) throws IOException {

		long countBytesDone = fileIoTask.countBytesDone();
		final long fileSize;
		try {
			fileSize = fileItem.size();
		} catch(final IOException e) {
			throw new AssertionError(e);
		}
		final long remainingSize = fileSize - countBytesDone;

		if(remainingSize > 0 && ACTIVE.equals(fileIoTask.status())) {
			final byte[] buff = new byte[
				remainingSize > REUSABLE_BUFF_SIZE_MAX ?
					REUSABLE_BUFF_SIZE_MAX : (int) remainingSize
			];
			final int n = inputStream.read(buff, 0, buff.length);
			outputStream.write(buff, 0, n);
			outputStream.hflush();
			countBytesDone += n;
			fileIoTask.countBytesDone(countBytesDone);
		}

		return countBytesDone >= fileSize;
	}

	/*protected boolean invokeFileConcat(
		final DataIoTask<? extends DataItem> fileIoTask, final DataItem fileItem,
		final List<? extends DataItem> srcItems, final FileSystem endpoint,
		final FsPermission fsPerm
	) throws IOException {

		final String dstPath = fileIoTask.dstPath();
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

	protected boolean invokeFileReadAndVerify(
		final DataIoTask<? extends DataItem> ioTask, final DataItem fileItem,
		final FSDataInputStream inputStream

	) throws DataSizeException, DataCorruptionException, IOException {
		long countBytesDone = ioTask.countBytesDone();
		final long contentSize = fileItem.size();

		if(countBytesDone < contentSize) {
			if(fileItem.isUpdated()) {
				final DataItem currRange = ioTask.currRange();
				final int nextRangeIdx = ioTask.currRangeIdx() + 1;
				final long nextRangeOffset = rangeOffset(nextRangeIdx);
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
							ioTask.currRangeIdx(nextRangeIdx);
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
			ioTask.countBytesDone(countBytesDone);
		}

		return countBytesDone >= contentSize;
	}

	protected boolean invokeFileReadAndVerifyRandomRanges(
		final DataIoTask<? extends DataItem> ioTask, final DataItem fileItem,
		final FSDataInputStream inputStream, final BitSet maskRangesPair[]
	) throws DataSizeException, DataCorruptionException, IOException {

		long countBytesDone = ioTask.countBytesDone();
		final long rangesSizeSum = ioTask.markedRangesSize();

		if(rangesSizeSum > 0 && rangesSizeSum > countBytesDone) {

			DataItem range2read;
			int currRangeIdx;
			while(true) {
				currRangeIdx = ioTask.currRangeIdx();
				if(currRangeIdx < rangeCount(fileItem.size())) {
					if(maskRangesPair[0].get(currRangeIdx) || maskRangesPair[1].get(currRangeIdx)) {
						range2read = ioTask.currRange();
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
						ioTask.currRangeIdx(++ currRangeIdx);
					}
				} else {
					ioTask.countBytesDone(rangesSizeSum);
					return true;
				}
			}

			final long currRangeSize = range2read.size();
			final long currPos = rangeOffset(currRangeIdx) + countBytesDone;
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
				ioTask.currRangeIdx(currRangeIdx + 1);
				ioTask.countBytesDone(0);
			} else {
				ioTask.countBytesDone(countBytesDone);
			}
		}

		return rangesSizeSum <= 0 || rangesSizeSum <= countBytesDone;
	}

	protected boolean invokeFileReadAndVerifyFixedRanges(
		final DataIoTask<? extends DataItem> ioTask, final DataItem fileItem,
		final FSDataInputStream inputStream, final List<Range> fixedRanges
	) throws DataSizeException, DataCorruptionException, IOException {

		final long baseItemSize = fileItem.size();
		final long fixedRangesSizeSum = ioTask.markedRangesSize();

		long countBytesDone = ioTask.countBytesDone();
		// "countBytesDone" is the current range done bytes counter here
		long rangeBytesDone = countBytesDone;
		long currOffset;
		long cellOffset;
		long cellEnd;
		int n;

		if(fixedRangesSizeSum > 0 && fixedRangesSizeSum > countBytesDone) {

			Range fixedRange;
			DataItem currRange;
			int currFixedRangeIdx = ioTask.currRangeIdx();
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
				n = rangeCount(currOffset + 1) - 1;
				cellOffset = rangeOffset(n);
				cellEnd = Math.min(baseItemSize, rangeOffset(n + 1));
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
						ioTask.countBytesDone(fixedRangesSizeSum);
						return true;
					} else {
						ioTask.currRangeIdx(currFixedRangeIdx + 1);
						rangeBytesDone = 0;
					}
				}
				ioTask.countBytesDone(rangeBytesDone);
			} else {
				ioTask.countBytesDone(fixedRangesSizeSum);
			}
		}

		return fixedRangesSizeSum <= 0 || fixedRangesSizeSum <= countBytesDone;
	}

	protected boolean invokeFileRead(
		final DataIoTask<? extends DataItem> ioTask, final DataItem fileItem,
		final FSDataInputStream inputStream
	) throws IOException {
		long countBytesDone = ioTask.countBytesDone();
		final long contentSize = fileItem.size();
		int n;
		if(countBytesDone < contentSize) {
			n = inputStream.read(
				DirectMemUtil.getThreadLocalReusableBuff(contentSize - countBytesDone)
			);
			if(n < 0) {
				ioTask.countBytesDone(countBytesDone);
				fileItem.size(countBytesDone);
				return true;
			} else {
				countBytesDone += n;
				ioTask.countBytesDone(countBytesDone);
			}
		}

		return countBytesDone >= contentSize;
	}

	protected boolean invokeFileReadRandomRanges(
		final DataIoTask<? extends DataItem> ioTask, final DataItem fileItem,
		final FSDataInputStream inputStream, final BitSet maskRangesPair[]
	) throws IOException {

		int n;
		long countBytesDone = ioTask.countBytesDone();
		final long rangesSizeSum = ioTask.markedRangesSize();

		if(rangesSizeSum > 0 && rangesSizeSum > countBytesDone) {

			DataItem range2read;
			int currRangeIdx;
			while(true) {
				currRangeIdx = ioTask.currRangeIdx();
				if(currRangeIdx < rangeCount(fileItem.size())) {
					if(maskRangesPair[0].get(currRangeIdx) || maskRangesPair[1].get(currRangeIdx)) {
						range2read = ioTask.currRange();
						break;
					} else {
						ioTask.currRangeIdx(++ currRangeIdx);
					}
				} else {
					ioTask.countBytesDone(rangesSizeSum);
					return true;
				}
			}

			final long currRangeSize = range2read.size();
			inputStream.seek(rangeOffset(currRangeIdx) + countBytesDone);
			n = inputStream.read(
				DirectMemUtil.getThreadLocalReusableBuff(currRangeSize - countBytesDone)
			);
			if(n < 0) {
				ioTask.countBytesDone(countBytesDone);
				return true;
			}
			countBytesDone += n;

			if(countBytesDone == currRangeSize) {
				ioTask.currRangeIdx(currRangeIdx + 1);
				ioTask.countBytesDone(0);
			} else {
				ioTask.countBytesDone(countBytesDone);
			}
		}

		return rangesSizeSum <= 0 || rangesSizeSum <= countBytesDone;
	}

	protected boolean invokeFileReadFixedRanges(
		final DataIoTask<? extends DataItem> ioTask, final DataItem fileItem,
		final FSDataInputStream inputStream, final List<Range> byteRanges
	) throws IOException {

		int n;
		long countBytesDone = ioTask.countBytesDone();
		final long baseItemSize = fileItem.size();
		final long rangesSizeSum = ioTask.markedRangesSize();

		if(rangesSizeSum > 0 && rangesSizeSum > countBytesDone) {

			Range byteRange;
			int currRangeIdx = ioTask.currRangeIdx();
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
					ioTask.countBytesDone(countBytesDone);
					return true;
				}
				countBytesDone += n;

				if(countBytesDone == rangeSize) {
					ioTask.currRangeIdx(currRangeIdx + 1);
					ioTask.countBytesDone(0);
				} else {
					ioTask.countBytesDone(countBytesDone);
				}
			} else {
				ioTask.countBytesDone(rangesSizeSum);
			}
		}

		return rangesSizeSum <= 0 || rangesSizeSum <= countBytesDone;
	}

	protected boolean invokeFileAppend(
		final DataIoTask<? extends DataItem> ioTask, final DataItem fileItem,
		final FSDataOutputStream outputStream, final Range appendRange
	) throws IOException {

		final long countBytesDone = ioTask.countBytesDone();
		final long appendSize = appendRange.getSize();
		final long remainingSize = appendSize - countBytesDone;
		long n;

		if(remainingSize > 0) {
			final WritableByteChannel outputChan = OutputStreamWrapperChannel
				.getThreadLocalInstance(outputStream, remainingSize);
			n = fileItem.writeToSocketChannel(outputChan, remainingSize);
			outputStream.hflush();
			ioTask.countBytesDone(countBytesDone + n);
			fileItem.size(fileItem.size() + n);
		}

		return remainingSize <= 0;
	}

	protected boolean invokeFileDelete(final DataIoTask<? extends DataItem> fileIoTask)
	throws IOException {

		final String dstPath = fileIoTask.dstPath();
		final DataItem fileItem = fileIoTask.item();
		final String itemName = fileItem.getName();
		final Path filePath = getFilePath(dstPath, itemName);
		final FileSystem endpoint = getEndpoint(getNextEndpointAddr());

		if(!endpoint.delete(filePath, false)) {
			Loggers.ERR.debug(
				"Failed to delete the file {} @ {}", filePath,
				endpoint.getCanonicalServiceName()
			);
			fileIoTask.startResponse();
			fileIoTask.finishResponse();
			fileIoTask.status(IoTask.Status.RESP_FAIL_UNKNOWN);
		}

		return true;
	}

	@Override
	public List<I> list(
		final ItemFactory<I> itemFactory, final String path, final String prefix, final int idRadix,
		final I lastPrevItem, final int count
	) throws IOException {
		return ListHelper.list(
			itemFactory, path, prefix, idRadix, lastPrevItem, count, getEndpoint(endpointAddrs[0])
		);
	}

	@Override
	public void adjustIoBuffers(final long avgTransferSize, final IoType ioType) {
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
		for(int i = 0; i < endpointAddrs.length; i ++) {
			endpointAddrs[i] = null;
		}
		if(ugi != null) {
			FileSystem.closeAllForUGI(ugi);
		} else {
			FileSystem.closeAll();
		}
	}

	@Override
	public String toString() {
		return String.format(super.toString(), "hdfs");
	}
}
