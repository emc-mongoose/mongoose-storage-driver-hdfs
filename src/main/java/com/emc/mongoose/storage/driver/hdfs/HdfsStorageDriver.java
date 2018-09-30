package com.emc.mongoose.storage.driver.hdfs;

import com.emc.mongoose.data.DataCorruptionException;
import com.emc.mongoose.data.DataInput;
import com.emc.mongoose.data.DataSizeException;
import com.emc.mongoose.exception.OmgShootMyFootException;
import com.emc.mongoose.item.DataItem;
import com.emc.mongoose.item.Item;
import com.emc.mongoose.item.ItemFactory;
import com.emc.mongoose.item.PathItem;
import com.emc.mongoose.item.op.OpType;
import com.emc.mongoose.item.op.Operation;
import com.emc.mongoose.item.op.data.DataOperation;
import com.emc.mongoose.item.op.path.PathOperation;
import com.emc.mongoose.logging.LogUtil;
import com.emc.mongoose.logging.Loggers;
import com.emc.mongoose.storage.Credential;
import com.emc.mongoose.storage.driver.coop.nio.NioStorageDriverBase;
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

import static com.emc.mongoose.item.DataItem.rangeCount;
import static com.emc.mongoose.item.DataItem.rangeOffset;
import static com.emc.mongoose.item.op.Operation.Status.ACTIVE;
import static com.emc.mongoose.item.op.Operation.Status.FAIL_IO;
import static com.emc.mongoose.item.op.Operation.Status.FAIL_UNKNOWN;
import static com.emc.mongoose.item.op.Operation.Status.RESP_FAIL_AUTH;
import static com.emc.mongoose.item.op.Operation.Status.RESP_FAIL_CLIENT;
import static com.emc.mongoose.item.op.Operation.Status.RESP_FAIL_CORRUPT;
import static com.emc.mongoose.item.op.Operation.Status.RESP_FAIL_UNKNOWN;
import static com.github.akurilov.commons.system.DirectMemUtil.REUSABLE_BUFF_SIZE_MAX;

public class HdfsStorageDriver<I extends Item, O extends Operation<I>>
extends NioStorageDriverBase<I, O> {

	protected final String uriSchema;
	protected final Configuration hadoopConfig;
	protected final FsPermission defaultFsPerm;
	protected final String[] endpointAddrs;
	protected final int nodePort;
	private final AtomicInteger rrc = new AtomicInteger(0);
	private final ConcurrentMap<DataOperation<? extends DataItem>, FSDataInputStream>
		fileInputStreams = new ConcurrentHashMap<>();
	private final ConcurrentMap<DataOperation<? extends DataItem>, FSDataOutputStream>
		fileOutputStreams = new ConcurrentHashMap<>();
	private final UserGroupInformation ugi;
	protected int inBuffSize = BUFF_SIZE_MIN;
	protected int outBuffSize = BUFF_SIZE_MAX;

	public HdfsStorageDriver(
		final String uriSchema, final String testStepId, final DataInput dataInput,
		final Config storageConfig, final boolean verifyFlag, final int batchSize
	)
	throws OmgShootMyFootException {
		super(testStepId, dataInput, storageConfig, verifyFlag, batchSize);
		this.uriSchema = uriSchema;
		hadoopConfig = new Configuration();
		hadoopConfig.setClassLoader(getClass().getClassLoader());
		defaultFsPerm = FsPermission
			.getDefault()
			.applyUMask(FsPermission.getUMask(hadoopConfig));
		final String uid = credential == null ? null : credential.getUid();
		if(uid != null && ! uid.isEmpty()) {
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
	protected void prepare(final O operation) {
		super.prepare(operation);
		String endpointAddr = operation.nodeAddr();
		if(endpointAddr == null) {
			endpointAddr = getNextEndpointAddr();
			operation.nodeAddr(endpointAddr);
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
		final DataOperation<? extends DataItem> createFileTask
	) {
		final String dstPath = createFileTask.dstPath();
		final DataItem fileItem = createFileTask.item();
		final String fileName = fileItem.name();
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
		final DataOperation<? extends DataItem> readFileTask
	) {
		final String srcPath = readFileTask.srcPath();
		if(srcPath == null || srcPath.isEmpty()) {
			return null;
		}
		final DataItem fileItem = readFileTask.item();
		final String fileName = fileItem.name();
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
		final DataOperation<? extends DataItem> updateFileTask
	) {
		final String dstPath = updateFileTask.dstPath();
		final DataItem fileItem = updateFileTask.item();
		final String fileName = fileItem.name();
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
		final DataOperation<? extends DataItem> appendFileTask
	) {
		final String dstPath = appendFileTask.dstPath();
		final DataItem fileItem = appendFileTask.item();
		final String fileName = fileItem.name();
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
	protected final void invokeNio(final O operation) {
		if(operation instanceof DataOperation) {
			invokeFileNio((DataOperation<? extends DataItem>) operation);
		} else if(operation instanceof PathOperation) {
			invokeDirectoryNio((PathOperation<? extends PathItem>) operation);
		} else {
			throw new AssertionError("Not implemented");
		}
	}

	private void invokeFileNio(final DataOperation<? extends DataItem> fileOperation) {
		final OpType opType = fileOperation.type();
		final DataItem fileItem = fileOperation.item();
		FSDataInputStream input = null;
		FSDataOutputStream output = null;
		try {
			switch(opType) {
				case NOOP:
					finishOperation((O) fileOperation);
					break;
				case CREATE:
					final List<? extends DataItem> srcItems = fileOperation.srcItemsToConcat();
					if(srcItems != null) {
						throw new AssertionError("Files concatenation support is not implemented");
					} else {
						input = fileInputStreams.computeIfAbsent(
							fileOperation, this::getReadFileStream
						);
						output = fileOutputStreams.computeIfAbsent(
							fileOperation, this::getCreateFileStream
						);
						if(input != null) {
							if(invokeFileCopy(fileOperation, fileItem, input, output)) {
								finishOperation((O) fileOperation);
							}
						} else {
							if(invokeFileCreate(fileOperation, fileItem, output)) {
								finishOperation((O) fileOperation);
							}
						}
					}
					break;
				case READ:
					input = fileInputStreams.computeIfAbsent(fileOperation, this::getReadFileStream);
					final List<Range> fixedRangesToRead = fileOperation.fixedRanges();
					if(verifyFlag) {
						try {
							if(fixedRangesToRead == null || fixedRangesToRead.isEmpty()) {
								if(fileOperation.hasMarkedRanges()) {
									if(
										invokeFileReadAndVerifyRandomRanges(
											fileOperation, fileItem, input,
											fileOperation.markedRangesMaskPair()
										)
									) {
										finishOperation((O) fileOperation);
									}
								} else {
									if(invokeFileReadAndVerify(fileOperation, fileItem, input)) {
										finishOperation((O) fileOperation);
									}
								}
							} else {
								if(
									invokeFileReadAndVerifyFixedRanges(
										fileOperation, fileItem, input, fixedRangesToRead
									)
								) {
									finishOperation((O) fileOperation);
								}
							}
						} catch(final DataSizeException e) {
							fileOperation.status(RESP_FAIL_CORRUPT);
							final long countBytesDone = fileOperation.countBytesDone() + e.getOffset();
							fileOperation.countBytesDone(countBytesDone);
							try {
								Loggers.MSG.debug(
									"{}: content size mismatch, expected: {}, actual: {}",
									fileItem.name(), fileItem.size(), countBytesDone
								);
							} catch(final IOException ignored) {
							}
						} catch(final DataCorruptionException e) {
							fileOperation.status(RESP_FAIL_CORRUPT);
							final long countBytesDone = fileOperation.countBytesDone() + e.getOffset();
							fileOperation.countBytesDone(countBytesDone);
							Loggers.MSG.debug(
								"{}: content mismatch @ offset {}, expected: {}, actual: {} ",
								fileItem.name(), countBytesDone,
								String.format("\"0x%X\"", (int) (e.expected & 0xFF)),
								String.format("\"0x%X\"", (int) (e.actual & 0xFF))
							);
						}
					} else {
						if(fixedRangesToRead == null || fixedRangesToRead.isEmpty()) {
							if(fileOperation.hasMarkedRanges()) {
								if(
									invokeFileReadRandomRanges(
										fileOperation, fileItem, input,
										fileOperation.markedRangesMaskPair()
									)
								) {
									fileOperation.countBytesDone(fileOperation.markedRangesSize());
									finishOperation((O) fileOperation);
								}
							} else {
								if(invokeFileRead(fileOperation, fileItem, input)) {
									finishOperation((O) fileOperation);
								}
							}
						} else {
							if(
								invokeFileReadFixedRanges(
									fileOperation, fileItem, input, fixedRangesToRead
								)
							) {
								finishOperation((O) fileOperation);
							}
						}
					}
					break;
				case UPDATE:
					final List<Range> fixedRangesToUpdate = fileOperation.fixedRanges();
					if(fixedRangesToUpdate == null || fixedRangesToUpdate.isEmpty()) {
						if(fileOperation.hasMarkedRanges()) {
							throw new AssertionError("Random byte ranges update isn't implemented");
						} else {
							// overwrite the file
							output = fileOutputStreams.computeIfAbsent(
								fileOperation, this::getUpdateFileStream
							);
							if(invokeFileCreate(fileOperation, fileItem, output)) {
								finishOperation((O) fileOperation);
							}
						}
					} else {
						if(fixedRangesToUpdate.size() == 1) {
							final Range range = fixedRangesToUpdate.get(0);
							if(range.getBeg() == 0 && range.getEnd() == fileItem.size() - 1) {
								// overwrite the file
								output = fileOutputStreams.computeIfAbsent(
									fileOperation, this::getUpdateFileStream
								);
								if(invokeFileCreate(fileOperation, fileItem, output)) {
									finishOperation((O) fileOperation);
								}
							} else if(range.getSize() > 0) {
								// append
								output = fileOutputStreams.computeIfAbsent(
									fileOperation, this::getAppendFileStream
								);
								if(invokeFileAppend(fileOperation, fileItem, output, range)) {
									finishOperation((O) fileOperation);
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
					if(invokeFileDelete(fileOperation)) {
						finishOperation((O) fileOperation);
					}
					break;
				case LIST:
				default:
					throw new AssertionError("\"" + opType + "\" operation isn't implemented");
			}
		} catch(final IOException e) {
			LogUtil.exception(
				Level.DEBUG, e, "I/O failure, operation: {}, file: {}", opType, fileItem.name()
			);
			finishOperation((O) fileOperation);
			fileOperation.status(FAIL_IO);
		} catch(final RuntimeException e) {
			final Throwable cause = e.getCause();
			final long countBytesDone = fileOperation.countBytesDone();
			if(cause instanceof AccessControlException) {
				LogUtil.exception(
					Level.DEBUG, cause, "Access to the file is forbidden: {}", fileItem.name()
				);
				fileItem.size(countBytesDone);
				finishOperation((O) fileOperation);
				fileOperation.status(RESP_FAIL_AUTH);
			} else if(cause instanceof IOException) {
				LogUtil.exception(
					Level.DEBUG, cause, "Failed open the file: {}", fileItem.name()
				);
				fileItem.size(countBytesDone);
				finishOperation((O) fileOperation);
				fileOperation.status(FAIL_IO);
			} else if(cause instanceof URISyntaxException) {
				LogUtil.exception(Level.DEBUG, cause, "Failed to calculate the HDFS service URI");
				fileItem.size(countBytesDone);
				finishOperation((O) fileOperation);
				fileOperation.status(RESP_FAIL_CLIENT);
			} else if(cause != null) {
				LogUtil.exception(Level.DEBUG, cause, "Unexpected failure");
				fileItem.size(countBytesDone);
				finishOperation((O) fileOperation);
				fileOperation.status(FAIL_UNKNOWN);
			} else {
				LogUtil.exception(Level.DEBUG, e, "Unexpected failure");
				fileItem.size(countBytesDone);
				finishOperation((O) fileOperation);
				fileOperation.status(FAIL_UNKNOWN);
			}
		} finally {
			if(! ACTIVE.equals(fileOperation.status())) {
				if(input != null) {
					fileInputStreams.remove(fileOperation);
					try {
						input.close();
					} catch(final IOException e) {
						Loggers.ERR.warn("Failed to close the source I/O channel");
					}
				}
				if(output != null) {
					fileOutputStreams.remove(fileOperation);
					try {
						output.close();
					} catch(final IOException e) {
						Loggers.ERR.warn("Failed to close the destination I/O channel");
					}
				}
			}
		}
	}

	private void invokeDirectoryNio(final PathOperation<? extends PathItem> diroperation) {
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
		final DataOperation<? extends DataItem> fileOperation, final DataItem fileItem,
		final FSDataOutputStream outputStream
	)
	throws IOException {
		final long fileSize;
		try {
			fileSize = fileItem.size();
		} catch(final IOException e) {
			throw new AssertionError(e);
		}
		long countBytesDone = fileOperation.countBytesDone();
		final long remainingBytes = fileSize - countBytesDone;
		if(remainingBytes > 0) {
			final WritableByteChannel outputChan = OutputStreamWrapperChannel
				.getThreadLocalInstance(outputStream, remainingBytes);
			countBytesDone += fileItem.writeToSocketChannel(outputChan, remainingBytes);
			outputStream.hflush();
			fileOperation.countBytesDone(countBytesDone);
		}
		return remainingBytes <= 0;
	}

	protected boolean invokeFileCopy(
		final DataOperation<? extends DataItem> fileOperation, final DataItem fileItem,
		final FSDataInputStream inputStream, final FSDataOutputStream outputStream
	)
	throws IOException {
		long countBytesDone = fileOperation.countBytesDone();
		final long fileSize;
		try {
			fileSize = fileItem.size();
		} catch(final IOException e) {
			throw new AssertionError(e);
		}
		final long remainingSize = fileSize - countBytesDone;
		if(remainingSize > 0 && ACTIVE.equals(fileOperation.status())) {
			final byte[] buff = new byte[
				remainingSize > REUSABLE_BUFF_SIZE_MAX ?
				REUSABLE_BUFF_SIZE_MAX : (int) remainingSize
				];
			final int n = inputStream.read(buff, 0, buff.length);
			outputStream.write(buff, 0, n);
			outputStream.hflush();
			countBytesDone += n;
			fileOperation.countBytesDone(countBytesDone);
		}
		return countBytesDone >= fileSize;
	}

	/*protected boolean invokeFileConcat(
		final DataOperation<? extends DataItem> fileOperation, final DataItem fileItem,
		final List<? extends DataItem> srcItems, final FileSystem endpoint,
		final FsPermission fsPerm
	) throws IOException {

		final String dstPath = fileOperation.dstPath();
		final int srcItemsCount = srcItems.size();
		final Path[] srcPaths = new Path[srcItems.size()];
		final String fileName = fileItem.name();
		final Path dstFilePath = driver.getFilePath(dstPath, fileName);
		DataItem srcItem;
		long dstItemSize = 0;

		for(int i = 0; i < srcItemsCount; i ++) {
			srcItem = srcItems.get(i);
			srcPaths[i] = driver.getFilePath(dstPath, srcItem.name());
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
		final DataOperation<? extends DataItem> operation, final DataItem fileItem,
		final FSDataInputStream inputStream
	)
	throws DataSizeException, DataCorruptionException, IOException {
		long countBytesDone = operation.countBytesDone();
		final long contentSize = fileItem.size();
		if(countBytesDone < contentSize) {
			if(fileItem.isUpdated()) {
				final DataItem currRange = operation.currRange();
				final int nextRangeIdx = operation.currRangeIdx() + 1;
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
							operation.currRangeIdx(nextRangeIdx);
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
			operation.countBytesDone(countBytesDone);
		}
		return countBytesDone >= contentSize;
	}

	protected boolean invokeFileReadAndVerifyRandomRanges(
		final DataOperation<? extends DataItem> operation, final DataItem fileItem,
		final FSDataInputStream inputStream, final BitSet maskRangesPair[]
	)
	throws DataSizeException, DataCorruptionException, IOException {
		long countBytesDone = operation.countBytesDone();
		final long rangesSizeSum = operation.markedRangesSize();
		if(rangesSizeSum > 0 && rangesSizeSum > countBytesDone) {
			DataItem range2read;
			int currRangeIdx;
			while(true) {
				currRangeIdx = operation.currRangeIdx();
				if(currRangeIdx < rangeCount(fileItem.size())) {
					if(maskRangesPair[0].get(currRangeIdx) || maskRangesPair[1].get(currRangeIdx)) {
						range2read = operation.currRange();
						if(Loggers.MSG.isTraceEnabled()) {
							Loggers.MSG.trace(
								"I/O task: {}, Range index: {}, size: {}, internal position: {}, " +
									"Done byte count: {}",
								operation.toString(), currRangeIdx, range2read.size(),
								range2read.position(), countBytesDone
							);
						}
						break;
					} else {
						operation.currRangeIdx(++ currRangeIdx);
					}
				} else {
					operation.countBytesDone(rangesSizeSum);
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
					operation.toString(), countBytesDone, range2read.size()
				);
			}
			if(countBytesDone == currRangeSize) {
				operation.currRangeIdx(currRangeIdx + 1);
				operation.countBytesDone(0);
			} else {
				operation.countBytesDone(countBytesDone);
			}
		}
		return rangesSizeSum <= 0 || rangesSizeSum <= countBytesDone;
	}

	protected boolean invokeFileReadAndVerifyFixedRanges(
		final DataOperation<? extends DataItem> operation, final DataItem fileItem,
		final FSDataInputStream inputStream, final List<Range> fixedRanges
	)
	throws DataSizeException, DataCorruptionException, IOException {
		final long baseItemSize = fileItem.size();
		final long fixedRangesSizeSum = operation.markedRangesSize();
		long countBytesDone = operation.countBytesDone();
		// "countBytesDone" is the current range done bytes counter here
		long rangeBytesDone = countBytesDone;
		long currOffset;
		long cellOffset;
		long cellEnd;
		int n;
		if(fixedRangesSizeSum > 0 && fixedRangesSizeSum > countBytesDone) {
			Range fixedRange;
			DataItem currRange;
			int currFixedRangeIdx = operation.currRangeIdx();
			long fixedRangeEnd;
			long fixedRangeSize;
			if(currFixedRangeIdx < fixedRanges.size()) {
				fixedRange = fixedRanges.get(currFixedRangeIdx);
				currOffset = fixedRange.getBeg();
				fixedRangeEnd = fixedRange.getEnd();
				if(currOffset == - 1) {
					// last "rangeEnd" bytes
					currOffset = baseItemSize - fixedRangeEnd;
					fixedRangeSize = fixedRangeEnd;
				} else if(fixedRangeEnd == - 1) {
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
						operation.countBytesDone(fixedRangesSizeSum);
						return true;
					} else {
						operation.currRangeIdx(currFixedRangeIdx + 1);
						rangeBytesDone = 0;
					}
				}
				operation.countBytesDone(rangeBytesDone);
			} else {
				operation.countBytesDone(fixedRangesSizeSum);
			}
		}
		return fixedRangesSizeSum <= 0 || fixedRangesSizeSum <= countBytesDone;
	}

	protected boolean invokeFileRead(
		final DataOperation<? extends DataItem> operation, final DataItem fileItem,
		final FSDataInputStream inputStream
	)
	throws IOException {
		long countBytesDone = operation.countBytesDone();
		final long contentSize = fileItem.size();
		int n;
		if(countBytesDone < contentSize) {
			n = inputStream.read(
				DirectMemUtil.getThreadLocalReusableBuff(contentSize - countBytesDone)
			);
			if(n < 0) {
				operation.countBytesDone(countBytesDone);
				fileItem.size(countBytesDone);
				return true;
			} else {
				countBytesDone += n;
				operation.countBytesDone(countBytesDone);
			}
		}
		return countBytesDone >= contentSize;
	}

	protected boolean invokeFileReadRandomRanges(
		final DataOperation<? extends DataItem> operation, final DataItem fileItem,
		final FSDataInputStream inputStream, final BitSet maskRangesPair[]
	)
	throws IOException {
		int n;
		long countBytesDone = operation.countBytesDone();
		final long rangesSizeSum = operation.markedRangesSize();
		if(rangesSizeSum > 0 && rangesSizeSum > countBytesDone) {
			DataItem range2read;
			int currRangeIdx;
			while(true) {
				currRangeIdx = operation.currRangeIdx();
				if(currRangeIdx < rangeCount(fileItem.size())) {
					if(maskRangesPair[0].get(currRangeIdx) || maskRangesPair[1].get(currRangeIdx)) {
						range2read = operation.currRange();
						break;
					} else {
						operation.currRangeIdx(++ currRangeIdx);
					}
				} else {
					operation.countBytesDone(rangesSizeSum);
					return true;
				}
			}
			final long currRangeSize = range2read.size();
			inputStream.seek(rangeOffset(currRangeIdx) + countBytesDone);
			n = inputStream.read(
				DirectMemUtil.getThreadLocalReusableBuff(currRangeSize - countBytesDone)
			);
			if(n < 0) {
				operation.countBytesDone(countBytesDone);
				return true;
			}
			countBytesDone += n;
			if(countBytesDone == currRangeSize) {
				operation.currRangeIdx(currRangeIdx + 1);
				operation.countBytesDone(0);
			} else {
				operation.countBytesDone(countBytesDone);
			}
		}
		return rangesSizeSum <= 0 || rangesSizeSum <= countBytesDone;
	}

	protected boolean invokeFileReadFixedRanges(
		final DataOperation<? extends DataItem> operation, final DataItem fileItem,
		final FSDataInputStream inputStream, final List<Range> byteRanges
	)
	throws IOException {
		int n;
		long countBytesDone = operation.countBytesDone();
		final long baseItemSize = fileItem.size();
		final long rangesSizeSum = operation.markedRangesSize();
		if(rangesSizeSum > 0 && rangesSizeSum > countBytesDone) {
			Range byteRange;
			int currRangeIdx = operation.currRangeIdx();
			long rangeBeg;
			long rangeEnd;
			long rangeSize;
			if(currRangeIdx < byteRanges.size()) {
				byteRange = byteRanges.get(currRangeIdx);
				rangeBeg = byteRange.getBeg();
				rangeEnd = byteRange.getEnd();
				if(rangeBeg == - 1) {
					// last "rangeEnd" bytes
					rangeBeg = baseItemSize - rangeEnd;
					rangeSize = rangeEnd;
				} else if(rangeEnd == - 1) {
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
					operation.countBytesDone(countBytesDone);
					return true;
				}
				countBytesDone += n;
				if(countBytesDone == rangeSize) {
					operation.currRangeIdx(currRangeIdx + 1);
					operation.countBytesDone(0);
				} else {
					operation.countBytesDone(countBytesDone);
				}
			} else {
				operation.countBytesDone(rangesSizeSum);
			}
		}
		return rangesSizeSum <= 0 || rangesSizeSum <= countBytesDone;
	}

	protected boolean invokeFileAppend(
		final DataOperation<? extends DataItem> operation, final DataItem fileItem,
		final FSDataOutputStream outputStream, final Range appendRange
	)
	throws IOException {
		final long countBytesDone = operation.countBytesDone();
		final long appendSize = appendRange.getSize();
		final long remainingSize = appendSize - countBytesDone;
		long n;
		if(remainingSize > 0) {
			final WritableByteChannel outputChan = OutputStreamWrapperChannel
				.getThreadLocalInstance(outputStream, remainingSize);
			n = fileItem.writeToSocketChannel(outputChan, remainingSize);
			outputStream.hflush();
			operation.countBytesDone(countBytesDone + n);
			fileItem.size(fileItem.size() + n);
		}
		return remainingSize <= 0;
	}

	protected boolean invokeFileDelete(final DataOperation<? extends DataItem> fileOperation)
	throws IOException {
		final String dstPath = fileOperation.dstPath();
		final DataItem fileItem = fileOperation.item();
		final String itemName = fileItem.name();
		final Path filePath = getFilePath(dstPath, itemName);
		final FileSystem endpoint = getEndpoint(getNextEndpointAddr());
		if(! endpoint.delete(filePath, false)) {
			Loggers.ERR.debug(
				"Failed to delete the file {} @ {}", filePath,
				endpoint.getCanonicalServiceName()
			);
			fileOperation.startResponse();
			fileOperation.finishResponse();
			fileOperation.status(RESP_FAIL_UNKNOWN);
		}
		return true;
	}

	@Override
	public List<I> list(
		final ItemFactory<I> itemFactory, final String path, final String prefix, final int idRadix,
		final I lastPrevItem, final int count
	)
	throws IOException {
		return ListHelper.list(
			itemFactory, path, prefix, idRadix, lastPrevItem, count, getEndpoint(endpointAddrs[0])
		);
	}

	@Override
	public void adjustIoBuffers(final long avgTransferSize, final OpType opType) {
		int size;
		if(avgTransferSize < BUFF_SIZE_MIN) {
			size = BUFF_SIZE_MIN;
		} else if(BUFF_SIZE_MAX < avgTransferSize) {
			size = BUFF_SIZE_MAX;
		} else {
			size = (int) avgTransferSize;
		}
		if(OpType.CREATE.equals(opType)) {
			Loggers.MSG.info("Adjust output buffer size: {}", SizeInBytes.formatFixedSize(size));
			outBuffSize = size;
		} else if(OpType.READ.equals(opType)) {
			Loggers.MSG.info("Adjust input buffer size: {}", SizeInBytes.formatFixedSize(size));
			inBuffSize = size;
		}
	}

	@Override
	protected void doClose()
	throws IOException {
		super.doClose();
		hadoopConfig.clear();
		for(final FSDataInputStream input : fileInputStreams.values()) {
			input.close();
		}
		fileInputStreams.clear();
		for(final FSDataOutputStream output : fileOutputStreams.values()) {
			output.close();
		}
		fileOutputStreams.clear();
		for(int i = 0; i < endpointAddrs.length; i++) {
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
