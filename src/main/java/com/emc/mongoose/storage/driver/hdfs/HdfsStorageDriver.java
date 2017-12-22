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
import static com.emc.mongoose.storage.driver.hdfs.FileIoHelper.getFilePath;
import static com.emc.mongoose.storage.driver.hdfs.FileIoHelper.invokeFileAppend;
import static com.emc.mongoose.storage.driver.hdfs.FileIoHelper.invokeFileCopy;
import static com.emc.mongoose.storage.driver.hdfs.FileIoHelper.invokeFileCreate;
import static com.emc.mongoose.storage.driver.hdfs.FileIoHelper.invokeFileRead;
import static com.emc.mongoose.storage.driver.hdfs.FileIoHelper.invokeFileReadAndVerify;
import static com.emc.mongoose.storage.driver.hdfs.FileIoHelper.invokeFileReadAndVerifyFixedRanges;
import static com.emc.mongoose.storage.driver.hdfs.FileIoHelper.invokeFileReadAndVerifyRandomRanges;
import static com.emc.mongoose.storage.driver.hdfs.FileIoHelper.invokeFileReadFixedRanges;
import static com.emc.mongoose.storage.driver.hdfs.FileIoHelper.invokeFileReadRandomRanges;

import com.github.akurilov.commons.collection.Range;
import com.github.akurilov.commons.system.SizeInBytes;

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
import java.rmi.RemoteException;
import java.rmi.ServerException;
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
		final LoadConfig loadConfig, final StorageConfig storageConfig, final boolean verifyFlag
	) throws OmgShootMyFootException {

		super(testStepId, dataInput, loadConfig, storageConfig, verifyFlag);

		this.uriSchema = uriSchema;
		hadoopConfig = new Configuration();
		hadoopConfig.setClassLoader(Extensions.CLS_LOADER);
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

		final NodeConfig nodeConfig = storageConfig.getNetConfig().getNodeConfig();
		nodePort = storageConfig.getNetConfig().getNodeConfig().getPort();
		final List<String> endpointAddrList = nodeConfig.getAddrs();
		endpointAddrs = endpointAddrList.toArray(new String[endpointAddrList.size()]);
		requestAuthTokenFunc = null; // do not use
		requestNewPathFunc = null; // do not use
	}

	private String getNextEndpointAddr() {
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
			Thread.currentThread().setContextClassLoader(Extensions.CLS_LOADER);
			return FileSystem.get(endpointUri, hadoopConfig);
		} catch(final URISyntaxException | IOException e) {
			throw new RuntimeException(e);
		} finally {
			// set the thread's context classloader back
			Thread.currentThread().setContextClassLoader(ClassLoader.getSystemClassLoader());
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

	protected FSDataOutputStream getCreateFileStream(
		final DataIoTask<? extends DataItem> createFileTask
	) {
		final String dstPath = createFileTask.getDstPath();
		final DataItem fileItem = createFileTask.getItem();
		final String fileName = fileItem.getName();
		final Path filePath = getFilePath(dstPath, fileName);
		final FileSystem endpoint = getEndpoint(createFileTask.getNodeAddr());
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
		final DataIoTask<? extends DataItem> readFileTask
	) {
		final String srcPath = readFileTask.getSrcPath();
		if(srcPath == null || srcPath.isEmpty()) {
			return null;
		}
		final DataItem fileItem = readFileTask.getItem();
		final String fileName = fileItem.getName();
		final Path filePath = getFilePath(srcPath, fileName);
		final FileSystem endpoint = getEndpoint(readFileTask.getNodeAddr());
		try {
			return endpoint.open(filePath, inBuffSize);
		} catch(final IOException e) {
			readFileTask.setStatus(FAIL_IO);
			throw new RuntimeException(e);
		}
	}

	protected FSDataOutputStream getUpdateFileStream(
		final DataIoTask<? extends DataItem> updateFileTask
	) {
		final String dstPath = updateFileTask.getDstPath();
		final DataItem fileItem = updateFileTask.getItem();
		final String fileName = fileItem.getName();
		final Path filePath = getFilePath(dstPath, fileName);
		final FileSystem endpoint = getEndpoint(updateFileTask.getNodeAddr());
		try {
			return endpoint.create(
				filePath, defaultFsPerm, true, outBuffSize,
				endpoint.getDefaultReplication(filePath), fileItem.size(), null
			);
		} catch(final IOException e) {
			updateFileTask.setStatus(FAIL_IO);
			throw new RuntimeException(e);
		}
	}

	protected FSDataOutputStream getAppendFileStream(
		final DataIoTask<? extends DataItem> appendFileTask
	) {
		final String dstPath = appendFileTask.getDstPath();
		final DataItem fileItem = appendFileTask.getItem();
		final String fileName = fileItem.getName();
		final Path filePath = getFilePath(dstPath, fileName);
		final FileSystem endpoint = getEndpoint(appendFileTask.getNodeAddr());
		try {
			return endpoint.append(filePath, outBuffSize);
		} catch(final IOException e) {
			appendFileTask.setStatus(FAIL_IO);
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

		final IoType ioType = fileIoTask.getIoType();
		final DataItem fileItem = fileIoTask.getItem();

		FSDataInputStream input = null;
		FSDataOutputStream output = null;

		try {
			switch(ioType) {

				case NOOP:
					finishIoTask((O) fileIoTask);
					break;

				case CREATE:
					final List<? extends DataItem> srcItems = fileIoTask.getSrcItemsToConcat();
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
					final List<Range> fixedRangesToRead = fileIoTask.getFixedRanges();
					if(verifyFlag) {
						try {
							if(fixedRangesToRead == null || fixedRangesToRead.isEmpty()) {
								if(fileIoTask.hasMarkedRanges()) {
									if(
										invokeFileReadAndVerifyRandomRanges(
											fileIoTask, fileItem, input,
											fileIoTask.getMarkedRangesMaskPair()
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
							fileIoTask.setStatus(IoTask.Status.RESP_FAIL_CORRUPT);
							final long countBytesDone = fileIoTask.getCountBytesDone()
								+ e.getOffset();
							fileIoTask.setCountBytesDone(countBytesDone);
							try {
								Loggers.MSG.debug(
									"{}: content size mismatch, expected: {}, actual: {}",
									fileItem.getName(), fileItem.size(), countBytesDone
								);
							} catch(final IOException ignored) {
							}
						} catch(final DataCorruptionException e) {
							fileIoTask.setStatus(IoTask.Status.RESP_FAIL_CORRUPT);
							final long countBytesDone = fileIoTask.getCountBytesDone()
								+ e.getOffset();
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
								if(
									invokeFileReadRandomRanges(
										fileIoTask, fileItem, input,
										fileIoTask.getMarkedRangesMaskPair()
									)
								) {
									fileIoTask.setCountBytesDone(fileIoTask.getMarkedRangesSize());
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
					final List<Range> fixedRangesToUpdate = fileIoTask.getFixedRanges();
					if(fixedRangesToUpdate == null || fixedRangesToUpdate.isEmpty()) {
						if(fileIoTask.hasMarkedRanges()) {
							throw new AssertionError("Random byte ranges update isn't implemented");
						} else {
							// overwrite
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
								// overwrite
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
			fileIoTask.setStatus(FAIL_IO);
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

	protected boolean invokeFileDelete(final DataIoTask<? extends DataItem> fileIoTask)
	throws IOException {

		final String dstPath = fileIoTask.getDstPath();
		final DataItem fileItem = fileIoTask.getItem();
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
			fileIoTask.setStatus(IoTask.Status.RESP_FAIL_UNKNOWN);
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
		for(int i = 0; i < endpointAddrs.length; i ++) {
			endpointAddrs[i] = null;
		}
		if(ugi != null) {
			FileSystem.closeAllForUGI(ugi);
		} else {
			FileSystem.closeAll();
		}
	}
}
