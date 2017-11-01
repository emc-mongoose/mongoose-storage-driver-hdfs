package com.emc.mongoose.storage.driver.hdfs;

import com.emc.mongoose.api.common.env.Extensions;
import com.emc.mongoose.api.common.exception.OmgShootMyFootException;
import com.emc.mongoose.api.model.data.DataInput;
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

import com.github.akurilov.commons.io.IoUtil;
import com.github.akurilov.commons.system.SizeInBytes;
import static com.github.akurilov.commons.io.IoUtil.REUSABLE_BUFF_SIZE_MAX;

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
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.rmi.RemoteException;
import java.rmi.ServerException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class HdfsStorageDriver<I extends Item, O extends IoTask<I>>
extends NioStorageDriverBase<I, O> {

	public static final String DEFAULT_URI_SCHEMA = "hdfs";

	private final ConcurrentMap<String, FileSystem> endpoints = new ConcurrentHashMap<>();
	private final String[] endpointAddrs;
	private final AtomicInteger rrc = new AtomicInteger(0);
	private final ConcurrentMap<DataIoTask<? extends DataItem>, FSDataInputStream>
		fileInputStreams = new ConcurrentHashMap<>();
	private final ConcurrentMap<DataIoTask<? extends DataItem>, FSDataOutputStream>
		fileOutputStreams = new ConcurrentHashMap<>();

	private final Configuration hadoopConfig;
	private final FsPermission defaultFsPerm;

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
				addr = nodeAddr.substring(portSepPos);
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

	protected FSDataOutputStream getCreateFileStream(
		final DataIoTask<? extends DataItem> createFileTask
	) {
		final String endpointAddr = createFileTask.getNodeAddr();
		final FileSystem endpoint = endpoints.get(endpointAddr);
		final String dstPath = createFileTask.getDstPath();
		final DataItem fileItem = createFileTask.getItem();
		final String fileName = fileItem.getName();
		final Path filePath;
		if(dstPath == null || dstPath.isEmpty() || fileName.startsWith(dstPath)) {
			filePath = new Path(fileName);
		} else {
			filePath = new Path(dstPath, fileName);
		}
		try {
			return endpoint.create(
				filePath, defaultFsPerm, false, outBuffSize, endpoint.getDefaultReplication(filePath), fileItem.size(),
				null
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
		final Path filePath;
		if(fileName.startsWith(srcPath)) {
			filePath = new Path(fileName);
		} else {
			filePath = new Path(srcPath, fileName);
		}
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
		FSDataInputStream input = null;
		FSDataOutputStream output = null;
		try {
			switch(ioType) {
				case NOOP:
					invokeNoop((O) fileIoTask);
					break;
				case CREATE:
					input = fileInputStreams.computeIfAbsent(fileIoTask, this::getReadFileStream);
					output = fileOutputStreams.computeIfAbsent(fileIoTask, this::getCreateFileStream);
					if(input == null) {
						invokeFileCreate(fileIoTask, output);
					} else {
						invokeFileCopy(fileIoTask, input, output);
					}
					break;
				case READ:
					input  = fileInputStreams.computeIfAbsent(fileIoTask, this::getReadFileStream);
					invokeFileRead(fileIoTask, input);
					break;
				case UPDATE:
					break;
				case DELETE:
					invokeFileDelete(fileIoTask);
					break;
				case LIST:
					break;
			}
		} catch(final RuntimeException e) {
			final Throwable cause = e.getCause();
			final DataItem fileItem = fileIoTask.getItem();
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
		final DataIoTask<? extends DataItem> fileIoTask, final FSDataOutputStream outputStream
	) {
		final DataItem fileItem = fileIoTask.getItem();
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
				final WritableByteChannel outputChan = IoUtil.getThreadLocalOutputChannel(
					outputStream, remainingBytes
				);
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
		final DataIoTask<? extends DataItem> fileIoTask, final FSDataInputStream inputStream,
		final FSDataOutputStream outputStream
	) {
		long countBytesDone = fileIoTask.getCountBytesDone();
		final long fileSize;
		try {
			fileSize = fileIoTask.getItem().size();
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

	protected void invokeFileRead(
		final DataIoTask<? extends DataItem> fileIoTask, final FSDataInputStream inputStream
	) {
		final DataItem fileItem = fileIoTask.getItem();
		final long fileSize;
		try {
			fileSize = fileItem.size();
		} catch(final IOException e) {
			throw new AssertionError(e);
		}
		long countBytesDone = fileIoTask.getCountBytesDone();
		final FSDataInputStream input = fileInputStreams.computeIfAbsent(
			fileIoTask, this::getReadFileStream
		);
		try {
			long remainingBytes = fileSize - countBytesDone;
			if(remainingBytes > 0) {
				if(verifyFlag) {
					countBytesDone += fileItem.readAndVerify(input, remainingBytes);
					fileIoTask.setCountBytesDone(countBytesDone);
				} else {
					input.read
				}
			} else {
				finishIoTask((O) fileIoTask);
				fileOutputStreams.remove(fileIoTask);
				input.close();
			}
		} catch(final IOException e) {
			LogUtil.exception(Level.DEBUG, e, "Failed to read the file: {}" + fileItem.getName());
			fileItem.size(countBytesDone);
			finishIoTask((O) fileIoTask);
			fileIoTask.setStatus(FAIL_IO);
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
