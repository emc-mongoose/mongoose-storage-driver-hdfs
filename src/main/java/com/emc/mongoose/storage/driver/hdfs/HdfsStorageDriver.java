package com.emc.mongoose.storage.driver.hdfs;

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

import com.github.akurilov.commons.system.SizeInBytes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

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

	private final Configuration hadoopConfig = new Configuration();

	private int nodePort = -1;
	private int inBuffSize = BUFF_SIZE_MIN;
	private int outBuffSize = BUFF_SIZE_MIN;

	private final ConcurrentMap<String, FileSystem> endpoints = new ConcurrentHashMap<>();
	private final String[] endpointAddrs;
	private final AtomicInteger rrc = new AtomicInteger(0);
	private final ConcurrentMap<DataIoTask<? extends DataItem>, FSDataInputStream>
		fileInputStreams = new ConcurrentHashMap<>();
	private final ConcurrentMap<DataIoTask<? extends DataItem>, FSDataOutputStream>
		fileOutputStreams = new ConcurrentHashMap<>();

	public HdfsStorageDriver(
		final String testStepId, final DataInput dataInput, final LoadConfig loadConfig,
		final StorageConfig storageConfig, final boolean verifyFlag
	) throws OmgShootMyFootException {

		super(testStepId, dataInput, loadConfig, storageConfig, verifyFlag);

		final NodeConfig nodeConfig = storageConfig.getNetConfig().getNodeConfig();
		nodePort = storageConfig.getNetConfig().getNodeConfig().getPort();
		endpointAddrs = (String[]) nodeConfig.getAddrs().toArray();
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
			final URI endpointUri = new URI("hdfs", uid, addr, port, "/", null, null);
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

	protected FSDataInputStream getReadFileStream(final O readTask) {
		final String endpointAddr = readTask.getNodeAddr();
		final FileSystem fs = endpoints.get(endpointAddr);
		final String srcPath = readTask.getSrcPath();
		final String itemName = readTask.getItem().getName();
		final Path itemPath;
		if(srcPath == null || srcPath.isEmpty()) {
			itemPath = new Path(itemName);
		} else {
			itemPath = new Path(srcPath, itemName);
		}
		try {
			return fs.open(itemPath, inBuffSize);
		} catch(final IOException e) {
			throw new RuntimeException(e);
		}
	}

	protected FSDataOutputStream getCreateFileStream(
		final DataIoTask<? extends DataItem> createFileTask
	) {
		final String endpointAddr = createFileTask.getNodeAddr();
		final FileSystem fs = endpoints.get(endpointAddr);
		final String dstPath = createFileTask.getDstPath();
		final String itemName = createFileTask.getItem().getName();
		final Path itemPath;
		if(dstPath == null || dstPath.isEmpty()) {
			itemPath = new Path(itemName);
		} else {
			itemPath = new Path(dstPath, itemName);
		}
		try {
			return fs.create(itemPath, false, outBuffSize);
		} catch(final IOException e) {
			throw new RuntimeException(e);
		}
	}

	protected FSDataOutputStream getUpdateFileStream(
		final DataIoTask<? extends DataItem> updateFileTask
	) {
		final String endpointAddr = updateFileTask.getNodeAddr();
		final FileSystem fs = endpoints.get(endpointAddr);
		final String dstPath = updateFileTask.getDstPath();
		final String itemName = updateFileTask.getItem().getName();
		final Path itemPath;
		if(dstPath == null || dstPath.isEmpty()) {
			itemPath = new Path(itemName);
		} else {
			itemPath = new Path(dstPath, itemName);
		}
		try {
			return fs.create(itemPath, true, outBuffSize);
		} catch(final IOException e) {
			throw new RuntimeException(e);
		}
	}

	protected FSDataOutputStream getAppendStream(
		final DataIoTask<? extends DataItem> appendFileTask
	) {
		final String endpointAddr = appendFileTask.getNodeAddr();
		final FileSystem fs = endpoints.get(endpointAddr);
		final String dstPath = appendFileTask.getDstPath();
		final String itemName = appendFileTask.getItem().getName();
		final Path itemPath;
		if(dstPath == null || dstPath.isEmpty()) {
			itemPath = new Path(itemName);
		} else {
			itemPath = new Path(dstPath, itemName);
		}
		try {
			return fs.append(itemPath, outBuffSize);
		} catch(final IOException e) {
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
		try {
			switch(ioType) {
				case NOOP:
					invokeNoop((O) fileIoTask);
					break;
				case CREATE:
					invokeFileCreate(fileIoTask);
					break;
				case READ:
					break;
				case UPDATE:
					invokeFileDelete(fileIoTask);
					break;
				case DELETE:
					break;
				case LIST:
					break;
			}
		} catch(final RuntimeException e) {
			final Throwable cause = e.getCause();
			final DataItem fileItem = fileIoTask.getItem();
			final long countBytesDone = fileIoTask.getCountBytesDone();
			if(cause instanceof IOException) {
				LogUtil.exception(Level.DEBUG, cause, "Failed open the file: {}" + fileItem.getName());
				fileItem.size(countBytesDone);
				finishIoTask((O) fileIoTask);
				fileIoTask.setStatus(IoTask.Status.FAIL_IO);
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
		}
	}

	protected void invokeNoop(final O ioTask) {
		finishIoTask(ioTask);
	}

	protected void invokeFileCreate(final DataIoTask<? extends DataItem> fileIoTask) {
		final DataItem fileItem = fileIoTask.getItem();
		final long fileSize;
		try {
			fileSize = fileItem.size();
		} catch(final IOException e) {
			throw new AssertionError(e);
		}
		long countBytesDone = fileIoTask.getCountBytesDone();
		final FSDataOutputStream output = fileOutputStreams.computeIfAbsent(
			fileIoTask, this::getCreateFileStream
		);
		try {
			long remainingBytes = fileSize - countBytesDone;
			if(remainingBytes > 0) {
				countBytesDone += fileItem.writeToStream(output, remainingBytes);
				fileIoTask.setCountBytesDone(countBytesDone);
			} else {
				finishIoTask((O) fileIoTask);
				output.close();
			}
		} catch(final IOException e) {
			LogUtil.exception(Level.DEBUG, e, "Failed to write to the file: {}" + fileItem.getName());
			fileItem.size(countBytesDone);
			finishIoTask((O) fileIoTask);
			fileIoTask.setStatus(IoTask.Status.FAIL_IO);
		}
	}

	protected void invokeFileDelete(final DataIoTask<? extends DataItem> fileIoTask) {
		final String endpointAddr = fileIoTask.getNodeAddr();
		final FileSystem fs = endpoints.get(endpointAddr);
		final String dstPath = fileIoTask.getDstPath();
		final DataItem fileItem = fileIoTask.getItem();
		final Path filePath = dstPath == null ?
			new Path(fileItem.getName()) : new Path(dstPath, fileItem.getName());
		try {
			fs.delete(filePath, false);
			finishIoTask((O) fileIoTask);
		} catch(final IOException e) {
			LogUtil.exception(
				Level.DEBUG, e, "Failed to delete the file {}", filePath, fs.getUri()
			);
			fileIoTask.startResponse();
			fileIoTask.finishResponse();
			fileIoTask.setStatus(IoTask.Status.FAIL_IO);
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
		return null;
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