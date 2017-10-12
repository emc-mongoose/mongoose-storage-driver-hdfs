package com.emc.mongoose.storage.driver.hdfs;

import com.emc.mongoose.api.common.exception.OmgShootMyFootException;
import com.emc.mongoose.api.model.data.DataInput;
import com.emc.mongoose.api.model.io.IoType;
import com.emc.mongoose.api.model.io.task.IoTask;
import com.emc.mongoose.api.model.item.Item;
import com.emc.mongoose.api.model.item.ItemFactory;
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
import org.apache.hadoop.hdfs.protocol.HdfsConstants;

import org.apache.logging.log4j.Level;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.rmi.RemoteException;
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
	private final ConcurrentMap<O, FSDataInputStream> inputs = new ConcurrentHashMap<>();
	private final ConcurrentMap<O, FSDataOutputStream> outputs = new ConcurrentHashMap<>();

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
			final URI endpointUri = new URI(
				HdfsConstants.HDFS_URI_SCHEME, uid, addr, port, "/", null, null
			);
			return FileSystem.get(endpointUri, hadoopConfig);
		} catch(final URISyntaxException | IOException e) {
			throw new RuntimeException(e);
		}
	}

	protected FSDataInputStream getInputStream(final O inputTask) {

	}

	protected FSDataOutputStream getOutputStream(final O outputTask) {

	}

	@Override
	protected void prepareIoTask(final O ioTask) {
		super.prepareIoTask(ioTask);
		String endpointAddr = ioTask.getNodeAddr();
		if(endpointAddr == null) {
			endpointAddr = getNextEndpointAddr();
			ioTask.setNodeAddr(endpointAddr);
		}
	}

	@Override
	protected void invokeNio(final O ioTask) {
		final FileSystem fs = endpoints.computeIfAbsent(ioTask.getNodeAddr(), this::getEndpoint);
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
		for(final FSDataInputStream input: inputs.values()) {
			input.close();
		}
		inputs.clear();
		for(final FSDataOutputStream output: outputs.values()) {
			output.close();
		}
		outputs.clear();
		for(final FileSystem endpoint: endpoints.values()) {
			endpoint.close();
		}
		endpoints.clear();
		for(int i = 0; i < endpointAddrs.length; i ++) {
			endpointAddrs[i] = null;
		}
	}
}
