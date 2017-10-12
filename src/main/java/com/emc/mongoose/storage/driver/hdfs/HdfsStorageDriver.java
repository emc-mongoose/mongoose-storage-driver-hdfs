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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.rmi.RemoteException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

public class HdfsStorageDriver<I extends Item, O extends IoTask<I>>
extends NioStorageDriverBase<I, O> {

	private final Configuration hadoopConfig = new Configuration();
	private int nodePort = -1;

	private final ConcurrentMap<String, FileSystem> endpoints = new ConcurrentHashMap<>();
	private final Function<String, FileSystem> getEndpointByAddrFunc = nodeAddr -> {
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
	};

	public HdfsStorageDriver(
		final String testStepId, final DataInput dataInput, final LoadConfig loadConfig,
		final StorageConfig storageConfig, final boolean verifyFlag
	) throws OmgShootMyFootException {

		super(testStepId, dataInput, loadConfig, storageConfig, verifyFlag);

		nodePort = storageConfig.getNetConfig().getNodeConfig().getPort();
	}

	@Override
	protected void invokeNio(final O ioTask) {

		final FileSystem endpoint = endpoints.get();

		endpoint.
	}

	@Override
	protected String requestNewPath(final String path) {
		return null;
	}

	@Override
	protected String requestNewAuthToken(final Credential credential) {
		return null;
	}

	@Override
	public List<I> list(
		final ItemFactory<I> itemFactory, final String path, final String prefix, final int idRadix,
		final I lastPrevItem, final int count
	) throws IOException {
		return null;
	}

	@Override
	public void adjustIoBuffers(final long avgDataItemSize, final IoType ioType)
	throws RemoteException {

	}

	@Override
	protected void doClose()
	throws IOException {
		super.doClose();
		for(final FileSystem endpoint : endpoints.values()) {
			endpoint.close();
		}
		hadoopConfig.clear();
	}
}
