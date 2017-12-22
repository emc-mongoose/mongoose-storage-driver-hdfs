package com.emc.mongoose.storage.driver.hdfs;

import com.emc.mongoose.api.common.exception.OmgShootMyFootException;
import com.emc.mongoose.api.model.data.DataInput;
import com.emc.mongoose.api.model.io.task.IoTask;
import com.emc.mongoose.api.model.item.Item;
import com.emc.mongoose.storage.driver.base.StorageDriverFactory;
import com.emc.mongoose.ui.config.load.LoadConfig;
import com.emc.mongoose.ui.config.storage.StorageConfig;

public class HdfsStorageDriverFactory<
	I extends Item, O extends IoTask<I>, T extends HdfsStorageDriver<I, O>
>
implements StorageDriverFactory<I, O, T> {

	private static final String NAME = "hdfs";

	@Override
	public String getName() {
		return NAME;
	}

	@Override @SuppressWarnings("unchecked")
	public T create(
		final String stepId, final DataInput dataInput, final LoadConfig loadConfig,
		final StorageConfig storageConfig, final boolean verifyFlag
	) throws OmgShootMyFootException, InterruptedException {
		return (T) new HdfsStorageDriver<I, O>(
			NAME, stepId, dataInput, loadConfig, storageConfig, verifyFlag
		);
	}
}
