package com.emc.mongoose.storage.driver.hdfs;

import com.emc.mongoose.api.model.item.DataItemFactory;
import com.emc.mongoose.api.model.item.Item;
import com.emc.mongoose.api.model.item.ItemFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ListHelper {

	static <I extends Item> List<I> list(
		final ItemFactory<I> itemFactory, final String path, final String prefix, final int idRadix,
		final I lastPrevItem, final int count, final FileSystem endpoint
	) throws IOException {

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
			if(itemFactory instanceof DataItemFactory) {
				// skip directory entries
				if(lfs.isDirectory()) {
					continue;
				}
			} else {
				// skip file entries
				if(!lfs.isDirectory()) {
					continue;
				}
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
}
