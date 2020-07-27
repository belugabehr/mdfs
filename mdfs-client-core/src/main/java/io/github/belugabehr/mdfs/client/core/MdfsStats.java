package io.github.belugabehr.mdfs.client.core;

import io.github.belugabehr.mdfs.api.Mdfs.MFile;
import io.github.belugabehr.mdfs.client.Stats;

public class MdfsStats implements Stats {

	private final MFile mfile;

	public MdfsStats(MFile mfile) {
		this.mfile = mfile;
	}

	@Override
	public long size() {
		return mfile.getFileSize();
	}

}
