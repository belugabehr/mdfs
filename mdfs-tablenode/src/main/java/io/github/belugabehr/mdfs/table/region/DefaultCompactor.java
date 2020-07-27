package io.github.belugabehr.mdfs.table.region;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Objects;

import com.google.common.base.Preconditions;


public class DefaultCompactor implements Compactor {

	@Override
	public RegionFile compact(final Path path, final Collection<RegionFile> files) throws IOException {
		Objects.requireNonNull(path);
		Objects.requireNonNull(files);
		Preconditions.checkArgument(files.size() > 1);

		return new RegionFile.Builder().withRandomID().withPath(path).merge(files, true);
	}

}
