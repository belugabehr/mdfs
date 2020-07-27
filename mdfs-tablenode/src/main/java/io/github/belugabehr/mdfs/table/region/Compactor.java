package io.github.belugabehr.mdfs.table.region;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;

public interface Compactor {
	
	RegionFile compact(Path path, Collection<RegionFile> files) throws IOException;
	
}
