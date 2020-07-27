package io.github.belugabehr.mdfs.table.region.io;

import java.io.IOException;
import java.util.stream.Stream;

import io.github.belugabehr.mdfs.region.Region.RegionRow;

public interface RegionFileReader extends AutoCloseable {

	Stream<RegionRow> rows();

	RegionRow readRow() throws IOException;
}
