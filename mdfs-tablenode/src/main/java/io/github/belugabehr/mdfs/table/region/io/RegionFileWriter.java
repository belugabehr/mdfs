package io.github.belugabehr.mdfs.table.region.io;

import java.io.IOException;

import org.apache.commons.lang3.Range;

import com.google.protobuf.ByteString;

import io.github.belugabehr.mdfs.region.Region.RegionRow;

public interface RegionFileWriter extends AutoCloseable {

	void writeRow(RegionRow row) throws IOException;

	Range<ByteString> getKeyRange();

	long getMaxVersion();

}
