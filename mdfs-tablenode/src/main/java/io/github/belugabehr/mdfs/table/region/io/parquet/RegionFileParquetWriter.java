package io.github.belugabehr.mdfs.table.region.io.parquet;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.commons.lang3.Range;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.ProtoParquetWriter2;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

import io.github.belugabehr.mdfs.region.Region.RegionRow;
import io.github.belugabehr.mdfs.table.region.io.RegionFileWriter;

public class RegionFileParquetWriter implements RegionFileWriter {
	private ParquetWriter<RegionRow> parquetWriter;
	private Range<ByteString> keyRange;
	private long maxVersion = -1L;
	private boolean closed;

	public RegionFileParquetWriter(Path path) throws IOException {
		open(path);
		closed = false;
	}

	protected void open(Path path) throws IOException {
		this.parquetWriter = new ProtoParquetWriter2<RegionRow>(path, RegionRow.class);
	}

	@Override
	public void writeRow(RegionRow row) throws IOException {
		Preconditions.checkState(!closed);

		final ByteString rowKey = row.getRowId().getRowKey();
		if (this.keyRange == null) {
			this.keyRange = Range.between(rowKey, rowKey, ByteString.unsignedLexicographicalComparator());
		} else {
			if (!this.keyRange.contains(rowKey)) {
				Preconditions.checkArgument(this.keyRange.isBefore(rowKey), "RowIds must be increasing");
				this.keyRange = Range.between(this.keyRange.getMinimum(), rowKey,
						ByteString.unsignedLexicographicalComparator());
			}
		}

		this.maxVersion = Math.max(this.maxVersion, row.getRowId().getVersion());

		this.parquetWriter.write(row);
	}

	@Override
	public Range<ByteString> getKeyRange() {
		return this.keyRange;
	}

	@Override
	public long getMaxVersion() {
		return this.maxVersion;
	}

	@Override
	public void close() throws Exception {
		this.closed = true;
		this.parquetWriter.close();
	}
}
