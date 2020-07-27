package io.github.belugabehr.mdfs.table.region.io.parquet;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.proto.ProtoReadSupport;

import io.github.belugabehr.mdfs.region.Region.RegionRow;
import io.github.belugabehr.mdfs.region.Region.RegionRowOrBuilder;
import io.github.belugabehr.mdfs.table.region.io.RegionFileReader;

public class RegionFileParquetReader implements RegionFileReader {

	private ParquetReader<RegionRow> parquetReader;

	public RegionFileParquetReader(InputFile inputFile) throws IOException {
		Objects.requireNonNull(inputFile);
		this.parquetReader = new ProtoParquetReaderBuilder(inputFile).build();
	}

	@Override
	public RegionRow readRow() throws IOException {
		RegionRowOrBuilder regionRow = parquetReader.read();
		return regionRow == null ? null : ((RegionRow.Builder) regionRow).build();
	}

	@Override
	public void close() throws Exception {
		this.parquetReader.close();
	}

	@Override
	public Stream<RegionRow> rows() {
		Iterator<RegionRow> iter = new Iterator<RegionRow>() {
			RegionRow nextRow = null;

			@Override
			public boolean hasNext() {
				if (nextRow != null) {
					return true;
				} else {
					try {
						nextRow = readRow();
						return (nextRow != null);
					} catch (IOException e) {
						throw new UncheckedIOException(e);
					}
				}
			}

			@Override
			public RegionRow next() {
				if (nextRow != null || hasNext()) {
					RegionRow row = nextRow;
					nextRow = null;
					return row;
				} else {
					throw new NoSuchElementException();
				}
			}
		};
		return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iter,
				Spliterator.ORDERED | Spliterator.SORTED | Spliterator.NONNULL), false);
	}

	private static class ProtoParquetReaderBuilder extends ParquetReader.Builder<RegionRow> {

		protected ProtoParquetReaderBuilder(InputFile file) {
			super(file);
		}

		@Override
		protected ReadSupport<RegionRow> getReadSupport() {
			return new ProtoReadSupport<>();
		}

	}

}
