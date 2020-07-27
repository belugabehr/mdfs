package io.github.belugabehr.mdfs.table.region;

import static io.github.belugabehr.mdfs.table.region.RegionComparators.ROWID_VERSIONED_COMPARATOR;
import static io.github.belugabehr.mdfs.table.region.RegionComparators.ROW_COMPARATOR;
import static io.github.belugabehr.mdfs.table.region.RegionComparators.ROW_VERSIONED_COMPARATOR;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.commons.lang3.Range;
import org.apache.parquet.io.InputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.Weigher;
import com.google.common.collect.ForwardingNavigableMap;
import com.google.common.collect.Iterators;
import com.google.protobuf.ByteString;

import io.github.belugabehr.mdfs.region.Region.RegionRow;
import io.github.belugabehr.mdfs.region.Region.RegionRow.RowId;
import io.github.belugabehr.mdfs.table.region.io.RegionFileReader;
import io.github.belugabehr.mdfs.table.region.io.RegionFileWriter;
import io.github.belugabehr.mdfs.table.region.io.parquet.RegionFileParquetReader;
import io.github.belugabehr.mdfs.table.region.io.parquet.RegionFileParquetWriter;
import io.github.belugabehr.mdfs.table.utils.BatchingIterator;
import io.github.belugabehr.mdfs.table.utils.CachingInputFile;

public final class RegionFile extends ForwardingNavigableMap<RegionRow.RowId, RegionRow> {

	private static final Logger LOG = LoggerFactory.getLogger(RegionFile.class);

	private final String id;
	private final Path path;
	private final Range<ByteString> keyRange;
	private final long maxVersion;
	private final LoadingCache<RegionRow.RowId, Optional<RegionRow>> cache;
	private final CachingInputFile inputFile;

	private RegionFile(String id, Path path, Range<ByteString> range, long maxVersion) {
		this.id = id;
		this.path = path;
		this.keyRange = range;
		this.maxVersion = maxVersion;
		this.inputFile = CachingInputFile.fromPath(path);

		this.cache = CacheBuilder.newBuilder().maximumWeight(32768).recordStats()
				.weigher(new Weigher<RegionRow.RowId, Optional<RegionRow>>() {
					public int weigh(RegionRow.RowId id, Optional<RegionRow> row) {
						final int idSize = id.getSerializedSize();
						final int rowSize = row.isPresent() ? row.get().getSerializedSize() : 0;
						return idSize + rowSize;
					}
				}).build(new CacheLoader<RegionRow.RowId, Optional<RegionRow>>() {
					public Optional<RegionRow> load(RegionRow.RowId key) {
						return doCeilingEntry(key);
					}
				});
	}

	@Override
	public Entry<RegionRow.RowId, RegionRow> ceilingEntry(RegionRow.RowId key) {
		try {
			final Optional<RegionRow> regionRow = this.cache.get(key);
			if (regionRow.isEmpty()) {
				return new AbstractMap.SimpleEntry<>(key, regionRow.get());
			}
		} catch (ExecutionException e) {
			throw new RuntimeException(e.getCause());
		}
		return null;
	}

	protected Optional<RegionRow> doCeilingEntry(RegionRow.RowId key) {
		Optional<RegionRow> ceilEntry = Optional.empty();
		try (RegionFileReader reader = new RegionFileParquetReader(this.inputFile)) {
			RegionRow currentRegionRow = reader.readRow();
			while (currentRegionRow != null) {
				if (ROWID_VERSIONED_COMPARATOR.compare(currentRegionRow.getRowId(), key) >= 0) {
					break;
				}
				currentRegionRow = reader.readRow();
			}
			ceilEntry = Optional.ofNullable(currentRegionRow);
		} catch (IOException ioe) {
			throw new UncheckedIOException(ioe);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return ceilEntry;
	}

	@Override
	public SortedMap<RegionRow.RowId, RegionRow> subMap(RegionRow.RowId fromKey, RegionRow.RowId toKey) {
		SortedMap<RegionRow.RowId, RegionRow> results = new TreeMap<>(ROWID_VERSIONED_COMPARATOR);
		try (RegionFileReader reader = new RegionFileParquetReader(this.inputFile)) {
			RegionRow currentRegionRow = reader.readRow();
			while (currentRegionRow != null) {
				if (between(fromKey, toKey, currentRegionRow.getRowId(), ROWID_VERSIONED_COMPARATOR)) {
					results.put(currentRegionRow.getRowId(), currentRegionRow);
				}
				currentRegionRow = reader.readRow();
			}
		} catch (IOException ioe) {
			throw new UncheckedIOException(ioe);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return results;
	}

	private boolean between(RegionRow.RowId minimum, RegionRow.RowId maximum, RegionRow.RowId rowId,
			Comparator<RegionRow.RowId> cmp) {
		return cmp.compare(rowId, minimum) >= 0 && cmp.compare(rowId, maximum) < 0;
	}

	public void delete() {
		try {
			Files.deleteIfExists(this.path);
		} catch (IOException ioe) {
			throw new UncheckedIOException(ioe);
		}
	}

	public String getId() {
		return id;
	}

	public Range<ByteString> getKeyRange() {
		return keyRange;
	}

	public long getMaxVersion() {
		return maxVersion;
	}

	public static Builder newBuilder() {
		return new Builder();
	}

	public static class Builder {
		private Map<RegionRow.RowId, RegionRow> rowMap = new LinkedHashMap<>();
		private Path path = null;
		private UUID id = null;

		public Builder withPath(final Path path) {
			this.path = path;
			return this;
		}

		public Builder withRandomID() {
			this.id = UUID.randomUUID();
			return this;
		}

		public Builder withID(UUID id) {
			this.id = id;
			return this;
		}

		public Builder withRows(Map<RegionRow.RowId, RegionRow> rows) {
			this.rowMap.putAll(rows);
			return this;
		}

		public RegionFile merge(Collection<RegionFile> regionFiles, boolean preserveTombstones) throws IOException {

			Collection<RegionFileReader> readers = new ArrayList<>();
			for (RegionFile regionFile : regionFiles) {
				// TODO: Do not use caching here since it will scan the entire file
				InputFile inputFile = CachingInputFile.fromPath(regionFile.path);
				readers.add(new RegionFileParquetReader(inputFile));
			}

			Iterator<RegionRow> mergedStream = Iterators.mergeSorted(
					readers.stream().map(reader -> reader.rows().iterator()).collect(Collectors.toList()),
					ROW_VERSIONED_COMPARATOR);

			Range<ByteString> keyRange = null;
			long maxVersion = -1L;

			LOG.info("Merging RegionFiles: [dst:{}][src:{}]", this.path, regionFiles);

			try (RegionFileWriter writer = new RegionFileParquetWriter(this.path)) {
				BatchingIterator<RegionRow> batchingIter = BatchingIterator.batch(mergedStream, ROW_COMPARATOR);

				while (batchingIter.hasNext()) {
					Collection<RegionRow> batchGroup = batchingIter.next();

					LOG.info("Examining {} rows for compaction", batchGroup.size());

					Iterator<RegionRow> iter = batchGroup.iterator();
					while (iter.hasNext()) {
						RegionRow row = iter.next();
						if (row.hasCell()) {
							LOG.info("Writing row [version: {}][{}]", row.getRowId().getVersion(),
									Arrays.toString(row.getRowId().getRowKey().toByteArray()));
							writer.writeRow(row);
						} else {
							if (preserveTombstones) {
								LOG.info("Writing tombstone [version: {}][{}]", row.getRowId().getVersion(),
										Arrays.toString(row.getRowId().getRowKey().toByteArray()));
								writer.writeRow(row);
							}
							break;
						}
					}
				}
				keyRange = writer.getKeyRange();
				maxVersion = writer.getMaxVersion();
			} catch (IOException ioe) {
				throw new UncheckedIOException(ioe);
			} catch (Exception e) {
				throw new RuntimeException(e);
			} finally {
				readers.forEach(reader -> {
					try {
						reader.close();
					} catch (Exception e) {
						LOG.error("Failed to close", e);
					}
				});
			}

			return new RegionFile(this.id.toString(), this.path, keyRange, maxVersion);
		}

		public RegionFile build() {
			LOG.info("Writing RegionFile: {}", this.path);

			Range<ByteString> keyRange = null;
			long maxVersion = -1L;

			try (RegionFileWriter writer = new RegionFileParquetWriter(this.path)) {
				for (RegionRow row : rowMap.values()) {
					writer.writeRow(row);
				}

				keyRange = writer.getKeyRange();
				maxVersion = writer.getMaxVersion();
			} catch (IOException ioe) {
				throw new UncheckedIOException(ioe);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}

			return new RegionFile(this.id.toString(), this.path, keyRange, maxVersion);
		}
	}

	@Override
	protected NavigableMap<RowId, RegionRow> delegate() {
		throw new UnsupportedOperationException();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		return prime * ((id == null) ? 0 : id.hashCode());
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (getClass() != obj.getClass())
			return false;
		RegionFile other = (RegionFile) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "RegionFile [id=" + id + ", path=" + path + ", keyRange=" + keyRange + ", maxVersion=" + maxVersion
				+ "]";
	}

}
