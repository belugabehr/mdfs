package io.github.belugabehr.mdfs.table.region;

import static io.github.belugabehr.mdfs.table.region.RegionComparators.ROWID_VERSIONED_COMPARATOR;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ForwardingNavigableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import io.github.belugabehr.mdfs.region.Region.RegionRow;
import io.github.belugabehr.mdfs.region.Region.RegionRow.RowId;
import io.github.belugabehr.mdfs.table.memstore.MemStore;

public final class Region extends ForwardingNavigableMap<RegionRow.RowId, RegionRow> {

	private static final Logger LOG = LoggerFactory.getLogger(Region.class);

	private static final RegionRow TOMBSTONE = RegionRow.getDefaultInstance();

	private final UUID id;
	private final Collection<RegionFile> regionFiles;
	private final MemStore memStore;
	private final ExecutorService executorService;
	private final Compactor compactor;
	private final URI root;

	private final ReadWriteLock regionFilesLock = new ReentrantReadWriteLock();

	private Region(URI root, UUID id, ExecutorService executorService, Compactor compactor) {
		this.id = Objects.requireNonNull(id);
		this.executorService = Objects.requireNonNull(executorService);
		this.compactor = Objects.requireNonNull(compactor);
		this.root = Objects.requireNonNull(root);
		this.regionFiles = new LinkedHashSet<>();
		this.memStore = new MemStore();
	}

	private Region init() {
		this.executorService.execute(() -> {
			try {
				doFlushMemStore();
			} catch (InterruptedException | IOException e) {
				LOG.error("Error", e);
			}
		});

		this.executorService.execute(() -> {
			try {
				while (true) {
					minorCompaction();
					Thread.sleep(30_000L);
				}
			} catch (InterruptedException | IOException e) {
				LOG.error("Error", e);
			}
		});

		return this;
	}

	protected void doFlushMemStore() throws IOException, InterruptedException {
		while (true) {
			final boolean added = this.memStore.waitForSize(10, 10L, TimeUnit.SECONDS);

			LOG.info("Flushing due to: {}", added ? "memstore size" : "timeout");
			flushMemStore();
		}
	}

	public RegionRow put(final RegionRow.RowId rowId, final RegionRow regionRow) {
		return this.memStore.put(rowId, regionRow);
	}

	@Override
	public SortedMap<RegionRow.RowId, RegionRow> subMap(RowId fromKey, RowId toKey) {
		SortedMap<RegionRow.RowId, RegionRow> results = new TreeMap<>(ROWID_VERSIONED_COMPARATOR);
		results.putAll(this.memStore.subMap(fromKey, toKey));
		this.regionFilesLock.readLock().lock();
		try {
			for (final RegionFile regionFile : this.regionFiles) {
				LOG.warn("RegionFile: {}", regionFile);
				results.putAll(regionFile.subMap(fromKey, toKey));
			}
		} finally {
			this.regionFilesLock.readLock().unlock();
		}
		return results;
	}

	@Override
	public Entry<RegionRow.RowId, RegionRow> ceilingEntry(RegionRow.RowId key) {

		final Entry<RegionRow.RowId, RegionRow> memstoreEntry = this.memStore.ceilingEntry(key);
		if (memstoreEntry != null && memstoreEntry.getKey().equals(key)) {
			return memstoreEntry;
		}

		final NavigableMap<RegionRow.RowId, RegionRow> results = new TreeMap<>(ROWID_VERSIONED_COMPARATOR);
		if (memstoreEntry != null) {
			results.put(memstoreEntry.getKey(), memstoreEntry.getValue());
		}

		this.regionFilesLock.readLock().lock();
		try {
			for (NavigableMap<RegionRow.RowId, RegionRow> source : this.regionFiles) {
				final Entry<RegionRow.RowId, RegionRow> entry = source.ceilingEntry(key);
				if (entry != null) {
					results.put(entry.getKey(), entry.getValue());
				}
			}
		} finally {
			this.regionFilesLock.readLock().unlock();
		}

		return results.firstEntry();
	}

	/**
	 * This break the contract of remove in that it always returns null
	 */
	@Override
	public RegionRow remove(Object object) {
		RegionRow.RowId rowid = (RegionRow.RowId) Objects.requireNonNull(object);
		RegionRow tombstoneMarker = RegionRow.newBuilder(TOMBSTONE).setRowId(rowid).build();
		put(rowid, tombstoneMarker);
		return null;
	}

	public void flushMemStore() throws IOException {
		final Map<RegionRow.RowId, RegionRow> entries = new LinkedHashMap<>(this.memStore);

		if (!entries.isEmpty()) {
			UUID regionID = UUID.randomUUID();
			Path regionFilePath = Paths.get(this.root.resolve("regionfiles/" + regionID));

			LOG.info("regionFilePath: {}", regionFilePath);
			LOG.info("Flushing {} entries to [{}][{}}", entries.size(), this.root, regionFilePath);

			RegionFile regionFile = RegionFile.newBuilder().withID(regionID).withPath(regionFilePath).withRows(entries)
					.build();

			LOG.info("Create new RegionFile [{}][{}]", regionFilePath, regionFile);

			// TODO: Bit dangerous to wait on this and block incoming requests, maybe spin
			// on the lock instead
			this.regionFilesLock.writeLock().lock();
			try {
				this.regionFiles.add(regionFile);
			} finally {
				this.regionFilesLock.writeLock().unlock();
			}
			this.memStore.remove(entries.entrySet());
		}
	}

	public void minorCompaction() throws IOException {
		LOG.info("Compacting region files");
		Set<RegionFile> targetRegionFiles = Collections.emptySet();
		Optional<RegionFile> compactedFile = Optional.empty();

		this.regionFilesLock.readLock().lock();
		try {
			targetRegionFiles = ImmutableSet.copyOf(Iterables.limit(this.regionFiles, 2));
			if (targetRegionFiles.size() > 1) {
				Path regionFilePath = Paths.get(this.root.resolve("regionfiles/" + UUID.randomUUID().toString()));
				compactedFile = Optional.of(this.compactor.compact(regionFilePath, targetRegionFiles));
			}
		} finally {
			this.regionFilesLock.readLock().unlock();
		}

		if (compactedFile.isPresent()) {
			this.regionFilesLock.writeLock().lock();
			try {
				this.regionFiles.removeAll(targetRegionFiles);
				this.regionFiles.add(compactedFile.get());
			} finally {
				this.regionFilesLock.writeLock().unlock();
			}

			LOG.info("Deleting old region files: {}", targetRegionFiles);
			targetRegionFiles.forEach(rf -> rf.delete());
		}
	}

	public UUID getId() {
		return id;
	}

	@Override
	protected NavigableMap<RowId, RegionRow> delegate() {
		throw new UnsupportedOperationException();
	}

	@Override
	public String toString() {
		return "Region [id=" + id + ", regionFiles=" + regionFiles + ", memStore=" + memStore + "]";
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
		Region other = (Region) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}

	public static Builder newBuilder() {
		return new Builder();
	}

	public static class Builder {
		private Optional<UUID> id = Optional.empty();
		private Optional<ExecutorService> executorService = Optional.empty();
		private Optional<Compactor> compactor = Optional.empty();
		private URI root = null;

		public Builder withID(UUID id) {
			this.id = Optional.ofNullable(id);
			return this;
		}

		public Builder usingExecutorService(ExecutorService execService) {
			this.executorService = Optional.ofNullable(execService);
			return this;
		}

		public Builder usingCompactor(Compactor compactor) {
			this.compactor = Optional.ofNullable(compactor);
			return this;
		}

		public Builder withRoot(URI root) {
			this.root = root;
			return this;
		}

		public Region build() {
			Objects.requireNonNull(this.root, "Root path may not be null");
			return new Region(this.root, this.id.orElseGet(UUID::randomUUID),
					this.executorService.orElseGet(Executors::newCachedThreadPool),
					this.compactor.orElseGet(DefaultCompactor::new)).init();
		}
	}

}
