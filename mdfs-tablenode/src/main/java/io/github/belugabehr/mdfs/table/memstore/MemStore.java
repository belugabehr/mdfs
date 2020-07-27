package io.github.belugabehr.mdfs.table.memstore;

import static io.github.belugabehr.mdfs.table.region.RegionComparators.ROWID_VERSIONED_COMPARATOR;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ForwardingNavigableMap;
import com.google.protobuf.ByteString;

import io.github.belugabehr.mdfs.region.Region.RegionRow;
import io.github.belugabehr.mdfs.region.Region.RegionRow.RowId;
import io.github.belugabehr.mdfs.tablenode.RegionOperations.RegionRowProjection;
import io.github.belugabehr.mdfs.tablenode.RegionOperations.RegionScanRequest;

public class MemStore extends ForwardingNavigableMap<RegionRow.RowId, RegionRow> {

	private static final Logger LOG = LoggerFactory.getLogger(MemStore.class);

	private final ConcurrentNavigableMap<RegionRow.RowId, RegionRow> memTable;

	private final AtomicLong size = new AtomicLong();

	private final Lock lock = new ReentrantLock(true);
	private final Condition newEntry = lock.newCondition();

	public MemStore() {
		this.memTable = new ConcurrentSkipListMap<>(ROWID_VERSIONED_COMPARATOR);
	}

	@Override
	protected NavigableMap<RegionRow.RowId, RegionRow> delegate() {
		return this.memTable;
	}

	@Override
	public RegionRow put(final RegionRow.RowId id, final RegionRow row) {
		LOG.info("Putting record in MemStore: {}", row);
		super.put(row.getRowId(), row);
		size.addAndGet(row.getSerializedSize());

		lock.lock();
		try {
			newEntry.signalAll();
		} finally {
			lock.unlock();
		}
		return row;
	}

	public Collection<RegionRow> scan(final RegionScanRequest request) {
		LOG.info("Scanning the MemStore: {}", request);
		if (this.memTable.isEmpty()) {
			return Collections.emptyList();
		}

//		RegionRow.RowId minRowId = RegionRow.RowId.newBuilder().setRowKey(request.getStartKey()).build();
//		RegionRow.RowId maxRowId = RegionRow.RowId.newBuilder().setRowKey(request.getStopKey()).build();
//
//		Collection<RegionRow> candidates = this.memTable.subMap(minRowId, maxRowId).values();
//		LOG.info("Candidates: {}", candidates);
//
//		List<RegionRow> results = new ArrayList<>();
//		for (RegionRow regionRow : candidates) {
//			if (isProjected(regionRow, request.getProjectionList())) {
//				results.add(regionRow);
//			}
//		}
		return null;
	}

	private boolean isProjected(RegionRow regionRow, List<RegionRowProjection> projections) {
		if (projections.isEmpty()) {
			return true;
		}
		for (RegionRowProjection projection : projections) {
			if (projection.getColumnFamily().equals(regionRow.getRowId().getColumnFamily())) {
				switch (projection.getColumnCase()) {
				case COLUMN_NAME:
					if (regionRow.getRowId().getColumnQualifier().equals(projection.getColumnName())) {
						return true;
					}
					break;
				case COLUMN_NOT_SET:
					return true;
				}
			}
		}
		return false;
	}

	public Collection<RegionRow> get(final ByteString minRowId, final ByteString maxRowId) {

		RegionRow.RowId minId = RegionRow.RowId.newBuilder().setRowKey(minRowId).setColumnFamily("")
				.setColumnQualifier("").setVersion(0L).build();

		ConcurrentNavigableMap<RowId, RegionRow> tailMap = this.memTable.tailMap(minId);
		Collection<RegionRow> results = new ArrayList<>();
		for (final Entry<RowId, RegionRow> entry : tailMap.entrySet()) {
			if (ByteString.unsignedLexicographicalComparator().compare(entry.getKey().getRowKey(), maxRowId) < 0) {
				results.add(entry.getValue());
			} else {
				break;
			}
		}

		return results;
	}

	public void remove(final Collection<Entry<RegionRow.RowId, RegionRow>> entries) {
		LOG.info("Removing {} records from MemStore", entries.size());
		for (final Entry<RegionRow.RowId, RegionRow> entry : entries) {
			this.memTable.remove(entry.getKey(), entry.getValue());
			this.size.addAndGet(-entry.getValue().getSerializedSize());
		}
	}

	public Collection<Entry<RegionRow.RowId, RegionRow>> getEntrySet() {
		return Collections.unmodifiableSet(new HashSet<>(this.memTable.entrySet()));
	}

	public long getSize() {
		return this.size.longValue();
	}

	public boolean waitForSize(final long size, final long timeout, final TimeUnit unit) throws InterruptedException {
		long nanos = unit.toNanos(timeout);
		lock.lock();
		try {
			while (this.size.longValue() < size) {
				if (nanos <= 0L) {
					return false;
				}
				nanos = newEntry.awaitNanos(nanos);
			}
			return true;
		} finally {
			lock.unlock();
		}
	}

	@Override
	public String toString() {
		return "MemStore [memTable=" + memTable + ", size=" + size + "]";
	}
}
