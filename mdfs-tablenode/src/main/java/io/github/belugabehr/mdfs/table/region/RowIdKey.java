package io.github.belugabehr.mdfs.table.region;

import static io.github.belugabehr.mdfs.table.region.RegionComparators.ROWID_VERSIONED_COMPARATOR;

import java.util.Comparator;

import io.github.belugabehr.mdfs.region.Region.RegionRow;
import io.github.belugabehr.mdfs.region.Region.RegionRow.RowId;

public final class RowIdKey implements Comparable<RowIdKey> {

	private final RowId rowId;
	private final Comparator<RowId> comparator;

	private RowIdKey(RegionRow.RowId rowId, Comparator<RowId> comparator) {
		this.rowId = rowId;
		this.comparator = comparator;
	}

	@Override
	public int compareTo(RowIdKey other) {
		return this.comparator.compare(this.rowId, other.rowId);
	}

	@Override
	public String toString() {
		return "RowIdKey [rowId=" + rowId + ", comparator=" + comparator + "]";
	}

	public static RowIdKey of(RegionRow.RowId rowId, Comparator<RowId> comparator) {
		return new RowIdKey(rowId, comparator);
	}

	public static RowIdKey of(RegionRow.RowId rowId) {
		return new RowIdKey(rowId, ROWID_VERSIONED_COMPARATOR);
	}
}
