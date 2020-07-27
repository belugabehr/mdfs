package io.github.belugabehr.mdfs.table.region;

import java.util.Comparator;

import com.google.protobuf.ByteString;

import io.github.belugabehr.mdfs.region.Region.RegionRow;
import io.github.belugabehr.mdfs.region.Region.RegionRow.RowId;

public final class RegionComparators {

	public static final Comparator<RegionRow> ROW_VERSIONED_COMPARATOR = Comparator
			.comparing(RegionRow::getRowId, (rid1, rid2) -> {
				return ByteString.unsignedLexicographicalComparator().compare(rid1.getRowKey(), rid2.getRowKey());
			})
			.thenComparing(RegionRow::getRowId,
					(rid1, rid2) -> rid1.getColumnFamily().compareTo(rid2.getColumnFamily()))
			.thenComparing(RegionRow::getRowId, (rid1, rid2) -> rid1.getColumnQualifier().compareTo(rid2.getColumnQualifier()))
			.thenComparingLong(row -> -row.getRowId().getVersion());

	public static final Comparator<RegionRow> ROW_COMPARATOR = Comparator
			.comparing(RegionRow::getRowId, (rid1, rid2) -> {
				return ByteString.unsignedLexicographicalComparator().compare(rid1.getRowKey(), rid2.getRowKey());
			})
			.thenComparing(RegionRow::getRowId,
					(rid1, rid2) -> rid1.getColumnFamily().compareTo(rid2.getColumnFamily()))
			.thenComparing(RegionRow::getRowId, (rid1, rid2) -> rid1.getColumnQualifier().compareTo(rid2.getColumnQualifier()));

	public static final Comparator<RegionRow.RowId> ROWID_VERSIONED_COMPARATOR = Comparator
			.comparing(RowId::getRowKey, ByteString.unsignedLexicographicalComparator())
			.thenComparing(RowId::getColumnFamily).thenComparing(RowId::getColumnQualifier)
			.thenComparingLong(rid -> -rid.getVersion());

	public static final Comparator<RegionRow.RowId> ROWID_COMPARATOR = Comparator
			.comparing(RowId::getRowKey, ByteString.unsignedLexicographicalComparator())
			.thenComparing(RowId::getColumnFamily).thenComparing(RowId::getColumnQualifier);

	private RegionComparators() {

	}
}
