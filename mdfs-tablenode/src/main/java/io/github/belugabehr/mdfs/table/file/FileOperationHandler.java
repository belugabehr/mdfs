package io.github.belugabehr.mdfs.table.file;

import java.util.Collection;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.common.base.Preconditions;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;

import io.github.belugabehr.mdfs.api.FileOperations.CreateFileRequest;
import io.github.belugabehr.mdfs.api.FileOperations.CreateFileResponse;
import io.github.belugabehr.mdfs.api.FileOperations.DeleteFileRequest;
import io.github.belugabehr.mdfs.api.FileOperations.DeleteFileResponse;
import io.github.belugabehr.mdfs.api.FileOperations.FileOperationErrorResponse;
import io.github.belugabehr.mdfs.api.FileOperations.FileOperationRequest;
import io.github.belugabehr.mdfs.api.FileOperations.FileOperationResponse;
import io.github.belugabehr.mdfs.api.FileOperations.FinalizeFileRequest;
import io.github.belugabehr.mdfs.api.FileOperations.FinalizeFileResponse;
import io.github.belugabehr.mdfs.api.FileOperations.ListFileRequest;
import io.github.belugabehr.mdfs.api.FileOperations.ListFileResponse;
import io.github.belugabehr.mdfs.api.Mdfs.MFile;
import io.github.belugabehr.mdfs.api.Mdfs.MFile.MFileId;
import io.github.belugabehr.mdfs.region.Region.RegionRow;
import io.github.belugabehr.mdfs.region.Region.RegionRow.RowId;
import io.github.belugabehr.mdfs.table.ClusterProperties;
import io.github.belugabehr.mdfs.table.region.Region;
import io.github.belugabehr.mdfs.table.region.RegionComparators;
import io.github.belugabehr.mdfs.table.region.RegionMapRepository;
import io.github.belugabehr.mdfs.table.utils.BatchingIterator;
import io.github.belugabehr.mdfs.table.utils.KeyUtils;
import io.github.belugabehr.mdfs.table.utils.TimestampUtils;

@Service
public class FileOperationHandler {

	private static final Logger LOG = LoggerFactory.getLogger(FileOperationHandler.class);

	@Autowired
	private FileManager fileManager;

	@Autowired
	private RegionMapRepository regionMapRepository;

	@Autowired
	private ClusterProperties clusterProperties;

	public void handle(long seqno, FileOperationRequest request) {
		Objects.requireNonNull(request);

		FileOperationResponse.Builder response = FileOperationResponse.newBuilder();
		try {
			switch (request.getRequestCase()) {
			case CREATEFILEREQUEST:
				response.setCreateFileResponse(handle(seqno, request.getCreateFileRequest()));
				break;
			case FINALIZEFILEREQUEST:
				response.setFinalizeFileResponse(handle(seqno, request.getFinalizeFileRequest()));
				break;
			case LISTFILEREQUEST:
				response.setListFileResponse(handle(seqno, request.getListFileRequest()));
				break;
			case DELETEFILEREQUEST:
				response.setDeleteFileResponse(handle(seqno, request.getDeleteFileRequest()));
				break;
			case REQUEST_NOT_SET:
				break;
			default:
				break;
			}
		} catch (Exception e) {
			response.setErrorResponse(FileOperationErrorResponse.newBuilder().setErrorMessage(e.getMessage()));
		}
		fileManager.response(request, response.build());
	}

	private DeleteFileResponse handle(long seqno, DeleteFileRequest deleteFileRequest) {

		final MFileId fileId = deleteFileRequest.getFileId();
		final ByteString fileRowKey = KeyUtils.getRowKey(fileId.getNamespaceBytes(), Optional.of(fileId.getId()));

		this.regionMapRepository.readLock();

		try {
			final Region region = this.regionMapRepository.get(fileRowKey);
			Preconditions.checkState(region != null, "File does not exist in any region hosted on this server");

			// Check the the row requested for deletion is the one that will be deleted
			RowId existingRowId = RegionRow.RowId.newBuilder().setRowKey(fileRowKey).setColumnFamily("file")
					.setColumnQualifier("meta").setVersion(Long.MAX_VALUE).build();

			final Entry<RowId, RegionRow> entry = region.ceilingEntry(existingRowId);
			Preconditions.checkState(region != null, "File does not exist in any region hosted on this server");
			Preconditions.checkState(entry.getKey().getRowKey().equals(fileRowKey));

			final RegionRow candidate = entry.getValue();

			if (candidate.hasCell()) {
				final MFile file = candidate.getCell().unpack(MFile.class);

				if (deleteFileRequest.hasCreationTime()) {
					Preconditions.checkState(file.getCreationTime().equals(deleteFileRequest.getCreationTime()),
							"Creation time does not match");
				}

//				region.remove(candidate.getRowId());
				final MFile deletedFile = MFile.newBuilder(file).setDeletionTime(TimestampUtils.now()).build();

				final RegionRow newRow = RegionRow.newBuilder(entry.getValue())
						.setRowId(RegionRow.RowId.newBuilder(entry.getKey()).setVersion(seqno))
						.setCell(Any.pack(deletedFile)).build();

				region.put(entry.getKey(), newRow);
			}

			return DeleteFileResponse.newBuilder().setOk(true).build();
		} catch (Exception e) {
			LOG.error("Error", e);
			throw new RuntimeException(e);
		} finally {
			this.regionMapRepository.readUnlock();
		}
	}

	private ListFileResponse handle(long seqno, ListFileRequest listFileRequest) {

		final ByteString namespaceId = listFileRequest.getListByNamespace().getNamespaceBytes();
		final Entry<ByteString, ByteString> minMaxRowIds = KeyUtils.getRowPrefix(namespaceId);

		LOG.warn("Min:[{}], Max:[{}]", minMaxRowIds.getKey(), minMaxRowIds.getValue());

		this.regionMapRepository.readLock();

		try {

			final Collection<Region> regions = this.regionMapRepository.get(minMaxRowIds.getKey(),
					minMaxRowIds.getValue());

			Preconditions.checkArgument(!regions.isEmpty(), "Key does not belong in any region hosted on this server");

			final RegionRow.RowId minRowIdRecord = RegionRow.RowId.newBuilder().setRowKey(minMaxRowIds.getKey())
					.setColumnFamily("").setColumnQualifier("").setVersion(0L).build();
			final RegionRow.RowId maxRowIdRecord = RegionRow.RowId.newBuilder().setRowKey(minMaxRowIds.getValue())
					.setColumnFamily("").setColumnQualifier("").setVersion(0L).build();

			final ListFileResponse.Builder response = ListFileResponse.newBuilder();

			for (final Region region : regions) {
				final SortedMap<RegionRow.RowId, RegionRow> sortedMap = region.subMap(minRowIdRecord, maxRowIdRecord);

				BatchingIterator<RegionRow> batchingIter = BatchingIterator.batch(sortedMap.values(),
						RegionComparators.ROW_COMPARATOR);
				while (batchingIter.hasNext()) {
					Collection<RegionRow> rows = batchingIter.next();
					Optional<RegionRow> candidate = rows.stream().sequential().limit(1L).findFirst();
					if (candidate.isPresent() && candidate.get().hasCell()) {
						final MFile file = candidate.get().getCell().unpack(MFile.class);
						if (file.hasFinalizeTime() && !file.hasDeletionTime()) {
							response.addFile(listFileRequest.getLongListing() ? file
									: MFile.newBuilder(file).clearBlock().build());
						}
					}
				}
			}

			// Location should be full URI
			LOG.info("Response");
			return response.build();
		} catch (Exception e) {
			LOG.error("Error", e);
		} finally {
			this.regionMapRepository.readUnlock();
		}
		return null;
	}

	private FinalizeFileResponse handle(long seqno, FinalizeFileRequest finalizeFileRequest) {

		final MFile mFile = finalizeFileRequest.getFile();
		final ByteString fileRowKey = KeyUtils.getRowKey(mFile.getFileId().getNamespaceBytes(),
				Optional.of(mFile.getFileId().getId()));

		this.regionMapRepository.readLock();

		try {
			final Region region = this.regionMapRepository.get(fileRowKey);
			Preconditions.checkState(region != null, "File does not exist in any region hosted on this server");

			RowId rowId = RegionRow.RowId.newBuilder().setRowKey(fileRowKey).setColumnFamily("file")
					.setColumnQualifier("meta").setVersion(Long.MAX_VALUE).build();

			final Entry<RowId, RegionRow> entry = region.ceilingEntry(rowId);
			Preconditions.checkState(entry != null, "File does not exist in any region hosted on this server");
			Preconditions.checkState(entry.getKey().getRowKey().equals(fileRowKey));

			// TODO: Check if oldRow is already finalized?
			final RegionRow oldRow = entry.getValue();

			final MFile finalizedfile = MFile.newBuilder(mFile).setFinalizeTime(TimestampUtils.now()).build();

			final RegionRow newRow = RegionRow.newBuilder(oldRow)
					.setRowId(RegionRow.RowId.newBuilder(oldRow.getRowId()).setVersion(seqno))
					.setCell(Any.pack(finalizedfile)).build();

			region.put(newRow.getRowId(), newRow);

			// Location should be full URI
			return FinalizeFileResponse.newBuilder().setFile(finalizedfile).build();
		} catch (Exception e) {
			LOG.error("Error", e);
			throw new RuntimeException(e);
		} finally {
			this.regionMapRepository.readUnlock();
		}
	}

	protected CreateFileResponse handle(long seqno, CreateFileRequest createFileRequest) {
		final MFile mFile = createFileRequest.getFile();

		final ByteString fileRowKey = KeyUtils.getRowKey(mFile.getFileId().getNamespaceBytes(),
				Optional.of(mFile.getFileId().getId()));

		final MFile createdFile = MFile.newBuilder(mFile).setCreationTime(TimestampUtils.now()).build();

		this.regionMapRepository.readLock();
		try {
			final Region region = this.regionMapRepository.get(fileRowKey);

			Preconditions.checkArgument(region != null, "Key does not belong in any region hosted on this server");

			RegionRow row = RegionRow.newBuilder().setRowId(RowId.newBuilder().setRowKey(fileRowKey)
					.setColumnFamily("file").setColumnQualifier("meta").setVersion(seqno))
					.setCell(Any.pack(createdFile)).build();

			region.put(row.getRowId(), row);
		} catch (Exception e) {
			LOG.error("Error", e);
		} finally {
			this.regionMapRepository.readUnlock();
		}

		CreateFileResponse.Builder builder = CreateFileResponse.newBuilder().setFile(mFile);
		for (String datanode : this.clusterProperties.getDatanodes()) {
			builder.addLocation("tcp://" + datanode);
		}
		return builder.build();
	}

}
