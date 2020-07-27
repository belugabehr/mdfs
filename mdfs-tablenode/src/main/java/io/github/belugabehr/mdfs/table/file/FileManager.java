package io.github.belugabehr.mdfs.table.file;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import io.github.belugabehr.mdfs.api.FileOperations.FileOperationRequest;
import io.github.belugabehr.mdfs.api.FileOperations.FileOperationResponse;

@Service
public class FileManager {
	
	private static final Logger LOG = LoggerFactory.getLogger(FileManager.class);

	private Map<FileOperationRequest, FileOperationResponseFuture> pendingRequests;

	public FileManager() {
		this.pendingRequests = new ConcurrentHashMap<>();
	}

	public Future<FileOperationResponse> request(FileOperationRequest request) {
		FileOperationResponseFuture future = new FileOperationResponseFuture();
		this.pendingRequests.put(request, future);
		return future;
	}

	public void response(FileOperationRequest request, FileOperationResponse response) {
		LOG.info("Handle response [{}[{}]", request, response);
		final FileOperationResponseFuture pendingFuture = this.pendingRequests.get(request);
		if (pendingFuture != null) {
			LOG.info("Set!!");
			pendingFuture.set(response);
		}
	}

}
