package io.github.belugabehr.mdfs.client.file;

import io.github.belugabehr.mdfs.api.FileOperations.CreateFileRequest;
import io.github.belugabehr.mdfs.api.FileOperations.CreateFileResponse;
import io.github.belugabehr.mdfs.api.FileOperations.DeleteFileRequest;
import io.github.belugabehr.mdfs.api.FileOperations.DeleteFileResponse;
import io.github.belugabehr.mdfs.api.FileOperations.FinalizeFileRequest;
import io.github.belugabehr.mdfs.api.FileOperations.FinalizeFileResponse;
import io.github.belugabehr.mdfs.api.FileOperations.ListFileRequest;
import io.github.belugabehr.mdfs.api.FileOperations.ListFileResponse;

public interface FileOperationClient extends AutoCloseable {
	CreateFileResponse request(CreateFileRequest request);
	
	FinalizeFileResponse request(FinalizeFileRequest request);
	
	ListFileResponse request(ListFileRequest request);
	
	DeleteFileResponse request(DeleteFileRequest request);
}
