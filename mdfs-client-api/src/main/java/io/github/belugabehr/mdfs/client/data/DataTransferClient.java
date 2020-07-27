package io.github.belugabehr.mdfs.client.data;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;

import io.github.belugabehr.mdfs.api.DataTransferOperations.DeleteBlockRequest;
import io.github.belugabehr.mdfs.api.DataTransferOperations.ReadBlockRequest;
import io.github.belugabehr.mdfs.api.DataTransferOperations.WriteBlockChunksRequest;
import io.github.belugabehr.mdfs.api.DataTransferOperations.WriteBlockRequest;
import io.github.belugabehr.mdfs.api.Mdfs.MBlockChunk;

public interface DataTransferClient extends AutoCloseable {

	void request(URI node, WriteBlockRequest request) throws IOException;

	void request(URI node, WriteBlockChunksRequest request) throws IOException;

	void request(URI node, DeleteBlockRequest request) throws IOException;

	Collection<MBlockChunk> request(URI node, ReadBlockRequest request) throws IOException;
}
