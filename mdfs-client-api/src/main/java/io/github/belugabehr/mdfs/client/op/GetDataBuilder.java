package io.github.belugabehr.mdfs.client.op;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;

public interface GetDataBuilder {

	GetDataBuilder fromPath(Path path);

	byte[] getBytes() throws IOException;

	ByteBuffer asByteBuffer() throws IOException;

	InputStream asInputStream() throws IOException;

}
