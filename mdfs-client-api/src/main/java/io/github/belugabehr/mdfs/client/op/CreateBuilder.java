package io.github.belugabehr.mdfs.client.op;

import java.io.IOException;
import java.nio.file.Path;

import io.github.belugabehr.mdfs.api.Mdfs.Replication;

public interface CreateBuilder {

	CreateBuilder withPath(Path path);

	CreateBuilder withReplication(Replication replication);

	void copyFromFile(final Path source) throws IOException;
}
