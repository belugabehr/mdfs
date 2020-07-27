package io.github.belugabehr.mdfs.client.op;

import java.nio.file.Path;
import java.util.Collection;

import io.github.belugabehr.mdfs.api.Mdfs.MFile;

public interface ListFilesBuilder {

	ListFilesBuilder fromNamespace(Path path);

	ListFilesBuilder asLongListing(boolean longList);

	Collection<MFile> list();
}
