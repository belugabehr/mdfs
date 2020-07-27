package io.github.belugabehr.mdfs.client.op;

import java.nio.file.Path;

import io.github.belugabehr.mdfs.client.Stats;

public interface ExistsBuilder {

	Stats forPath(Path path);

}
