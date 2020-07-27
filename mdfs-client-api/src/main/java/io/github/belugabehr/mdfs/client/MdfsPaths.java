package io.github.belugabehr.mdfs.client;

import java.net.URI;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public final class MdfsPaths {

	/** The URI scheme for the file system */
	public static final String URI_SCHEME = "mdfs";

	public static final String PATH_SEPARATOR = "/";

	public static final char PATH_SEPARATOR_CHAR = '/';

	/**
	 * /ns/uuid
	 *
	 * @param uri
	 * @return
	 */
	public static Map.Entry<String, String> parse(URI uri) {
		Objects.requireNonNull(uri);
		List<String> parts = Arrays.asList(uri.getPath().split(PATH_SEPARATOR)).stream().filter(p -> !p.isBlank())
				.collect(Collectors.toList());
		if (parts.size() != 2) {
			throw new IllegalArgumentException();
		}
		return new AbstractMap.SimpleEntry<>(parts.get(0), parts.get(1));
	}

	public static Map.Entry<String, String> parse(Path path) {
		Objects.requireNonNull(path);
		return parse(path.toUri());
	}

	/**
	 * /ns/uuid
	 *
	 * @param uri
	 * @return
	 */
	public static String parseNamespace(Path path) {
		Objects.requireNonNull(path);
		List<String> parts = Arrays.asList(path.toUri().getPath().split(PATH_SEPARATOR)).stream()
				.filter(p -> !p.isBlank()).collect(Collectors.toList());
		if (parts.size() != 2 && parts.size() != 1) {
			throw new IllegalArgumentException();
		}
		return parts.get(0);
	}

}
