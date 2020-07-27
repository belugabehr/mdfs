package io.github.belugabehr.mdfs.client.core;

import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import io.github.belugabehr.mdfs.client.MdfsFramework;
import io.github.belugabehr.mdfs.client.Stats;

public final class MdfsBasicFileAttributes implements BasicFileAttributes {

	private final MdfsFramework client;
	private final Path path;
	private final Set<LinkOption> options;
	private Optional<Stats> stats;

	public MdfsBasicFileAttributes(MdfsFramework client, Path path, LinkOption... options) {
		this.client = client;
		this.path = path;
		this.options = new HashSet<>(Arrays.asList(options));
		this.stats = Optional.empty();
	}

	@Override
	public FileTime creationTime() {
		return null;
	}

	@Override
	public Object fileKey() {
		return null;
	}

	@Override
	public boolean isDirectory() {
		return false;
	}

	@Override
	public boolean isOther() {
		return false;
	}

	@Override
	public boolean isRegularFile() {
		return true;
	}

	@Override
	public boolean isSymbolicLink() {
		return false;
	}

	@Override
	public FileTime lastAccessTime() {
		return null;
	}

	@Override
	public FileTime lastModifiedTime() {
		return null;
	}

	@Override
	public long size() {
		loadMetadata();
		return stats.get().size();
	}

	private void loadMetadata() {
		if (this.stats.isEmpty()) {
			Stats stats = this.client.checkExists().forPath(this.path);
			this.stats = Optional.of(stats);
		}
	}

}
