package io.github.belugabehr.mdfs.fsp.zk;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchEvent.Modifier;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;

final class MdfsZkPath implements Path {

	private final FileSystem fs;
	private final String path;
	private Optional<URI> uriCache;

	MdfsZkPath(FileSystem fs, String path) {
		this.fs = Objects.requireNonNull(fs);
		this.path = Objects.requireNonNull(path);
		this.uriCache = Optional.empty();
	}

	@Override
	public int compareTo(Path that) {
		MdfsZkPath other = (MdfsZkPath) that;
		return Comparator.comparing(MdfsZkPath::getMdfsZkFileSystem, (fs1, fs2) -> fs1.getUri().compareTo(fs2.getUri()))
				.compare(this, other);
	}

	@Override
	public boolean endsWith(Path path) {
		return false;
	}

	@Override
	public Path getFileName() {
		return null;
	}

	@Override
	public FileSystem getFileSystem() {
		return this.fs;
	}

	public MdfsZkFileSystem getMdfsZkFileSystem() {
		return (MdfsZkFileSystem) this.fs;
	}

	public String asString() {
		return path;
	}

	@Override
	public Path getName(int arg0) {
		return null;
	}

	@Override
	public int getNameCount() {
		return 0;
	}

	@Override
	public Path getParent() {
		return null;
	}

	@Override
	public Path getRoot() {
		return null;
	}

	@Override
	public boolean isAbsolute() {
		return true;
	}

	@Override
	public Path normalize() {
		return null;
	}

	@Override
	public WatchKey register(WatchService arg0, Kind<?>[] arg1, Modifier... arg2) throws IOException {
		return null;
	}

	@Override
	public Path relativize(Path path) {
		return null;
	}

	@Override
	public Path resolve(Path path) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Path resolve(String other) {
		String newPath = String.join("/", this.path, other);
		return new MdfsZkPath(this.fs, newPath);
	}

	@Override
	public boolean startsWith(Path path) {
		return false;
	}

	@Override
	public Path subpath(int arg0, int arg1) {
		return null;
	}

	@Override
	public Path toAbsolutePath() {
		return null;
	}

	@Override
	public Path toRealPath(LinkOption... arg0) throws IOException {
		return null;
	}

	@Override
	public URI toUri() {
		if (uriCache.isPresent()) {
			return uriCache.get();
		}
		URI uri = this.getMdfsZkFileSystem().toUri(this);
		this.uriCache = Optional.of(uri);
		return uri;
	}

	@Override
	public String toString() {
		return "MdfsZkPath [fs=" + fs + ", path=" + path + "]";
	}

}
