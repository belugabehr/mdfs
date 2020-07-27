package io.github.belugabehr.mdfs.fsp;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchEvent.Modifier;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;

final class MdfsPath implements Path {

	private final FileSystem fs;
	private final URI uri;

	public MdfsPath(FileSystem fs, URI uri) {
		this.fs = fs;
		this.uri = uri;
	}

	@Override
	public int compareTo(Path that) {
		return this.uri.compareTo(that.toUri());
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
		return false;
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
		// TODO: Rely on super implementation
		return new MdfsPath(this.fs, this.uri.resolve(other));
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
		return this.uri;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((uri == null) ? 0 : uri.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MdfsPath other = (MdfsPath) obj;
		if (uri == null) {
			if (other.uri != null)
				return false;
		} else if (!uri.equals(other.uri))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "MfsPath [uri=" + uri + "]";
	}

}
