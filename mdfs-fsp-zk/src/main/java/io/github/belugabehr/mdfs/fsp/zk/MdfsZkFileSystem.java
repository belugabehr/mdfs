package io.github.belugabehr.mdfs.fsp.zk;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchService;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import org.apache.curator.framework.CuratorFramework;

import io.github.belugabehr.mdfs.client.MdfsFramework;
import io.github.belugabehr.mdfs.client.MdfsPaths;
import io.github.belugabehr.mdfs.zk.MdfsZkFramework;

final class MdfsZkFileSystem extends FileSystem {

	private final URI uri;
	private final MdfsFramework client;

	private MdfsZkFileSystem(URI uri, MdfsFramework client) {
		this.uri = Objects.requireNonNull(uri);
		this.client = Objects.requireNonNull(client);
	}

	@Override
	public void close() throws IOException {

	}

	@Override
	public Iterable<FileStore> getFileStores() {
		return Collections.emptyList();
	}

	@Override
	public Path getPath(String arg0, String... arg1) {
		return null;
	}

	@Override
	public PathMatcher getPathMatcher(String arg0) {
		return null;
	}

	@Override
	public Iterable<Path> getRootDirectories() {
		return Collections.emptyList();
	}

	@Override
	public String getSeparator() {
		return MdfsPaths.PATH_SEPARATOR;
	}

	@Override
	public UserPrincipalLookupService getUserPrincipalLookupService() {
		return null;
	}

	@Override
	public boolean isOpen() {
		return this.client.isOpen();
	}

	public MdfsFramework getClient() {
		return this.client;
	}

	@Override
	public boolean isReadOnly() {
		return false;
	}

	@Override
	public WatchService newWatchService() throws IOException {
		return null;
	}

	@Override
	public FileSystemProvider provider() {
		return MdfsZkFileSystemProvider.instance();
	}

	@Override
	public Set<String> supportedFileAttributeViews() {
		return Collections.emptySet();
	}

	public URI toUri(MdfsZkPath path) {
		return this.uri.resolve(path.asString());
	}

	public URI getUri() {
		return uri;
	}

	public static Builder newBuilder() {
		return new Builder();
	}

	static class Builder {
		private CuratorFramework curator;
		private URI uri;

		public Builder usingCurator(CuratorFramework curator) {
			this.curator = curator;
			return this;
		}

		public Builder withURI(URI uri) {
			this.uri = uri;
			return this;
		}

		MdfsZkFileSystem build() throws Exception {

			MdfsFramework client = MdfsZkFramework.newBuilder().usingCurator(this.curator).build();

			return new MdfsZkFileSystem(uri, client);
		}
	}

	@Override
	public String toString() {
		return "MdfsZkFileSystem [uri=" + uri + "]";
	}
}
