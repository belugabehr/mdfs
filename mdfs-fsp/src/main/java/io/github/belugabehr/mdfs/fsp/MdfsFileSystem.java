package io.github.belugabehr.mdfs.fsp;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchService;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import io.github.belugabehr.mdfs.MdfsFrameworkImpl;
import io.github.belugabehr.mdfs.client.MdfsFramework;
import io.github.belugabehr.mdfs.client.MdfsPaths;

final class MdfsFileSystem extends FileSystem {

	private final MdfsFramework mdfsClient;

	public MdfsFileSystem(MdfsFramework mdfsClient) {
		this.mdfsClient = mdfsClient;
	}

	@Override
	public void close() throws IOException {

	}

	@Override
	public Iterable<FileStore> getFileStores() {
		// TODO: A FileStore should probably be a "bucket"
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
		return null;
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
		return this.mdfsClient.isOpen();
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
		return MdfsFileSystemProvider.instance();
	}

	@Override
	public Set<String> supportedFileAttributeViews() {
		return Collections.emptySet();
	}

	public MdfsFramework getClient() {
		return mdfsClient;
	}

	static class Builder {

		private List<String> hosts = new ArrayList<>();

		Builder addHosts(Collection<String> hosts) {
			this.hosts.addAll(hosts);
			return this;
		}

		MdfsFileSystem build() throws Exception {
			MdfsFrameworkImpl mdfsClient = MdfsFrameworkImpl.newBuilder().withHost(hosts.iterator().next()).withPort(41414)
					.build();

			return new MdfsFileSystem(mdfsClient);
		}
	}

}
