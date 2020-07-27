package io.github.belugabehr.mdfs.fsp;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemAlreadyExistsException;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.belugabehr.mdfs.client.MdfsFramework;
import io.github.belugabehr.mdfs.client.MdfsPaths;
import io.github.belugabehr.mdfs.client.util.MdfsDirectoryStream;
import io.github.belugabehr.mdfs.client.util.MdfsReadableByteChannel;
import io.github.belugabehr.mdfs.client.util.MdfsWritableByteChannel;

public class MdfsFileSystemProvider extends FileSystemProvider {

	private static final Logger LOG = LoggerFactory.getLogger(MdfsFileSystemProvider.class);

	private static final MdfsFileSystemProvider INSTANCE = new MdfsFileSystemProvider();

	private static final Map<URI, MdfsFileSystem> FS_CACHE = new ConcurrentHashMap<>();

	/** Returns the singleton instance of this provider. */
	public static MdfsFileSystemProvider instance() {
		return INSTANCE;
	}

	@Override
	public void checkAccess(Path path, AccessMode... arg1) throws IOException {
		throw new NoSuchFileException(path + " does not exist");

	}

	@Override
	public void copy(Path source, Path target, CopyOption... options) throws IOException {
	}

	@Override
	public void createDirectory(Path arg0, FileAttribute<?>... arg1) throws IOException {
		throw new UnsupportedOperationException();

	}

	@Override
	public void delete(Path arg0) throws IOException {

	}

	@Override
	public <V extends FileAttributeView> V getFileAttributeView(Path arg0, Class<V> arg1, LinkOption... arg2) {
		return null;
	}

	@Override
	public FileStore getFileStore(Path path) throws IOException {
		return null;
	}

	@Override
	public FileSystem getFileSystem(URI uri) {
		FileSystem fileSystem = FS_CACHE.get(uri);
		if (fileSystem == null) {
			throw new FileSystemNotFoundException(uri.toString());
		}
		return fileSystem;
	}

	@Override
	public Path getPath(URI uri) {
		URI fsURI = toFileSystemUri(uri);
		FileSystem fs = FS_CACHE.get(fsURI);
		if (fs == null) {
			throw new FileSystemNotFoundException();
		}
		return new MdfsPath(fs, uri);
	}

	@Override
	public String getScheme() {
		return MdfsPaths.URI_SCHEME;
	}

	@Override
	public boolean isHidden(Path path) throws IOException {
		return false;
	}

	@Override
	public boolean isSameFile(Path path1, Path path2) throws IOException {
		return path1.equals(path2);
	}

	@Override
	public void move(Path arg0, Path arg1, CopyOption... arg2) throws IOException {
	}

	@Override
	public SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... arg2)
			throws IOException {
		LOG.warn("Options: {}", options);

		MdfsFileSystem fs = (MdfsFileSystem) path.getFileSystem();
		MdfsFramework client = fs.getClient();

		if (options.contains(StandardOpenOption.WRITE)) {
			return new MdfsWritableByteChannel(client, path, options);
		}
		if (options.contains(StandardOpenOption.READ)) {
			return new MdfsReadableByteChannel(client, path, options);
		}

		throw new UnsupportedOperationException("Must be READ or WRITE");
	}

	@Override
	public DirectoryStream<Path> newDirectoryStream(Path path, Filter<? super Path> filter) throws IOException {
		MdfsFileSystem fs = (MdfsFileSystem) path.getFileSystem();
		MdfsFramework client = fs.getClient();
		return new MdfsDirectoryStream(client, path, filter);
	}

	@Override
	public FileSystem newFileSystem(URI uri, Map<String, ?> env) throws IOException {
		if (FS_CACHE.containsKey(uri)) {
			throw new FileSystemAlreadyExistsException(uri.toString());
		}

		@SuppressWarnings("unchecked")
		Collection<String> hosts = (Collection<String>) env.get("hosts");

		try {
			MdfsFileSystem fs = new MdfsFileSystem.Builder().addHosts(hosts).build();
			FS_CACHE.put(uri, fs);
			return fs;
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public FileSystem newFileSystem(Path path, Map<String, ?> env) throws IOException {
		return newFileSystem(path.toUri(), env);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options)
			throws IOException {
		LOG.warn("readAttributes: {}", type.getCanonicalName());
		return (A) new MdfsBasicFileAttributes();
	}

	@Override
	public OutputStream newOutputStream(Path path, OpenOption... options) throws IOException {
		for (OpenOption opt : options) {
			if (opt == StandardOpenOption.APPEND) {
				throw new UnsupportedOperationException("'" + opt + "' not allowed");
			}
		}
		return super.newOutputStream(path, options);
	}

	@Override
	public InputStream newInputStream(Path path, OpenOption... arg1) throws IOException {
		return super.newInputStream(path, StandardOpenOption.READ);
	}

	@Override
	public Map<String, Object> readAttributes(Path path, String arg1, LinkOption... arg2) throws IOException {
		throw new NoSuchFileException(path + " does not exist");
	}

	@Override
	public void setAttribute(Path arg0, String arg1, Object arg2, LinkOption... arg3) throws IOException {
	}

	private static URI toFileSystemUri(URI uri) {
		try {
			return new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(), uri.getPort(), null, null, null);
		} catch (URISyntaxException e) {
			throw new AssertionError(e);
		}
	}

}
