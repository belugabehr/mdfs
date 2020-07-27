package io.github.belugabehr.mdfs.client.core;

import java.io.IOException;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.AbstractIterator;

import io.github.belugabehr.mdfs.api.Mdfs.MFile;
import io.github.belugabehr.mdfs.client.MdfsFramework;

public class MdfsDirectoryStream implements DirectoryStream<Path> {

	private static final Logger LOG = LoggerFactory.getLogger(MdfsDirectoryStream.class);

	private final Path path;
	private final Filter<? super Path> filter;
	private final MdfsFramework client;

	public MdfsDirectoryStream(MdfsFramework client, Path path, Filter<? super Path> filter) {
		this.client = Objects.requireNonNull(client);
		this.path = Objects.requireNonNull(path);
		this.filter = Objects.requireNonNull(filter);
	}

	@Override
	public void close() throws IOException {

	}

	@Override
	public Iterator<Path> iterator() {
		Collection<MFile> results = this.client.listFiles().fromNamespace(this.path).list();
		return new MdfsDirectoryStreamIterator(results);
	}

	class MdfsDirectoryStreamIterator extends AbstractIterator<Path> {

		private final Collection<MFile> results;
		private final Iterator<MFile> iter;

		public MdfsDirectoryStreamIterator(Collection<MFile> results) {
			this.results = results;
			this.iter = results.iterator();
		}

		@Override
		protected Path computeNext() {
			try {
				while (iter.hasNext()) {
					MFile mFile = iter.next();
					Path child = path.resolve(mFile.getFileId().getId().toStringUtf8());

					if (filter.accept(child)) {
						return child;
					}
				}
				return endOfData();
			} catch (IOException e) {
				throw new DirectoryIteratorException(e);
			}
		}

	}

}
