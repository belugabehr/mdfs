package io.github.belugabehr.mdfs.fsp;

import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;

public final class MdfsBasicFileAttributes implements BasicFileAttributes {

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
		return 0;
	}

}
