package io.github.belugabehr.mdfs.client;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Base64;

/**
 * https://tools.ietf.org/html/draft-thiemann-hash-urn-01
 */
public enum DataIntegrity {

	MD5("md5"), CRC32("crc32"), SHA1("sha1"), SHA_224("sha224"), SHA_256("sha256"), SHA_384("sha384"),
	SHA_512("sha512");

	private final String hashScheme;

	DataIntegrity(String hashScheme) {
		this.hashScheme = hashScheme;
	}

	/**
	 * An opaque URI is an absolute URI whose scheme-specific part does not begin
	 * with a slash character ('/'). Opaque URIs are not subject to further parsing.
	 *
	 * @param hash
	 * @return
	 */
	public URI getURI(byte[] hash) {
		try {
			return new URI("urn:hash::" + hashScheme + ":" + Base64.getEncoder().encodeToString(hash));
		} catch (URISyntaxException e) {
			throw new RuntimeException(e);
		}
	}
}
