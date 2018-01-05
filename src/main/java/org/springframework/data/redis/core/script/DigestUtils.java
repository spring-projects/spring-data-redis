/*
 * Copyright 2013-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.core.script;

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Utilties for working with {@link MessageDigest}
 *
 * @author Jennifer Hickey
 */
abstract public class DigestUtils {

	private static final char[] HEX_CHARS = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e',
			'f' };

	private static final Charset UTF8_CHARSET = Charset.forName("UTF8");

	/**
	 * Returns the SHA1 of the provided data
	 *
	 * @param data The data to calculate, such as the contents of a file
	 * @return The human-readable SHA1
	 */
	public static String sha1DigestAsHex(String data) {
		byte[] dataBytes = getDigest("SHA").digest(data.getBytes(UTF8_CHARSET));
		return new String(encodeHex(dataBytes));
	}

	private static char[] encodeHex(byte[] data) {
		int l = data.length;
		char[] out = new char[l << 1];
		for (int i = 0, j = 0; i < l; i++) {
			out[j++] = HEX_CHARS[(0xF0 & data[i]) >>> 4];
			out[j++] = HEX_CHARS[0x0F & data[i]];
		}
		return out;
	}

	/**
	 * Creates a new {@link MessageDigest} with the given algorithm. Necessary because {@code MessageDigest} is not
	 * thread-safe.
	 */
	private static MessageDigest getDigest(String algorithm) {
		try {
			return MessageDigest.getInstance(algorithm);
		} catch (NoSuchAlgorithmException ex) {
			throw new IllegalStateException("Could not find MessageDigest with algorithm \"" + algorithm + "\"", ex);
		}
	}
}
