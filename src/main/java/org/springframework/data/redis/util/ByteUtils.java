/*
 * Copyright 2015-2019 the original author or authors.
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
package org.springframework.data.redis.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.util.Assert;

/**
 * Some handy methods for dealing with byte arrays.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.7
 */
public final class ByteUtils {

	private ByteUtils() {}

	public static byte[] concat(byte[] arg1, byte[] arg2) {

		byte[] result = Arrays.copyOf(arg1, arg1.length + arg2.length);
		System.arraycopy(arg2, 0, result, arg1.length, arg2.length);

		return result;
	}

	public static byte[] concatAll(byte[]... args) {

		if (args.length == 0) {
			return new byte[] {};
		}
		if (args.length == 1) {
			return args[0];
		}

		byte[] cur = concat(args[0], args[1]);
		for (int i = 2; i < args.length; i++) {
			cur = concat(cur, args[i]);
		}
		return cur;
	}

	public static byte[][] split(byte[] source, int c) {

		if (source == null || source.length == 0) {
			return new byte[][] {};
		}

		List<byte[]> bytes = new ArrayList<byte[]>();
		int offset = 0;
		for (int i = 0; i <= source.length; i++) {

			if (i == source.length) {

				bytes.add(Arrays.copyOfRange(source, offset, i));
				break;
			}

			if (source[i] == c) {
				bytes.add(Arrays.copyOfRange(source, offset, i));
				offset = i + 1;
			}
		}
		return bytes.toArray(new byte[bytes.size()][]);
	}

	/**
	 * Merge multiple {@code byte} arrays into one array
	 *
	 * @param firstArray must not be {@literal null}
	 * @param additionalArrays must not be {@literal null}
	 * @return
	 */
	public static byte[][] mergeArrays(byte[] firstArray, byte[]... additionalArrays) {

		Assert.notNull(firstArray, "first array must not be null");
		Assert.notNull(additionalArrays, "additional arrays must not be null");

		byte[][] result = new byte[additionalArrays.length + 1][];
		result[0] = firstArray;
		System.arraycopy(additionalArrays, 0, result, 1, additionalArrays.length);

		return result;
	}

	/**
	 * Tests if the {@code haystack} starts with the given {@code prefix}.
	 *
	 * @param haystack the source to scan.
	 * @param prefix the prefix to find.
	 * @return {@literal true} if {@code haystack} at position {@code offset} starts with {@code prefix}.
	 * @since 1.8.10
	 * @see #startsWith(byte[], byte[], int)
	 */
	public static boolean startsWith(byte[] haystack, byte[] prefix) {
		return startsWith(haystack, prefix, 0);
	}

	/**
	 * Tests if the {@code haystack} beginning at the specified {@code offset} starts with the given {@code prefix}.
	 *
	 * @param haystack the source to scan.
	 * @param prefix the prefix to find.
	 * @param offset the offset to start at.
	 * @return {@literal true} if {@code haystack} at position {@code offset} starts with {@code prefix}.
	 * @since 1.8.10
	 */
	public static boolean startsWith(byte[] haystack, byte[] prefix, int offset) {

		int to = offset;
		int prefixOffset = 0;
		int prefixLength = prefix.length;

		if ((offset < 0) || (offset > haystack.length - prefixLength)) {
			return false;
		}

		while (--prefixLength >= 0) {
			if (haystack[to++] != prefix[prefixOffset++]) {
				return false;
			}
		}

		return true;
	}

	/**
	 * Searches the specified array of bytes for the specified value. Returns the index of the first matching value in the
	 * {@code haystack}s natural order or {@code -1} of {@code needle} could not be found.
	 *
	 * @param haystack the source to scan.
	 * @param needle the value to scan for.
	 * @return index of first appearance, or -1 if not found.
	 * @since 1.8.10
	 */
	public static int indexOf(byte[] haystack, byte needle) {

		for (int i = 0; i < haystack.length; i++) {
			if (haystack[i] == needle) {
				return i;
			}
		}

		return -1;
	}
}
