/*
 * Copyright 2015 the original author or authors.
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

import java.util.Arrays;

/**
 * Some handy methods for dealing with byte arrays.
 * 
 * @author Christoph Strobl
 */
public final class ByteUtils {

	private ByteUtils() {}

	public static boolean startsWith(byte[] source, byte[] prefix) {

		if (source == null && prefix == null) {
			return true;
		}

		if (source == null || prefix == null) {
			return false;
		}

		if (prefix.length > source.length) {
			return false;
		}

		for (int i = 0; i < prefix.length; i++) {
			if (source[i] != prefix[i]) {
				return false;
			}
		}
		return true;
	}

	public static byte[] concat(byte[] arg1, byte[] arg2) {

		byte[] result = Arrays.copyOf(arg1, arg1.length + arg2.length);
		System.arraycopy(arg2, 0, result, arg1.length, arg2.length);

		return result;
	}

	public static byte[] concatAll(byte[]... args) {

		byte[] cur = concat(args[0], args[1]);
		for (int i = 2; i < args.length; i++) {
			cur = concat(cur, args[i]);
		}
		return cur;

	}

	public static byte[] extract(byte[] source, byte[] prefix, byte[] postfix) {

		if (prefix.length > source.length) {
			return new byte[] {};
		}

		for (int i = 0; i < prefix.length; i++) {
			if (source[i] != prefix[i]) {
				return new byte[] {};
			}
		}

		for (int i = 0; i < postfix.length; i++) {
			for (int j = prefix.length + i; j < source.length; j++) {
				if (source[j] == postfix[i]) {
					return Arrays.copyOfRange(source, prefix.length, j);
				}
			}
		}

		return new byte[] {};
	}
}
