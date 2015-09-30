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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Some handy methods for dealing with byte arrays.
 * 
 * @author Christoph Strobl
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
}
