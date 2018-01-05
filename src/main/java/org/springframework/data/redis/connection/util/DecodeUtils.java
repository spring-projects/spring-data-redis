/*
 * Copyright 2011-2018 the original author or authors.
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
package org.springframework.data.redis.connection.util;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.lang.Nullable;

/**
 * Simple class containing various decoding utilities.
 *
 * @author Costin Leau
 * @auhtor Christoph Strobl
 * @auhtor Mark Paluch
 */
public abstract class DecodeUtils {

	public static String decode(byte[] bytes) {
		return new String(Base64.getDecoder().decode(bytes));
	}

	public static String[] decodeMultiple(byte[]... bytes) {
		String[] result = new String[bytes.length];
		for (int i = 0; i < bytes.length; i++) {
			result[i] = decode(bytes[i]);
		}
		return result;
	}

	@Nullable
	public static byte[] encode(@Nullable String string) {
		return (string == null ? null : Base64.getEncoder().encode(string.getBytes()));
	}

	public static Map<byte[], byte[]> encodeMap(Map<String, byte[]> map) {
		Map<byte[], byte[]> result = new LinkedHashMap<>(map.size());
		for (Map.Entry<String, byte[]> entry : map.entrySet()) {
			result.put(encode(entry.getKey()), entry.getValue());
		}
		return result;
	}

	public static Map<String, byte[]> decodeMap(Map<byte[], byte[]> tuple) {
		Map<String, byte[]> result = new LinkedHashMap<>(tuple.size());
		for (Map.Entry<byte[], byte[]> entry : tuple.entrySet()) {
			result.put(decode(entry.getKey()), entry.getValue());
		}
		return result;
	}

	public static Set<byte[]> convertToSet(Collection<String> keys) {
		Set<byte[]> set = new LinkedHashSet<>(keys.size());

		for (String string : keys) {
			set.add(encode(string));
		}
		return set;
	}

	public static List<byte[]> convertToList(Collection<String> keys) {
		List<byte[]> set = new ArrayList<>(keys.size());

		for (String string : keys) {
			set.add(encode(string));
		}
		return set;
	}
}
