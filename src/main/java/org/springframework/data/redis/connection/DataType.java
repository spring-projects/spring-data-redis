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

package org.springframework.data.redis.connection;

import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Enumeration of the Redis data types.
 *
 * @author Costin Leau
 */
public enum DataType {

	NONE("none"), STRING("string"), LIST("list"), SET("set"), ZSET("zset"), HASH("hash");

	private static final Map<String, DataType> codeLookup = new ConcurrentHashMap<>(6);

	static {
		for (DataType type : EnumSet.allOf(DataType.class))
			codeLookup.put(type.code, type);

	}

	private final String code;

	DataType(String name) {
		this.code = name;
	}

	/**
	 * Returns the code associated with the current enum.
	 *
	 * @return code of this enum
	 */
	public String code() {
		return code;
	}

	/**
	 * Utility method for converting an enum code to an actual enum.
	 *
	 * @param code enum code
	 * @return actual enum corresponding to the given code
	 */
	public static DataType fromCode(String code) {
		DataType data = codeLookup.get(code);
		if (data == null)
			throw new IllegalArgumentException("unknown data type code");
		return data;
	}
}
