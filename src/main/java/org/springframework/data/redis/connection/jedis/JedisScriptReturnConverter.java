/*
 * Copyright 2013-2025 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.connection.jedis;

import java.util.ArrayList;
import java.util.List;

import org.jspecify.annotations.Nullable;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.redis.connection.ReturnType;

/**
 * Converts the value returned by Jedis script eval to the expected {@link ReturnType}
 *
 * @author Jennifer Hickey
 */
public class JedisScriptReturnConverter implements Converter<Object, Object> {

	private final ReturnType returnType;

	public JedisScriptReturnConverter(ReturnType returnType) {
		this.returnType = returnType;
	}

	@SuppressWarnings({ "unchecked", "NullAway" })
	public @Nullable Object convert(@Nullable Object result) {
		if (result instanceof String stringResult) {
			// evalsha converts byte[] to String. Convert back for consistency
			return JedisConverters.toBytes(stringResult);
		}
		if (returnType == ReturnType.STATUS) {
			return JedisConverters.toString((byte[]) result);
		}
		if (returnType == ReturnType.BOOLEAN) {
			// Lua false comes back as a null bulk reply
			if (result == null) {
				return Boolean.FALSE;
			}
			return ((Long) result == 1);
		}
		if (returnType == ReturnType.MULTI && result != null) {
			List<Object> resultList = (List<Object>) result;
			List<Object> convertedResults = new ArrayList<>();
			for (Object res : resultList) {
				if (res instanceof String stringResult) {
					// evalsha converts byte[] to String. Convert back for
					// consistency
					convertedResults.add(JedisConverters.toBytes(stringResult));
				} else {
					convertedResults.add(res);
				}
			}
			return convertedResults;
		}
		return result;
	}
}
