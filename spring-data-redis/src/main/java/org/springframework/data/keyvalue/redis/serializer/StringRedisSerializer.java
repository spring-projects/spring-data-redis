/*
 * Copyright 2010-2011 the original author or authors.
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
package org.springframework.data.keyvalue.redis.serializer;

import java.nio.charset.Charset;

import org.springframework.util.Assert;

/**
 * Simple String to byte[] (and back) serializer. Converts Strings into bytes and vice-versa
 * using the specified charset (by default UTF-8).
 * <p/> 
 * Useful when the interaction with the Redis happens mainly through Strings.
 * 
 * <p/> Converts null into empty arrays (which get translated into empty strings on deserialization).
 * 
 * @author Costin Leau
 */
public class StringRedisSerializer implements RedisSerializer<String> {

	private final static byte[] EMPTY_ARRAY = new byte[0];
	private final String EMPTY_STRING = "";
	private final Charset charset;

	public StringRedisSerializer() {
		this(Charset.forName("UTF8"));
	}

	public StringRedisSerializer(Charset charset) {
		Assert.notNull(charset);
		this.charset = charset;
	}

	@Override
	public String deserialize(byte[] bytes) {
		return (SerializerUtils.isEmpty(bytes) ? EMPTY_STRING : new String(bytes, charset));
	}

	@Override
	public byte[] serialize(String string) {
		return (string == null ? EMPTY_ARRAY : string.getBytes(charset));
	}
}