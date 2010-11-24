/*
 * Copyright 2010 the original author or authors.
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

/**
 * Simple String to byte[] (and back) serializer. Relies on the specified charset
 * to properly convert the String into bytes and vice-versa.
 * 
 * @author Costin Leau
 */
public class StringRedisSerializer implements RedisSerializer<String> {

	private final Charset charset;

	public StringRedisSerializer() {
		this(Charset.forName("UTF8"));
	}

	public StringRedisSerializer(Charset charset) {
		this.charset = charset;
	}

	@Override
	public String deserialize(byte[] bytes) {
		return new String(bytes, charset);
	}

	@Override
	public byte[] serialize(String object) {
		return object.toString().getBytes(charset);
	}
}
