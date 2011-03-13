/*
 * Copyright 2011 the original author or authors.
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

import java.lang.reflect.Constructor;
import java.nio.charset.Charset;

import org.springframework.beans.BeanUtils;
import org.springframework.util.Assert;

/**
 * Simple toString() serializer for the core (lang) numberic JDK types.
 * 
 * @see String#valueOf(Object)
 * @see Long#valueOf(String)
 * @author Costin Leau
 */
public class BasicNumberToStringSerializer<T extends Number> implements RedisSerializer<T> {

	private final Charset charset;
	private final Constructor<T> ctor;

	public BasicNumberToStringSerializer(Class<T> type) {
		this(type, Charset.forName("UTF8"));
	}

	public BasicNumberToStringSerializer(Class<T> type, Charset charset) {
		Assert.notNull(type);
		this.charset = charset;

		if (!(Byte.class.isAssignableFrom(type) || Short.class.isAssignableFrom(type)
				|| Long.class.isAssignableFrom(type) || Integer.class.isAssignableFrom(type)
				|| Float.class.isAssignableFrom(type) || Double.class.isAssignableFrom(type))) {
			throw new IllegalArgumentException("Type " + type + " not supported");
		}

		try {
			ctor = type.getConstructor(String.class);
		} catch (Exception ex) {
			throw new IllegalArgumentException("Cannot find suitable constructor for " + type);
		}
	}

	@Override
	public T deserialize(byte[] bytes) {
		String string = new String(bytes, charset);
		return BeanUtils.instantiateClass(ctor, string);
	}

	@Override
	public byte[] serialize(T object) {
		String string = String.valueOf(object);
		return string.getBytes(charset);
	}
}