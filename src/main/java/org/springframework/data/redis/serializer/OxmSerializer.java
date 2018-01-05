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
package org.springframework.data.redis.serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.lang.Nullable;
import org.springframework.oxm.Marshaller;
import org.springframework.oxm.Unmarshaller;
import org.springframework.util.Assert;

/**
 * Serializer adapter on top of Spring's O/X Mapping. Delegates serialization/deserialization to OXM {@link Marshaller}
 * and {@link Unmarshaller}. <b>Note:</b> {@literal null} objects are serialized as empty arrays and vice versa.
 *
 * @author Costin Leau
 * @author Mark Paluch
 */
public class OxmSerializer implements InitializingBean, RedisSerializer<Object> {

	private @Nullable Marshaller marshaller;
	private @Nullable Unmarshaller unmarshaller;

	/**
	 * Creates a new, uninitialized {@link OxmSerializer}. Requires {@link #setMarshaller(Marshaller)} and
	 * {@link #setUnmarshaller(Unmarshaller)} to be set before this serializer can be used.
	 */
	public OxmSerializer() {}

	/**
	 * Creates a new {@link OxmSerializer} given {@link Marshaller} and {@link Unmarshaller}.
	 *
	 * @param marshaller must not be {@literal null}.
	 * @param unmarshaller must not be {@literal null}.
	 */
	public OxmSerializer(Marshaller marshaller, Unmarshaller unmarshaller) {

		setMarshaller(marshaller);
		setUnmarshaller(unmarshaller);
	}

	/**
	 * @param marshaller The marshaller to set.
	 */
	public void setMarshaller(Marshaller marshaller) {

		Assert.notNull(marshaller, "Marshaller must not be null!");

		this.marshaller = marshaller;
	}

	/**
	 * @param unmarshaller The unmarshaller to set.
	 */
	public void setUnmarshaller(Unmarshaller unmarshaller) {

		Assert.notNull(unmarshaller, "Unmarshaller must not be null!");

		this.unmarshaller = unmarshaller;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	@Override
	public void afterPropertiesSet() {

		Assert.state(marshaller != null, "non-null marshaller required");
		Assert.state(unmarshaller != null, "non-null unmarshaller required");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.serializer.RedisSerializer#deserialize(byte[])
	 */
	@Override
	public Object deserialize(@Nullable byte[] bytes) throws SerializationException {

		if (SerializationUtils.isEmpty(bytes)) {
			return null;
		}

		try {
			return unmarshaller.unmarshal(new StreamSource(new ByteArrayInputStream(bytes)));
		} catch (Exception ex) {
			throw new SerializationException("Cannot deserialize bytes", ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.serializer.RedisSerializer#serialize(java.lang.Object)
	 */
	@Override
	public byte[] serialize(@Nullable Object t) throws SerializationException {

		if (t == null) {
			return SerializationUtils.EMPTY_ARRAY;
		}

		ByteArrayOutputStream stream = new ByteArrayOutputStream();
		StreamResult result = new StreamResult(stream);

		try {
			marshaller.marshal(t, result);
		} catch (Exception ex) {
			throw new SerializationException("Cannot serialize object", ex);
		}
		return stream.toByteArray();
	}
}
