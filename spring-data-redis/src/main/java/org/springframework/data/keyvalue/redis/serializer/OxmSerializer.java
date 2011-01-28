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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.oxm.Marshaller;
import org.springframework.oxm.Unmarshaller;
import org.springframework.util.Assert;

/**
 * Serializer adapter on top of Spring's O/X Mapping.
 * Delegates serialization/deserialization to OXM {@link Marshaller} and
 * {@link Unmarshaller}.
 * 
 * <b>Note:</b>Null objects are serialized as empty arrays.
 * 
 * @author Costin Leau
 */
public class OxmSerializer implements InitializingBean, RedisSerializer<Object> {

	private Marshaller marshaller;
	private Unmarshaller unmarshaller;

	public OxmSerializer() {
	}

	public OxmSerializer(Marshaller marshaller, Unmarshaller unmarshaller) {
		this.marshaller = marshaller;
		this.unmarshaller = unmarshaller;

		afterPropertiesSet();
	}

	@Override
	public void afterPropertiesSet() {
		Assert.notNull(marshaller, "non-null marshaller required");
		Assert.notNull(unmarshaller, "non-null unmarshaller required");
	}

	/**
	 * @param marshaller The marshaller to set.
	 */
	public void setMarshaller(Marshaller marshaller) {
		this.marshaller = marshaller;
	}

	/**
	 * @param unmarshaller The unmarshaller to set.
	 */
	public void setUnmarshaller(Unmarshaller unmarshaller) {
		this.unmarshaller = unmarshaller;
	}

	@Override
	public Object deserialize(byte[] bytes) throws SerializationException {
		if (SerializerUtils.isEmpty(bytes)) {
			return null;
		}

		try {
			return unmarshaller.unmarshal(new StreamSource(new ByteArrayInputStream(bytes)));
		} catch (Exception ex) {
			throw new SerializationException("Cannot deserialize bytes", ex);
		}
	}

	@Override
	public byte[] serialize(Object t) throws SerializationException {
		if (t == null) {
			return SerializerUtils.EMPTY_ARRAY;
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