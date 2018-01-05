/*
 * Copyright 2015-2018 the original author or authors.
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

import static org.hamcrest.core.Is.*;
import static org.hamcrest.core.IsInstanceOf.*;
import static org.hamcrest.core.IsNull.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.util.ReflectionTestUtils.*;
import static org.springframework.util.ObjectUtils.*;

import java.io.IOException;

import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.beans.BeanUtils;
import org.springframework.cache.support.NullValue;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.TypeResolverBuilder;
import com.fasterxml.jackson.databind.type.TypeFactory;

/**
 * @author Christoph Strobl
 */
public class GenericJackson2JsonRedisSerializerUnitTests {

	private static final SimpleObject SIMPLE_OBJECT = new SimpleObject(1L);
	private static final ComplexObject COMPLEX_OBJECT = new ComplexObject("steelheart", SIMPLE_OBJECT);

	@Test // DATAREDIS-392
	public void shouldUseDefaultTyping() {
		assertThat(extractTypeResolver(new GenericJackson2JsonRedisSerializer()), notNullValue());
	}

	@Test // DATAREDIS-392
	public void shouldUseDefaultTypingWhenClassPropertyNameIsEmpty() {

		TypeResolverBuilder<?> typeResolver = extractTypeResolver(new GenericJackson2JsonRedisSerializer(""));
		assertThat((String) getField(typeResolver, "_typeProperty"), is(JsonTypeInfo.Id.CLASS.getDefaultPropertyName()));
	}

	@Test // DATAREDIS-392
	public void shouldUseDefaultTypingWhenClassPropertyNameIsNull() {

		TypeResolverBuilder<?> typeResolver = extractTypeResolver(new GenericJackson2JsonRedisSerializer((String) null));
		assertThat((String) getField(typeResolver, "_typeProperty"), is(JsonTypeInfo.Id.CLASS.getDefaultPropertyName()));
	}

	@Test // DATAREDIS-392
	public void shouldUseDefaultTypingWhenClassPropertyNameIsProvided() {

		TypeResolverBuilder<?> typeResolver = extractTypeResolver(new GenericJackson2JsonRedisSerializer("firefight"));
		assertThat((String) getField(typeResolver, "_typeProperty"), is("firefight"));
	}

	@Test // DATAREDIS-392
	public void serializeShouldReturnEmptyByteArrayWhenSouceIsNull() {
		assertThat(new GenericJackson2JsonRedisSerializer().serialize(null), is(SerializationUtils.EMPTY_ARRAY));
	}

	@Test // DATAREDIS-392
	public void deserializeShouldReturnNullWhenSouceIsNull() {
		assertThat(new GenericJackson2JsonRedisSerializer().deserialize(null), nullValue());
	}

	@Test // DATAREDIS-392
	public void deserializeShouldReturnNullWhenSouceIsEmptyArray() {
		assertThat(new GenericJackson2JsonRedisSerializer().deserialize(SerializationUtils.EMPTY_ARRAY), nullValue());
	}

	@Test // DATAREDIS-392
	public void deserializeShouldBeAbleToRestoreSimpleObjectAfterSerialization() {

		GenericJackson2JsonRedisSerializer serializer = new GenericJackson2JsonRedisSerializer();

		assertThat((SimpleObject) serializer.deserialize(serializer.serialize(SIMPLE_OBJECT)), is(SIMPLE_OBJECT));
	}

	@Test // DATAREDIS-392
	public void deserializeShouldBeAbleToRestoreComplexObjectAfterSerialization() {

		GenericJackson2JsonRedisSerializer serializer = new GenericJackson2JsonRedisSerializer();

		assertThat((ComplexObject) serializer.deserialize(serializer.serialize(COMPLEX_OBJECT)), is(COMPLEX_OBJECT));
	}

	@Test(expected = SerializationException.class) // DATAREDIS-392
	public void serializeShouldThrowSerializationExceptionProcessingError() throws JsonProcessingException {

		ObjectMapper objectMapperMock = mock(ObjectMapper.class);
		when(objectMapperMock.writeValueAsBytes(any())).thenThrow(new JsonGenerationException("nightwielder"));

		new GenericJackson2JsonRedisSerializer(objectMapperMock).serialize(SIMPLE_OBJECT);
	}

	@Test(expected = SerializationException.class) // DATAREDIS-392
	public void deserializeShouldThrowSerializationExceptionProcessingError() throws IOException {

		ObjectMapper objectMapperMock = mock(ObjectMapper.class);
		when(objectMapperMock.readValue(Mockito.any(byte[].class), Mockito.any(Class.class)))
				.thenThrow(new JsonMappingException("conflux"));

		new GenericJackson2JsonRedisSerializer(objectMapperMock).deserialize(new byte[] { 1 });
	}

	@Test // DATAREDIS-553
	public void shouldSerializeNullValueSoThatItCanBeDeserializedWithDefaultTypingEnabled() {

		GenericJackson2JsonRedisSerializer serializer = new GenericJackson2JsonRedisSerializer();

		NullValue nv = BeanUtils.instantiateClass(NullValue.class);

		byte[] serializedValue = serializer.serialize(nv);
		assertThat(serializedValue, is(notNullValue()));

		Object deserializedValue = serializer.deserialize(serializedValue);
		assertThat(deserializedValue, is(instanceOf(NullValue.class)));
	}

	private TypeResolverBuilder<?> extractTypeResolver(GenericJackson2JsonRedisSerializer serializer) {

		ObjectMapper mapper = (ObjectMapper) getField(serializer, "mapper");
		return mapper.getSerializationConfig().getDefaultTyper(TypeFactory.defaultInstance().constructType(Object.class));
	}

	static class ComplexObject {

		public String stringValue;
		public SimpleObject simpleObject;

		public ComplexObject() {}

		public ComplexObject(String stringValue, SimpleObject simpleObject) {
			this.stringValue = stringValue;
			this.simpleObject = simpleObject;
		}

		@Override
		public int hashCode() {
			return nullSafeHashCode(stringValue) + nullSafeHashCode(simpleObject);
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (!(obj instanceof ComplexObject)) {
				return false;
			}
			ComplexObject other = (ComplexObject) obj;
			return nullSafeEquals(this.stringValue, other.stringValue)
					&& nullSafeEquals(this.simpleObject, other.simpleObject);
		}

	}

	static class SimpleObject {

		public Long longValue;

		public SimpleObject() {}

		public SimpleObject(Long longValue) {
			this.longValue = longValue;
		}

		@Override
		public int hashCode() {
			return nullSafeHashCode(this.longValue);
		}

		@Override
		public boolean equals(Object obj) {

			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (!(obj instanceof SimpleObject)) {
				return false;
			}
			SimpleObject other = (SimpleObject) obj;
			return nullSafeEquals(this.longValue, other.longValue);
		}
	}

}
