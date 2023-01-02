/*
 * Copyright 2015-2023 the original author or authors.
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
package org.springframework.data.redis.serializer;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.util.ReflectionTestUtils.*;
import static org.springframework.util.ObjectUtils.*;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateSerializer;
import lombok.Data;
import lombok.ToString;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.BeanUtils;
import org.springframework.cache.support.NullValue;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.annotation.JsonView;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectMapper.DefaultTyping;
import com.fasterxml.jackson.databind.jsontype.TypeResolverBuilder;
import com.fasterxml.jackson.databind.type.TypeFactory;
import org.springframework.lang.Nullable;

/**
 * Unit tests for {@link GenericJackson2JsonRedisSerializer}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
class GenericJackson2JsonRedisSerializerUnitTests {

	private static final SimpleObject SIMPLE_OBJECT = new SimpleObject(1L);
	private static final ComplexObject COMPLEX_OBJECT = new ComplexObject("steelheart", SIMPLE_OBJECT);

	@Test // DATAREDIS-392
	void shouldUseDefaultTyping() {
		assertThat(extractTypeResolver(new GenericJackson2JsonRedisSerializer())).isNotNull();
	}

	@Test // DATAREDIS-392
	void shouldUseDefaultTypingWhenClassPropertyNameIsEmpty() {

		TypeResolverBuilder<?> typeResolver = extractTypeResolver(new GenericJackson2JsonRedisSerializer(""));
		assertThat((String) getField(typeResolver, "_typeProperty"))
				.isEqualTo(JsonTypeInfo.Id.CLASS.getDefaultPropertyName());
	}

	@Test // DATAREDIS-392
	void shouldUseDefaultTypingWhenClassPropertyNameIsNull() {

		TypeResolverBuilder<?> typeResolver = extractTypeResolver(new GenericJackson2JsonRedisSerializer((String) null));
		assertThat((String) getField(typeResolver, "_typeProperty"))
				.isEqualTo(JsonTypeInfo.Id.CLASS.getDefaultPropertyName());
	}

	@Test // DATAREDIS-392
	void shouldUseDefaultTypingWhenClassPropertyNameIsProvided() {

		TypeResolverBuilder<?> typeResolver = extractTypeResolver(new GenericJackson2JsonRedisSerializer("firefight"));
		assertThat((String) getField(typeResolver, "_typeProperty")).isEqualTo("firefight");
	}

	@Test // DATAREDIS-392
	void serializeShouldReturnEmptyByteArrayWhenSouceIsNull() {
		assertThat(new GenericJackson2JsonRedisSerializer().serialize(null)).isEqualTo(SerializationUtils.EMPTY_ARRAY);
	}

	@Test // DATAREDIS-392
	void deserializeShouldReturnNullWhenSouceIsNull() {
		assertThat(new GenericJackson2JsonRedisSerializer().deserialize(null)).isNull();
	}

	@Test // DATAREDIS-392
	void deserializeShouldReturnNullWhenSouceIsEmptyArray() {
		assertThat(new GenericJackson2JsonRedisSerializer().deserialize(SerializationUtils.EMPTY_ARRAY)).isNull();
	}

	@Test // DATAREDIS-392
	void deserializeShouldBeAbleToRestoreSimpleObjectAfterSerialization() {

		GenericJackson2JsonRedisSerializer serializer = new GenericJackson2JsonRedisSerializer();

		assertThat((SimpleObject) serializer.deserialize(serializer.serialize(SIMPLE_OBJECT))).isEqualTo(SIMPLE_OBJECT);
	}

	@Test // DATAREDIS-392
	void deserializeShouldBeAbleToRestoreComplexObjectAfterSerialization() {

		GenericJackson2JsonRedisSerializer serializer = new GenericJackson2JsonRedisSerializer();

		assertThat((ComplexObject) serializer.deserialize(serializer.serialize(COMPLEX_OBJECT))).isEqualTo(COMPLEX_OBJECT);
	}

	@Test // DATAREDIS-392
	void serializeShouldThrowSerializationExceptionProcessingError() throws JsonProcessingException {

		ObjectMapper objectMapperMock = mock(ObjectMapper.class);
		when(objectMapperMock.writeValueAsBytes(any())).thenThrow(new JsonGenerationException("nightwielder"));

		assertThatExceptionOfType(SerializationException.class)
				.isThrownBy(() -> new GenericJackson2JsonRedisSerializer(objectMapperMock).serialize(SIMPLE_OBJECT));
	}

	@Test // DATAREDIS-392
	void deserializeShouldThrowSerializationExceptionProcessingError() throws IOException {

		ObjectMapper objectMapperMock = mock(ObjectMapper.class);
		when(objectMapperMock.readValue(Mockito.any(byte[].class), Mockito.any(Class.class)))
				.thenThrow(new JsonMappingException("conflux"));

		assertThatExceptionOfType(SerializationException.class)
				.isThrownBy(() -> new GenericJackson2JsonRedisSerializer(objectMapperMock).deserialize(new byte[] { 1 }));
	}

	@Test // DATAREDIS-553, DATAREDIS-865
	void shouldSerializeNullValueSoThatItCanBeDeserializedWithDefaultTypingEnabled() {

		GenericJackson2JsonRedisSerializer serializer = new GenericJackson2JsonRedisSerializer();

		serializeAndDeserializeNullValue(serializer);
	}

	@Test // DATAREDIS-865
	void shouldSerializeNullValueWithCustomObjectMapper() {

		ObjectMapper mapper = new ObjectMapper();
		mapper.enableDefaultTyping(DefaultTyping.EVERYTHING, As.PROPERTY);

		GenericJackson2JsonRedisSerializer.registerNullValueSerializer(mapper, null);
		GenericJackson2JsonRedisSerializer serializer = new GenericJackson2JsonRedisSerializer(mapper);

		serializeAndDeserializeNullValue(serializer);
	}

	@Test // GH-1566
	void deserializeShouldBeAbleToRestoreFinalObjectAfterSerialization() {

		GenericJackson2JsonRedisSerializer serializer = new GenericJackson2JsonRedisSerializer();

		FinalObject source = new FinalObject();
		source.longValue = 1L;
		source.myArray = new int[] { 1, 2, 3 };
		source.simpleObject = new SimpleObject(2L);

		assertThat(serializer.deserialize(serializer.serialize(source))).isEqualTo(source);
		assertThat(serializer.deserialize(
				("{\"@class\":\"org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializerUnitTests$FinalObject\",\"longValue\":1,\"myArray\":[1,2,3],\n"
						+ "\"simpleObject\":{\"@class\":\"org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializerUnitTests$SimpleObject\",\"longValue\":2}}")
								.getBytes())).isEqualTo(source);
	}

	@Test // GH-2361
	void shouldDeserializePrimitiveArrayWithoutTypeHint() {

		GenericJackson2JsonRedisSerializer gs = new GenericJackson2JsonRedisSerializer();
		CountAndArray result = (CountAndArray) gs.deserialize(
				("{\"@class\":\"org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializerUnitTests$CountAndArray\", \"count\":1, \"available\":[0,1]}")
						.getBytes());

		assertThat(result.getCount()).isEqualTo(1);
		assertThat(result.getAvailable()).containsExactly(0, 1);
	}

	@Test // GH-2322
	void readsToMapForNonDefaultTyping() {

		GenericJackson2JsonRedisSerializer serializer = new GenericJackson2JsonRedisSerializer(new ObjectMapper());

		User user = new User();
		user.email = "walter@heisenberg.com";
		user.id = 42;
		user.name = "Walter White";

		byte[] serializedValue = serializer.serialize(user);

		Object deserializedValue = serializer.deserialize(serializedValue, Object.class);
		assertThat(deserializedValue).isInstanceOf(Map.class);
	}

	@Test // GH-2322
	void shouldConsiderWriter() {

		User user = new User();
		user.email = "walter@heisenberg.com";
		user.id = 42;
		user.name = "Walter White";

		GenericJackson2JsonRedisSerializer serializer = new GenericJackson2JsonRedisSerializer((String) null,
				JacksonObjectReader.create(), (mapper, source) -> {
					return mapper.writerWithView(Views.Basic.class).writeValueAsBytes(source);
				});

		byte[] result = serializer.serialize(user);

		assertThat(new String(result)).contains("id").contains("name").doesNotContain("email");
	}

	@Test // GH-2322
	void considersWriterForCustomObjectMapper() {

		GenericJackson2JsonRedisSerializer serializer = new GenericJackson2JsonRedisSerializer(new ObjectMapper(),
				JacksonObjectReader.create(), (mapper, source) -> {
					return mapper.writerWithView(Views.Basic.class).writeValueAsBytes(source);
				});

		User user = new User();
		user.email = "walter@heisenberg.com";
		user.id = 42;
		user.name = "Walter White";

		byte[] serializedValue = serializer.serialize(user);

		assertThat(new String(serializedValue)).contains("id").contains("name").doesNotContain("email");
	}

	@Test // GH-2322
	void shouldConsiderReader() {

		User user = new User();
		user.email = "walter@heisenberg.com";
		user.id = 42;
		user.name = "Walter White";

		GenericJackson2JsonRedisSerializer serializer = new GenericJackson2JsonRedisSerializer((String) null,
				(mapper, source, type) -> {
					if (type.getRawClass() == User.class) {
						return mapper.readerWithView(Views.Basic.class).forType(type).readValue(source);
					}
					return mapper.readValue(source, type);
				}, JacksonObjectWriter.create());

		byte[] serializedValue = serializer.serialize(user);

		Object result = serializer.deserialize(serializedValue);
		assertThat(result).isInstanceOf(User.class).satisfies(it -> {
			User u = (User) it;
			assertThat(u.id).isEqualTo(user.id);
			assertThat(u.name).isEqualTo(user.name);
			assertThat(u.email).isNull();
			assertThat(u.mobile).isNull();
		});
	}

	@Test // GH-2361
	void shouldDeserializePrimitiveWrapperArrayWithoutTypeHint() {

		GenericJackson2JsonRedisSerializer gs = new GenericJackson2JsonRedisSerializer();
		CountAndArray result = (CountAndArray) gs.deserialize(
				("{\"@class\":\"org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializerUnitTests$CountAndArray\", \"count\":1, \"arrayOfPrimitiveWrapper\":[0,1]}")
						.getBytes());

		assertThat(result.getCount()).isEqualTo(1);
		assertThat(result.getArrayOfPrimitiveWrapper()).containsExactly(0L, 1L);
	}

	@Test // GH-2361
	void doesNotIncludeTypingForPrimitiveArrayWrappers() {

		GenericJackson2JsonRedisSerializer serializer = new GenericJackson2JsonRedisSerializer();

		WithWrapperTypes source = new WithWrapperTypes();
		source.primitiveWrapper = new AtomicReference<>();
		source.primitiveArrayWrapper = new AtomicReference<>(new Integer[] { 200, 300 });
		source.simpleObjectWrapper = new AtomicReference<>();

		byte[] serializedValue = serializer.serialize(source);

		assertThat(new String(serializedValue)) //
				.contains("\"primitiveArrayWrapper\":[200,300]") //
				.doesNotContain("\"[Ljava.lang.Integer;\"");

		assertThat(serializer.deserialize(serializedValue)) //
				.isInstanceOf(WithWrapperTypes.class) //
				.satisfies(it -> {
					WithWrapperTypes deserialized = (WithWrapperTypes) it;
					assertThat(deserialized.primitiveArrayWrapper).hasValue(source.primitiveArrayWrapper.get());
				});
	}

	@Test // GH-2361
	void doesNotIncludeTypingForPrimitiveWrappers() {

		GenericJackson2JsonRedisSerializer serializer = new GenericJackson2JsonRedisSerializer();

		WithWrapperTypes source = new WithWrapperTypes();
		source.primitiveWrapper = new AtomicReference<>(123L);

		byte[] serializedValue = serializer.serialize(source);

		assertThat(new String(serializedValue)) //
				.contains("\"primitiveWrapper\":123") //
				.doesNotContain("\"Ljava.lang.Long;\"");

		assertThat(serializer.deserialize(serializedValue)) //
				.isInstanceOf(WithWrapperTypes.class) //
				.satisfies(it -> {
					WithWrapperTypes deserialized = (WithWrapperTypes) it;
					assertThat(deserialized.primitiveWrapper).hasValue(source.primitiveWrapper.get());
				});
	}

	@Test // GH-2361
	void includesTypingForWrappedObjectTypes() {

		GenericJackson2JsonRedisSerializer serializer = new GenericJackson2JsonRedisSerializer();

		SimpleObject simpleObject = new SimpleObject(100L);
		WithWrapperTypes source = new WithWrapperTypes();
		source.simpleObjectWrapper = new AtomicReference<>(simpleObject);

		byte[] serializedValue = serializer.serialize(source);

		assertThat(new String(serializedValue)) //
				.contains(
						"\"simpleObjectWrapper\":{\"@class\":\"org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializerUnitTests$SimpleObject\",\"longValue\":100}");

		assertThat(serializer.deserialize(serializedValue)) //
				.isInstanceOf(WithWrapperTypes.class) //
				.satisfies(it -> {
					WithWrapperTypes deserialized = (WithWrapperTypes) it;
					assertThat(deserialized.simpleObjectWrapper).hasValue(source.simpleObjectWrapper.get());
				});
	}

	@Test // GH-2396
	void verifySerializeUUIDIntoBytes() {

		GenericJackson2JsonRedisSerializer serializer = new GenericJackson2JsonRedisSerializer();

		UUID source = UUID.fromString("730145fe-324d-4fb1-b12f-60b89a045730");
		assertThat(serializer.serialize(source)).isEqualTo(("\"" + source + "\"").getBytes(StandardCharsets.UTF_8));
	}

	@Test // GH-2396
	void deserializesUUIDFromBytes() {

		GenericJackson2JsonRedisSerializer serializer = new GenericJackson2JsonRedisSerializer();
		UUID deserializedUuid = serializer
				.deserialize("\"730145fe-324d-4fb1-b12f-60b89a045730\"".getBytes(StandardCharsets.UTF_8), UUID.class);

		assertThat(deserializedUuid).isEqualTo(UUID.fromString("730145fe-324d-4fb1-b12f-60b89a045730"));
	}

	@Test // GH-2396
	void serializesEnumIntoBytes() {

		GenericJackson2JsonRedisSerializer serializer = new GenericJackson2JsonRedisSerializer();

		assertThat(serializer.serialize(EnumType.ONE)).isEqualTo(("\"ONE\"").getBytes(StandardCharsets.UTF_8));
	}

	@Test // GH-2396
	void deserializesEnumFromBytes() {

		GenericJackson2JsonRedisSerializer serializer = new GenericJackson2JsonRedisSerializer();

		assertThat(serializer.deserialize("\"TWO\"".getBytes(StandardCharsets.UTF_8), EnumType.class)).isEqualTo(EnumType.TWO);
	}

	@Test // GH-2396
	void serializesJavaTimeIntoBytes() {

		GenericJackson2JsonRedisSerializer serializer = new GenericJackson2JsonRedisSerializer();

		WithJsr310 source = new WithJsr310();
		source.myDate = java.time.LocalDate.of(2022,9,2);

		assertThat(serializer.serialize(source)).isEqualTo(("{\"@class\":\"org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializerUnitTests$WithJsr310\",\"myDate\":[2022,9,2]}").getBytes(StandardCharsets.UTF_8));
	}

	@Test // GH-2396
	void deserializesJavaTimeFrimBytes() {

		GenericJackson2JsonRedisSerializer serializer = new GenericJackson2JsonRedisSerializer();

		byte[] source = "{\"@class\":\"org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializerUnitTests$WithJsr310\",\"myDate\":[2022,9,2]}".getBytes(StandardCharsets.UTF_8);
		assertThat(serializer.deserialize(source, WithJsr310.class).myDate).isEqualTo(java.time.LocalDate.of(2022,9,2));
	}

	private static void serializeAndDeserializeNullValue(GenericJackson2JsonRedisSerializer serializer) {

		NullValue nv = BeanUtils.instantiateClass(NullValue.class);

		byte[] serializedValue = serializer.serialize(nv);
		assertThat(serializedValue).isNotNull();

		Object deserializedValue = serializer.deserialize(serializedValue);
		assertThat(deserializedValue).isInstanceOf(NullValue.class);
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
		public boolean equals(@Nullable Object obj) {
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

	@Data
	static final class FinalObject {
		public Long longValue;
		public int[] myArray;
		SimpleObject simpleObject;
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
		public boolean equals(@Nullable Object obj) {

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

	@ToString
	static class User {
		@JsonView(Views.Basic.class) public int id;
		@JsonView(Views.Basic.class) public String name;
		@JsonView(Views.Detailed.class) public String email;
		@JsonView(Views.Detailed.class) public String mobile;
	}

	static class Views {

		interface Basic {}

		interface Detailed {}
	}

	@Data
	static class CountAndArray {

		private int count;
		private int[] available;
		private Long[] arrayOfPrimitiveWrapper;
	}

	@Data
	static class WithWrapperTypes {

		AtomicReference<Long> primitiveWrapper;
		AtomicReference<Integer[]> primitiveArrayWrapper;
		AtomicReference<SimpleObject> simpleObjectWrapper;
	}

	enum EnumType {
		ONE, TWO
	}

	static class WithJsr310 {
		@JsonSerialize(using = LocalDateSerializer.class)
		@JsonDeserialize(using = LocalDateDeserializer.class)
		private LocalDate myDate;
	}
}
