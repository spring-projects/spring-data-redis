/*
 * Copyright 2026-present the original author or authors.
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
import static org.junit.jupiter.params.provider.Arguments.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.core.ResolvableType;

import com.fasterxml.jackson.databind.ObjectMapper;

import tools.jackson.core.StreamReadConstraints;
import tools.jackson.core.TokenStreamFactory;
import tools.jackson.core.json.JsonFactory;
import tools.jackson.core.json.JsonReadFeature;
import tools.jackson.databind.json.JsonMapper;

/**
 * Unit tests for the raw-JSON splitting and joining of {@link RedisJsonSerializer}
 * ({@link RedisJsonSerializer#splitArray splitArray}, {@link RedisJsonSerializer#splitObject splitObject} and
 * {@link RedisJsonSerializer#joinObject joinObject}), run against the byte-slicing implementations in
 * {@link GenericJackson2JsonRedisSerializer} and {@link GenericJacksonJsonRedisSerializer} to compare their behavior,
 * and against the {@link RedisJsonSerializer} defaults to pin the behavior these three share.
 *
 * @author Moritz Halbritter
 */
class RedisJsonSerializerSplitUnitTests {

	/**
	 * The byte-slicing implementations, plus the {@link RedisJsonSerializer} defaults - which an implementation backed
	 * by a JSON library that cannot slice raw bytes (e.g. Gson) gets.
	 */
	static Stream<Arguments> serializers() {

		return Stream.concat(slicingSerializers(), Stream.of(argumentSet("defaults",
				new PrimitivesOnlySerializer(GenericJacksonJsonRedisSerializer.builder().build()))));
	}

	static Stream<Arguments> slicingSerializers() {

		return Stream.of(argumentSet("jackson2", new GenericJackson2JsonRedisSerializer()),
				argumentSet("jackson3", GenericJacksonJsonRedisSerializer.builder().build()));
	}

	private static List<String> splitArray(RedisJsonSerializer serializer, String json) {
		return serializer.splitArray(json.getBytes(StandardCharsets.UTF_8)).stream()
				.map(it -> new String(it, StandardCharsets.UTF_8)).collect(Collectors.toList());
	}

	private static Map<String, String> splitObject(RedisJsonSerializer serializer, String json) {
		return serializer.splitObject(json.getBytes(StandardCharsets.UTF_8)).entrySet().stream()
				.collect(Collectors.toMap(Map.Entry::getKey, it -> new String(it.getValue(), StandardCharsets.UTF_8)));
	}

	private static String joinObject(RedisJsonSerializer serializer, Map<String, String> members) {

		Map<String, byte[]> raw = new LinkedHashMap<>();
		members.forEach((key, value) -> raw.put(key, value.getBytes(StandardCharsets.UTF_8)));

		return new String(serializer.joinObject(raw), StandardCharsets.UTF_8);
	}

	@ParameterizedTest
	@MethodSource("serializers")
	void splitsArrays(RedisJsonSerializer serializer) {

		assertThat(splitArray(serializer, "[]")).isEmpty();
		assertThat(splitArray(serializer, "[\"John\"]")).containsExactly("\"John\"");
		assertThat(splitArray(serializer, "[1,2,3]")).containsExactly("1", "2", "3");
		assertThat(splitArray(serializer, "[ 1 , 2 , 3 ]")).containsExactly("1", "2", "3");

		// a JSON null element stays distinct from an absent value
		assertThat(splitArray(serializer, "[null]")).containsExactly("null");

		// nested containers, and separators inside strings, do not split
		assertThat(splitArray(serializer, "[[\"admin\",\"dev\"],[\"ops\"]]")).containsExactly("[\"admin\",\"dev\"]",
				"[\"ops\"]");
		assertThat(splitArray(serializer, "[\"a,b\",\"c\"]")).containsExactly("\"a,b\"", "\"c\"");
		assertThat(splitArray(serializer, "[\"a\\\"b\",\"c\"]")).containsExactly("\"a\\\"b\"", "\"c\"");
	}

	@ParameterizedTest
	@MethodSource("serializers")
	void splitsObjects(RedisJsonSerializer serializer) {

		assertThat(splitObject(serializer, "{}")).isEmpty();
		assertThat(splitObject(serializer, "{\"$.a\":[1],\"$.b\":[2]}")).containsExactly(entry("$.a", "[1]"),
				entry("$.b", "[2]"));

		// member names are handed out unescaped
		assertThat(splitObject(serializer, "{\"$['a\\\"b']\":1}")).containsExactly(entry("$['a\"b']", "1"));

		// duplicate member names are legal JSON but cannot both survive a Map, so last-wins is pinned rather than
		// incidental; RedisJSON never sends a duplicate, as each member is a distinct requested path
		assertThat(splitObject(serializer, "{\"a\":1,\"a\":2}")).containsExactly(entry("a", "2"));
	}

	@ParameterizedTest
	@MethodSource("serializers")
	void joinsObjects(RedisJsonSerializer serializer) {

		assertThat(joinObject(serializer, Map.of())).isEqualTo("{}");

		Map<String, String> members = new LinkedHashMap<>();
		members.put("b", "2");
		members.put("a", "{\"nested\":[1,null,\"x\"]}");
		members.put("n", "null");

		assertThat(joinObject(serializer, members)).isEqualTo("{\"b\":2,\"a\":{\"nested\":[1,null,\"x\"]},\"n\":null}");
		assertThat(joinObject(serializer, Map.of("a\"b", "1"))).isEqualTo("{\"a\\\"b\":1}");
		assertThat(joinObject(serializer, Map.of("", "1"))).isEqualTo("{\"\":1}");
	}

	/**
	 * {@code joinObject} is documented as the inverse of {@code splitObject}, so the pair has to round-trip for the
	 * shapes {@code RedisJsonTemplate} feeds through it - including JSONPath keys, which carry characters that need
	 * escaping.
	 */
	@ParameterizedTest
	@MethodSource("serializers")
	void roundTripsSplitAndJoin(RedisJsonSerializer serializer) {

		Map<String, String> members = new LinkedHashMap<>();
		members.put("$['a\"b']", "[1]");
		members.put("$..city", "[\"Rand\"]");
		members.put("plain", "null");

		assertThat(splitObject(serializer, joinObject(serializer, members))).isEqualTo(members);
	}

	/**
	 * A wrong shape surfaces as the {@link SerializationException} the SPI documents - raised by an explicit check in
	 * the slicing overrides, and by the underlying {@code deserialize} in the defaults.
	 */
	@ParameterizedTest
	@MethodSource("serializers")
	void rejectsMismatchedOpening(RedisJsonSerializer serializer) {

		assertThatExceptionOfType(SerializationException.class)
				.isThrownBy(() -> serializer.splitArray("{\"a\":1}".getBytes()));
		assertThatExceptionOfType(SerializationException.class).isThrownBy(() -> serializer.splitObject("[1]".getBytes()));
	}

	@ParameterizedTest
	@MethodSource("slicingSerializers")
	void slicingPreservesNumberPrecisionAndFormattingVerbatim(RedisJsonSerializer serializer) {
		assertThat(splitArray(serializer, "[1.10,3.14159265358979323846,1e5]")).containsExactly("1.10",
				"3.14159265358979323846", "1e5");
	}

	/**
	 * Jackson only reports the byte offsets the split methods slice by for UTF-8 input; a payload in another encoding
	 * gets a parser without them, which has to fail loudly rather than hand out bogus slices.
	 */
	@ParameterizedTest
	@MethodSource("slicingSerializers")
	void slicingFailsForPayloadsWithoutByteOffsets(RedisJsonSerializer serializer) {

		byte[] source = "[1,2,3]".getBytes(StandardCharsets.UTF_16);

		assertThatExceptionOfType(SerializationException.class).isThrownBy(() -> serializer.splitArray(source))
				.withMessageContaining("byte offsets");
	}

	/**
	 * The split methods derive their parser from the caller's own factory and mapper rather than a fresh, independent
	 * one, so what the caller configured still applies while splitting:
	 * <ul>
	 * <li>Slicing needs the byte-backed parser, which Jackson only picks for {@code byte[]} input when
	 * {@code CANONICALIZE_PROPERTY_NAMES} is on, so the factory is rebuilt with that one feature forced on - disabling
	 * it must not affect splitting.</li>
	 * <li>A {@link StreamReadConstraints} the caller set for their own reasons (e.g. capping nesting depth against
	 * oversized/malicious payloads) is not silently discarded.</li>
	 * <li>In Jackson 3, read features are held by the mapper's configuration rather than by its {@link JsonFactory}, so
	 * they have to be picked up from the mapper - otherwise a payload the caller's mapper happily deserializes cannot
	 * be split.</li>
	 * </ul>
	 */
	@Test
	void splittingHonorsTheCallerFactoryAndMapperJackson3() {

		JsonFactory factory = JsonFactory.builder().disable(TokenStreamFactory.Feature.CANONICALIZE_PROPERTY_NAMES)
				.streamReadConstraints(StreamReadConstraints.builder().maxNestingDepth(2).build()).build();
		RedisJsonSerializer serializer = GenericJacksonJsonRedisSerializer
				.builder(() -> JsonMapper.builder(factory).enable(JsonReadFeature.ALLOW_SINGLE_QUOTES)).build();

		assertThat(splitArray(serializer, "[1,2,3]")).containsExactly("1", "2", "3");
		assertThat(splitObject(serializer, "{'a':1}")).containsExactly(entry("a", "1"));
		assertThat(splitArray(serializer, "['a']")).containsExactly("'a'");
		assertThat(splitArray(serializer, "[[1]]")).containsExactly("[1]");
		assertThatExceptionOfType(SerializationException.class)
				.isThrownBy(() -> serializer.splitArray("[[[1]]]".getBytes(StandardCharsets.UTF_8)))
				.withMessageContaining("nesting depth");
	}

	/**
	 * Same guarantees as {@link #splittingHonorsTheCallerFactoryAndMapperJackson3()}, for the Jackson 2 based
	 * {@link GenericJackson2JsonRedisSerializer}: its factory is reachable through the public
	 * {@code GenericJackson2JsonRedisSerializerBuilder#objectMapper(ObjectMapper)} customization point, its
	 * canonicalization feature is named {@code CANONICALIZE_FIELD_NAMES}, and read features live on the factory the
	 * mapper configures rather than on the mapper itself.
	 */
	@Test
	void splittingHonorsTheCallerFactoryAndMapperJackson2() {

		com.fasterxml.jackson.core.JsonFactory factory = com.fasterxml.jackson.core.JsonFactory.builder()
				.disable(com.fasterxml.jackson.core.JsonFactory.Feature.CANONICALIZE_FIELD_NAMES)
				.streamReadConstraints(
						com.fasterxml.jackson.core.StreamReadConstraints.builder().maxNestingDepth(2).build())
				.build();
		ObjectMapper objectMapper = new ObjectMapper(factory)
				.configure(com.fasterxml.jackson.core.JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
		RedisJsonSerializer serializer = GenericJackson2JsonRedisSerializer.builder().objectMapper(objectMapper).build();

		assertThat(splitArray(serializer, "[1,2,3]")).containsExactly("1", "2", "3");
		assertThat(splitObject(serializer, "{'a':1}")).containsExactly(entry("a", "1"));
		assertThat(splitArray(serializer, "['a']")).containsExactly("'a'");
		assertThat(splitArray(serializer, "[[1]]")).containsExactly("[1]");
		assertThatExceptionOfType(SerializationException.class)
				.isThrownBy(() -> serializer.splitArray("[[[1]]]".getBytes(StandardCharsets.UTF_8)))
				.withMessageContaining("nesting depth");
	}

	/**
	 * The defaults re-serialize rather than slice, so unlike the overrides they do not preserve the original byte
	 * representation of numbers - as their javadoc states. Note this needs a literal RedisJSON does not emit: it
	 * normalizes floats on ingest, so {@code 1.10} comes back as {@code 1.1} anyway.
	 */
	@Test
	void defaultsDoNotPreserveNumberFormattingUnlikeTheSlicingOverrides() {

		RedisJsonSerializer slicing = GenericJacksonJsonRedisSerializer.builder().build();

		assertThat(splitArray(new PrimitivesOnlySerializer(slicing), "[1.10]")).containsExactly("1.1");
		assertThat(splitArray(slicing, "[1.10]")).containsExactly("1.10");
	}

	/**
	 * {@code JsonResult#asBytes} promises the bytes Redis sent, which for an element of {@code matches()} the defaults
	 * deliver by re-serializing rather than slicing. That is only equivalent while the mapper writes JSON the way
	 * RedisJSON does, so this pins it against a verbatim reply exercising the conventions that could diverge: raw UTF-8
	 * rather than {@code \\uXXXX} escapes, a retained {@code \\t}, an unescaped {@code /}, nested members in insertion
	 * rather than sorted order, and a large integer beyond double precision.
	 */
	@Test
	void defaultsReproduceARealRedisReplyByteForByte() {

		String reply = "[{\"a\":\"äx\",\"b\":\"tab\\there\",\"c\":\"café\",\"d\":{\"z\":1,\"a\":2},\"e\":\"sl/ash\","
				+ "\"f\":12345678901234567890,\"g\":3.141592653589793,\"h\":100000.0}]";
		RedisJsonSerializer slicing = GenericJacksonJsonRedisSerializer.builder().build();

		assertThat(splitArray(new PrimitivesOnlySerializer(slicing), reply)).isEqualTo(splitArray(slicing, reply));
	}

	/**
	 * Slicing needs a parser reporting byte offsets into the very {@code source} handed to it, which an
	 * {@code InputDecorator} rewriting the content breaks - the offsets then index the decorated stream. Both
	 * serializers detect that up front and fall back to the re-serializing defaults rather than failing, so the
	 * decorated mapper keeps working, only without byte-exact slices. Only the number formatting the defaults lose
	 * proves the fallback is what answered; their behavior itself is covered by the {@code defaults} argument set
	 * above.
	 */
	@Nested
	class FallbackWhenOffsetsAreUnavailable {

		private final RedisJsonSerializer jackson3 = GenericJacksonJsonRedisSerializer
				.builder(() -> JsonMapper.builder(JsonFactory.builder().inputDecorator(new Jackson3Prefixer()).build()))
				.build();

		private final RedisJsonSerializer jackson2 = GenericJackson2JsonRedisSerializer.builder()
				.objectMapper(new ObjectMapper(com.fasterxml.jackson.core.JsonFactory.builder()
						.inputDecorator(new Jackson2Prefixer()).build()))
				.build();

		@Test
		void answersWithTheReSerializingDefaults() {

			assertThat(splitArray(jackson3, "[1.10]")).containsExactly("1.1");
			assertThat(splitArray(jackson2, "[1.10]")).containsExactly("1.1");

			assertThat(splitObject(jackson3, "{\"$.a\":[1]}")).containsExactly(entry("$.a", "[1]"));
			assertThat(splitObject(jackson2, "{\"$.a\":[1]}")).containsExactly(entry("$.a", "[1]"));
		}

	}

	/**
	 * Rewrites the content by prepending a space, so that a parser's byte offsets no longer index the {@code source}
	 * given to it. Decorating the {@code byte[]} input is enough - that is the only overload the split code path uses.
	 */
	private static class Jackson3Prefixer extends tools.jackson.core.io.InputDecorator {

		@Override
		public InputStream decorate(tools.jackson.core.io.IOContext ctxt, InputStream in) {
			return in;
		}

		@Override
		public InputStream decorate(tools.jackson.core.io.IOContext ctxt, byte[] src, int offset, int length) {
			return new ByteArrayInputStream(prefixWithSpace(src, offset, length));
		}

		@Override
		public Reader decorate(tools.jackson.core.io.IOContext ctxt, Reader r) {
			return r;
		}

	}

	/**
	 * Jackson 2 counterpart of {@link Jackson3Prefixer}.
	 */
	private static class Jackson2Prefixer extends com.fasterxml.jackson.core.io.InputDecorator {

		@Override
		public InputStream decorate(com.fasterxml.jackson.core.io.IOContext ctxt, InputStream in) {
			return in;
		}

		@Override
		public InputStream decorate(com.fasterxml.jackson.core.io.IOContext ctxt, byte[] src, int offset, int length) {
			return new ByteArrayInputStream(prefixWithSpace(src, offset, length));
		}

		@Override
		public Reader decorate(com.fasterxml.jackson.core.io.IOContext ctxt, Reader r) {
			return r;
		}

	}

	private static byte[] prefixWithSpace(byte[] src, int offset, int length) {

		byte[] prefixed = new byte[length + 1];
		prefixed[0] = ' ';
		System.arraycopy(src, offset, prefixed, 1, length);

		return prefixed;
	}

	/**
	 * A {@link RedisJsonSerializer} implementing only the primitives every implementation has to provide, so the
	 * {@code splitArray}/{@code splitObject}/{@code joinObject} defaults are the ones under test. Delegates the
	 * primitives rather than hand-rolling JSON.
	 */
	private record PrimitivesOnlySerializer(RedisJsonSerializer delegate) implements RedisJsonSerializer {

		@Override
		public byte[] serialize(@Nullable Object value) throws SerializationException {
			return delegate.serialize(value);
		}

		@Override
		public @Nullable Object deserialize(byte @Nullable [] bytes) throws SerializationException {
			return delegate.deserialize(bytes);
		}

		@Override
		public @Nullable Object deserialize(byte[] source, ResolvableType type) throws SerializationException {
			return delegate.deserialize(source, type);
		}

	}
}
