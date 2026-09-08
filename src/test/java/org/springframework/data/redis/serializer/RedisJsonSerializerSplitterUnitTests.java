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

import tools.jackson.core.StreamReadConstraints;
import tools.jackson.core.TokenStreamFactory;
import tools.jackson.core.json.JsonFactory;
import tools.jackson.core.json.JsonReadFeature;
import tools.jackson.databind.json.JsonMapper;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.data.redis.serializer.RedisJsonSerializer.Splitter;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Unit tests for {@link Splitter}.
 *
 * @author Moritz Halbritter
 * @author Mark Paluch
 */
class RedisJsonSerializerSplitterUnitTests {

	@ParameterizedTest // GH-3433
	@MethodSource("splitters")
	void splitEmptyArray(Splitter splitter) {

		List<byte[]> result = splitter.splitArray(bytes("[]"));

		assertThat(result).isEmpty();
	}

	@ParameterizedTest // GH-3433
	@MethodSource("splitters")
	void splitArrayWithString(Splitter splitter) {

		List<byte[]> result = splitter.splitArray(bytes("[\"John\"]"));

		assertThat(result).extracting(RedisJsonSerializerSplitterUnitTests::text).containsExactly("\"John\"");
	}

	@ParameterizedTest // GH-3433
	@MethodSource("splitters")
	void splitArrayWithNumbers(Splitter splitter) {

		List<byte[]> result = splitter.splitArray(bytes("[1,2,3]"));

		assertThat(result).extracting(RedisJsonSerializerSplitterUnitTests::text).containsExactly("1", "2", "3");
	}

	@ParameterizedTest // GH-3433
	@MethodSource("splitters")
	void splitArrayWithWhitespace(Splitter splitter) {

		List<byte[]> result = splitter.splitArray(bytes("[ 1 , 2 , 3 ]"));

		assertThat(result).extracting(RedisJsonSerializerSplitterUnitTests::text).containsExactly("1", "2", "3");
	}

	@ParameterizedTest // GH-3433
	@MethodSource("splitters")
	void splitArrayWithNull(Splitter splitter) {

		List<byte[]> result = splitter.splitArray(bytes("[null]"));

		assertThat(result).extracting(RedisJsonSerializerSplitterUnitTests::text).containsExactly("null");
	}

	@ParameterizedTest // GH-3433
	@MethodSource("splitters")
	void splitArrayWithNestedArrays(Splitter splitter) {

		List<byte[]> result = splitter.splitArray(bytes("[[\"admin\",\"dev\"],[\"ops\"]]"));

		assertThat(result).extracting(RedisJsonSerializerSplitterUnitTests::text).containsExactly("[\"admin\",\"dev\"]",
				"[\"ops\"]");
	}

	@ParameterizedTest // GH-3433
	@MethodSource("splitters")
	void splitArrayWithCommaInString(Splitter splitter) {

		List<byte[]> result = splitter.splitArray(bytes("[\"a,b\",\"c\"]"));

		assertThat(result).extracting(RedisJsonSerializerSplitterUnitTests::text).containsExactly("\"a,b\"", "\"c\"");
	}

	@ParameterizedTest // GH-3433
	@MethodSource("splitters")
	void splitArrayWithEscapedQuote(Splitter splitter) {

		List<byte[]> result = splitter.splitArray(bytes("[\"a\\\"b\",\"c\"]"));

		assertThat(result).extracting(RedisJsonSerializerSplitterUnitTests::text).containsExactly("\"a\\\"b\"", "\"c\"");
	}

	@ParameterizedTest // GH-3433
	@MethodSource("splitters")
	void splitEmptyObject(Splitter splitter) {

		Map<String, byte[]> result = splitter.splitObject(bytes("{}"));

		assertThat(result).isEmpty();
	}

	@ParameterizedTest // GH-3433
	@MethodSource("splitters")
	void splitObjectPreservesMemberOrder(Splitter splitter) {

		Map<String, byte[]> result = splitter.splitObject(bytes("{\"$.b\":[2],\"$.a\":[1]}"));

		assertThat(textValues(result)).containsExactly(entry("$.b", "[2]"), entry("$.a", "[1]"));
	}

	@ParameterizedTest // GH-3433
	@MethodSource("splitters")
	void splitObjectWithEscapedMemberName(Splitter splitter) {

		Map<String, byte[]> result = splitter.splitObject(bytes("{\"$['a\\\"b']\":1}"));

		assertThat(textValues(result)).containsExactly(entry("$['a\"b']", "1"));
	}

	@ParameterizedTest // GH-3433
	@MethodSource("splitters")
	void splitObjectWithDuplicateMemberNames(Splitter splitter) {

		Map<String, byte[]> result = splitter.splitObject(bytes("{\"a\":1,\"a\":2}"));

		assertThat(textValues(result)).containsExactly(entry("a", "2"));
	}

	@ParameterizedTest // GH-3433
	@MethodSource("splitters")
	void joinEmptyObject(Splitter splitter) {

		byte[] result = splitter.joinObject(Map.of());

		assertThat(result).isEqualTo(bytes("{}"));
	}

	@ParameterizedTest // GH-3433
	@MethodSource("splitters")
	void joinObjectPreservesMemberOrder(Splitter splitter) {

		Map<String, byte[]> members = new LinkedHashMap<>();
		members.put("b", bytes("2"));
		members.put("a", bytes("1"));

		byte[] result = splitter.joinObject(members);

		assertThat(result).isEqualTo(bytes("{\"b\":2,\"a\":1}"));
	}

	@ParameterizedTest // GH-3433
	@MethodSource("splitters")
	void joinObjectWithNestedValue(Splitter splitter) {

		Map<String, byte[]> members = new LinkedHashMap<>();
		members.put("a", bytes("{\"nested\":[1,null,\"x\"]}"));

		byte[] result = splitter.joinObject(members);

		assertThat(result).isEqualTo(bytes("{\"a\":{\"nested\":[1,null,\"x\"]}}"));
	}

	@ParameterizedTest // GH-3433
	@MethodSource("splitters")
	void joinObjectWithNull(Splitter splitter) {

		Map<String, byte[]> members = new LinkedHashMap<>();
		members.put("n", bytes("null"));

		byte[] result = splitter.joinObject(members);

		assertThat(result).isEqualTo(bytes("{\"n\":null}"));
	}

	@ParameterizedTest // GH-3433
	@MethodSource("splitters")
	void joinObjectWithEscapedMemberName(Splitter splitter) {

		Map<String, byte[]> members = new LinkedHashMap<>();
		members.put("a\"b", bytes("1"));

		byte[] result = splitter.joinObject(members);

		assertThat(result).isEqualTo(bytes("{\"a\\\"b\":1}"));
	}

	@ParameterizedTest // GH-3433
	@MethodSource("splitters")
	void joinObjectWithEmptyMemberName(Splitter splitter) {

		Map<String, byte[]> members = new LinkedHashMap<>();
		members.put("", bytes("1"));

		byte[] result = splitter.joinObject(members);

		assertThat(result).isEqualTo(bytes("{\"\":1}"));
	}

	@ParameterizedTest // GH-3433
	@MethodSource("splitters")
	void splitAndJoinObjectWithJsonPathMembers(Splitter splitter) {

		Map<String, byte[]> members = new LinkedHashMap<>();
		members.put("$['a\"b']", bytes("[1]"));
		members.put("$..city", bytes("[\"Rand\"]"));
		members.put("plain", bytes("null"));

		byte[] json = splitter.joinObject(members);
		Map<String, byte[]> result = splitter.splitObject(json);

		assertThat(textValues(result)).containsExactlyEntriesOf(textValues(members));
	}

	@ParameterizedTest // GH-3433
	@MethodSource("splitters")
	void splitArrayWithObjectInput(Splitter splitter) {

		assertThatExceptionOfType(SerializationException.class).isThrownBy(() -> splitter.splitArray(bytes("{\"a\":1}")));
	}

	@ParameterizedTest // GH-3433
	@MethodSource("splitters")
	void splitObjectWithArrayInput(Splitter splitter) {

		assertThatExceptionOfType(SerializationException.class).isThrownBy(() -> splitter.splitObject(bytes("[1]")));
	}

	@ParameterizedTest // GH-3433
	@MethodSource("slicingSplitters")
	void splitArrayPreservesNumberRepresentation(Splitter splitter) {

		List<byte[]> result = splitter.splitArray(bytes("[1.10,3.14159265358979323846,1e5]"));

		assertThat(result).extracting(RedisJsonSerializerSplitterUnitTests::text).containsExactly("1.10",
				"3.14159265358979323846", "1e5");
	}

	@ParameterizedTest // GH-3433
	@MethodSource("slicingSplitters")
	void splitArrayWithUtf16Input(Splitter splitter) {

		byte[] source = "[1,2,3]".getBytes(StandardCharsets.UTF_16);

		assertThatExceptionOfType(SerializationException.class).isThrownBy(() -> splitter.splitArray(source))
				.withMessageContaining("byte offsets");
	}

	@ParameterizedTest // GH-3433
	@MethodSource("singleQuoteSplitters")
	void splitArrayWithSingleQuotes(Splitter splitter) {

		List<byte[]> result = splitter.splitArray(bytes("['a']"));

		assertThat(result).extracting(RedisJsonSerializerSplitterUnitTests::text).containsExactly("'a'");
	}

	@ParameterizedTest // GH-3433
	@MethodSource("singleQuoteSplitters")
	void splitObjectWithSingleQuotes(Splitter splitter) {

		Map<String, byte[]> result = splitter.splitObject(bytes("{'a':1}"));

		assertThat(textValues(result)).containsExactly(entry("a", "1"));
	}

	@ParameterizedTest // GH-3433
	@MethodSource("depthLimitedSplitters")
	void splitArrayAtMaxNestingDepth(Splitter splitter) {

		List<byte[]> result = splitter.splitArray(bytes("[[1]]"));

		assertThat(result).extracting(RedisJsonSerializerSplitterUnitTests::text).containsExactly("[1]");
	}

	@ParameterizedTest // GH-3433
	@MethodSource("nonCanonicalizingSplitters")
	void splitArrayWithoutPropertyNameCanonicalization(Splitter splitter) {

		List<byte[]> result = splitter.splitArray(bytes("[1,2,3]"));

		assertThat(result).extracting(RedisJsonSerializerSplitterUnitTests::text).containsExactly("1", "2", "3");
	}

	@ParameterizedTest // GH-3433
	@MethodSource("depthLimitedSplitters")
	void splitArrayExceedingMaxNestingDepth(Splitter splitter) {

		assertThatExceptionOfType(SerializationException.class).isThrownBy(() -> splitter.splitArray(bytes("[[[1]]]")))
				.withMessageContaining("nesting depth");
	}

	@Test // GH-3433
	void defaultSplitterReserializesNumbers() {

		Splitter splitter = new DefaultSplitter(GenericJacksonJsonRedisSerializer.builder().build());

		List<byte[]> result = splitter.splitArray(bytes("[1.10]"));

		assertThat(result).extracting(RedisJsonSerializerSplitterUnitTests::text).containsExactly("1.1");
	}

	@ParameterizedTest // GH-3433
	@MethodSource("splitters")
	void splitArrayPreservesObjectRepresentation(Splitter splitter) {

		String value = "{\"a\":\"äx\",\"b\":\"tab\\there\",\"c\":\"café\",\"d\":{\"z\":1,\"a\":2},\"e\":\"sl/ash\",\"f\":12345678901234567890,\"g\":3.141592653589793,\"h\":100000.0}";

		List<byte[]> result = splitter.splitArray(bytes("[" + value + "]"));

		assertThat(result).singleElement().isEqualTo(bytes(value));
	}

	@ParameterizedTest // GH-3433
	@MethodSource("decoratedSplitters")
	void splitArrayWithDecoratedInput(Splitter splitter) {

		List<byte[]> result = splitter.splitArray(bytes("[1.10]"));

		assertThat(result).extracting(RedisJsonSerializerSplitterUnitTests::text).containsExactly("1.1");
	}

	@ParameterizedTest // GH-3433
	@MethodSource("decoratedSplitters")
	void splitObjectWithDecoratedInput(Splitter splitter) {

		Map<String, byte[]> result = splitter.splitObject(bytes("{\"$.a\":[1]}"));

		assertThat(textValues(result)).containsExactly(entry("$.a", "[1]"));
	}

	static Stream<Arguments> splitters() {
		return Stream.concat(slicingSplitters(),
				Stream.of(argumentSet("defaults", new DefaultSplitter(GenericJacksonJsonRedisSerializer.builder().build()))));
	}

	static Stream<Arguments> slicingSplitters() {
		return Stream.of(argumentSet("Jackson 2", new GenericJackson2JsonRedisSerializer().splitter()),
				argumentSet("Jackson 3", GenericJacksonJsonRedisSerializer.builder().build().splitter()));
	}

	static Stream<Arguments> singleQuoteSplitters() {
		ObjectMapper jackson2 = new ObjectMapper()
				.configure(com.fasterxml.jackson.core.JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
		return Stream.of(
				argumentSet("Jackson 2",
						GenericJackson2JsonRedisSerializer.builder().objectMapper(jackson2).build().splitter()),
				argumentSet("Jackson 3", GenericJacksonJsonRedisSerializer
						.builder(() -> JsonMapper.builder().enable(JsonReadFeature.ALLOW_SINGLE_QUOTES)).build().splitter()));
	}

	static Stream<Arguments> depthLimitedSplitters() {
		ObjectMapper jackson2 = new ObjectMapper(com.fasterxml.jackson.core.JsonFactory.builder()
				.streamReadConstraints(com.fasterxml.jackson.core.StreamReadConstraints.builder().maxNestingDepth(2).build())
				.build());
		JsonFactory jackson3 = JsonFactory.builder()
				.streamReadConstraints(StreamReadConstraints.builder().maxNestingDepth(2).build()).build();
		return Stream.of(
				argumentSet("Jackson 2",
						GenericJackson2JsonRedisSerializer.builder().objectMapper(jackson2).build().splitter()),
				argumentSet("Jackson 3",
						GenericJacksonJsonRedisSerializer.builder(() -> JsonMapper.builder(jackson3)).build().splitter()));
	}

	static Stream<Arguments> nonCanonicalizingSplitters() {
		ObjectMapper jackson2 = new ObjectMapper(com.fasterxml.jackson.core.JsonFactory.builder()
				.disable(com.fasterxml.jackson.core.JsonFactory.Feature.CANONICALIZE_FIELD_NAMES).build());
		JsonFactory jackson3 = JsonFactory.builder().disable(TokenStreamFactory.Feature.CANONICALIZE_PROPERTY_NAMES)
				.build();
		return Stream.of(
				argumentSet("Jackson 2",
						GenericJackson2JsonRedisSerializer.builder().objectMapper(jackson2).build().splitter()),
				argumentSet("Jackson 3",
						GenericJacksonJsonRedisSerializer.builder(() -> JsonMapper.builder(jackson3)).build().splitter()));
	}

	static Stream<Arguments> decoratedSplitters() {
		ObjectMapper jackson2 = new ObjectMapper(
				com.fasterxml.jackson.core.JsonFactory.builder().inputDecorator(new Jackson2Prefixer()).build());
		JsonFactory jackson3 = JsonFactory.builder().inputDecorator(new Jackson3Prefixer()).build();
		return Stream.of(
				argumentSet("Jackson 2",
						GenericJackson2JsonRedisSerializer.builder().objectMapper(jackson2).build().splitter()),
				argumentSet("Jackson 3",
						GenericJacksonJsonRedisSerializer.builder(() -> JsonMapper.builder(jackson3)).build().splitter()));
	}

	private static byte[] bytes(String value) {
		return value.getBytes(StandardCharsets.UTF_8);
	}

	private static String text(byte[] value) {
		return new String(value, StandardCharsets.UTF_8);
	}

	private static Map<String, String> textValues(Map<String, byte[]> values) {
		Map<String, String> result = new LinkedHashMap<>();
		values.forEach((key, value) -> result.put(key, text(value)));
		return result;
	}

	private static byte[] prefixWithSpace(byte[] source, int offset, int length) {
		byte[] prefixed = new byte[length + 1];
		prefixed[0] = ' ';
		System.arraycopy(source, offset, prefixed, 1, length);
		return prefixed;
	}

	// A decorated stream prevents the parser from reporting byte offsets into the
	// original input.
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

}
