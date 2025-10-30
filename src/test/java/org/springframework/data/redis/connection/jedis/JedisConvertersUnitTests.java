/*
 * Copyright 2014-2025 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import redis.clients.jedis.Protocol;
import redis.clients.jedis.params.GetExParams;
import redis.clients.jedis.params.HGetExParams;
import redis.clients.jedis.params.HSetExParams;
import redis.clients.jedis.params.SetParams;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.RedisHashCommands;
import org.springframework.data.redis.connection.RedisServer;
import org.springframework.data.redis.connection.RedisStringCommands.SetOption;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.core.types.RedisClientInfo;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * Unit tests for {@link JedisConverters}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author John Blum
 */
class JedisConvertersUnitTests {

	private static final String CLIENT_ALL_SINGLE_LINE_RESPONSE = "addr=127.0.0.1:60311 fd=6 name= age=4059 idle=0 flags=N db=0 sub=0 psub=0 multi=-1 qbuf=0 qbuf-free=32768 obl=0 oll=0 omem=0 events=r cmd=client";

	@Test // DATAREDIS-268
	void convertingEmptyStringToListOfRedisClientInfoShouldReturnEmptyList() {
		assertThat(JedisConverters.toListOfRedisClientInformation("")).isEqualTo(Collections.<RedisClientInfo> emptyList());
	}

	@Test // DATAREDIS-268
	void convertingNullToListOfRedisClientInfoShouldReturnEmptyList() {
		assertThat(JedisConverters.toListOfRedisClientInformation(null))
				.isEqualTo(Collections.<RedisClientInfo> emptyList());
	}

	@Test // DATAREDIS-268
	void convertingMultipleLiesToListOfRedisClientInfoReturnsListCorrectly() {

		StringBuilder sb = new StringBuilder();
		sb.append(CLIENT_ALL_SINGLE_LINE_RESPONSE);
		sb.append("\r\n");
		sb.append(CLIENT_ALL_SINGLE_LINE_RESPONSE);

		assertThat(JedisConverters.toListOfRedisClientInformation(sb.toString()).size()).isEqualTo(2);
	}

	@Test // DATAREDIS-330
	void convertsSingleMapToRedisServerReturnsCollectionCorrectly() {

		Map<String, String> values = getRedisServerInfoMap("mymaster", 23697);
		List<RedisServer> servers = JedisConverters.toListOfRedisServer(Collections.singletonList(values));

		assertThat(servers.size()).isEqualTo(1);
		verifyRedisServerInfo(servers.get(0), values);
	}

	@Test // DATAREDIS-330
	void convertsMultipleMapsToRedisServerReturnsCollectionCorrectly() {

		List<Map<String, String>> vals = Arrays.asList(getRedisServerInfoMap("mymaster", 23697),
				getRedisServerInfoMap("yourmaster", 23680));
		List<RedisServer> servers = JedisConverters.toListOfRedisServer(vals);

		assertThat(servers.size()).isEqualTo(vals.size());
		for (int i = 0; i < vals.size(); i++) {
			verifyRedisServerInfo(servers.get(i), vals.get(i));
		}
	}

	@Test // DATAREDIS-330
	void convertsRedisServersCorrectlyWhenGivenAnEmptyList() {
		assertThat(JedisConverters.toListOfRedisServer(Collections.<Map<String, String>> emptyList())).isNotNull();
	}

	@Test // DATAREDIS-330
	void convertsRedisServersCorrectlyWhenGivenNull() {
		assertThat(JedisConverters.toListOfRedisServer(null)).isNotNull();
	}

	/**
	 */
	@Test
	void boundaryToBytesForZRangeByLexShouldReturnDefaultValueWhenBoundaryIsNull() {

		byte[] defaultValue = "tyrion".getBytes();

		assertThat(JedisConverters.boundaryToBytesForZRangeByLex(null, defaultValue)).isEqualTo(defaultValue);
	}

	@Test // DATAREDIS-378
	void boundaryToBytesForZRangeByLexShouldReturnDefaultValueWhenBoundaryValueIsNull() {

		byte[] defaultValue = "tyrion".getBytes();

		assertThat(JedisConverters.boundaryToBytesForZRangeByLex(Range.Bound.unbounded(), defaultValue))
				.isEqualTo(defaultValue);
	}

	@Test // DATAREDIS-378
	void boundaryToBytesForZRangeByLexShouldReturnValueCorrectlyWhenBoundaryIsIncluing() {

		assertThat(JedisConverters.boundaryToBytesForZRangeByLex(Range.Bound.inclusive(JedisConverters.toBytes("a")), null))
				.isEqualTo(JedisConverters.toBytes("[a"));
	}

	@Test // DATAREDIS-378
	void boundaryToBytesForZRangeByLexShouldReturnValueCorrectlyWhenBoundaryIsExcluding() {

		assertThat(JedisConverters.boundaryToBytesForZRangeByLex(Range.Bound.exclusive(JedisConverters.toBytes("a")), null))
				.isEqualTo(JedisConverters.toBytes("(a"));
	}

	@Test // DATAREDIS-378
	void boundaryToBytesForZRangeByLexShouldReturnValueCorrectlyWhenBoundaryIsAString() {

		assertThat(JedisConverters.boundaryToBytesForZRangeByLex(Range.Bound.exclusive(JedisConverters.toBytes("a")), null))
				.isEqualTo(JedisConverters.toBytes("(a"));
	}

	@Test // DATAREDIS-378
	void boundaryToBytesForZRangeByLexShouldReturnValueCorrectlyWhenBoundaryIsANumber() {

		assertThat(JedisConverters.boundaryToBytesForZRangeByLex(Range.Bound.exclusive(JedisConverters.toBytes("1")), null))
				.isEqualTo(JedisConverters.toBytes("(1"));
	}

	@Test // DATAREDIS-352
	void boundaryToBytesForZRangeByShouldReturnDefaultValueWhenBoundaryIsNull() {

		byte[] defaultValue = "tyrion".getBytes();

		assertThat(JedisConverters.boundaryToBytesForZRange(null, defaultValue)).isEqualTo(defaultValue);
	}

	@Test // DATAREDIS-352
	void boundaryToBytesForZRangeByShouldReturnDefaultValueWhenBoundaryValueIsNull() {

		byte[] defaultValue = "tyrion".getBytes();

		assertThat(JedisConverters.boundaryToBytesForZRange(Range.Bound.unbounded(), defaultValue)).isEqualTo(defaultValue);
	}

	@Test // DATAREDIS-352
	void boundaryToBytesForZRangeByShouldReturnValueCorrectlyWhenBoundaryIsIncluing() {

		assertThat(JedisConverters.boundaryToBytesForZRange(Range.Bound.inclusive(JedisConverters.toBytes("a")), null))
				.isEqualTo(JedisConverters.toBytes("a"));
	}

	@Test // DATAREDIS-352
	void boundaryToBytesForZRangeByShouldReturnValueCorrectlyWhenBoundaryIsExcluding() {

		assertThat(JedisConverters.boundaryToBytesForZRange(Range.Bound.exclusive(JedisConverters.toBytes("a")), null))
				.isEqualTo(JedisConverters.toBytes("(a"));
	}

	@Test // DATAREDIS-352
	void boundaryToBytesForZRangeByShouldReturnValueCorrectlyWhenBoundaryIsAString() {

		assertThat(JedisConverters.boundaryToBytesForZRange(Range.Bound.exclusive("a"), null))
				.isEqualTo(JedisConverters.toBytes("(a"));
	}

	@Test // DATAREDIS-352
	void boundaryToBytesForZRangeByShouldReturnValueCorrectlyWhenBoundaryIsANumber() {

		assertThat(
				JedisConverters.boundaryToBytesForZRange(org.springframework.data.domain.Range.Bound.exclusive(1L), null))
				.isEqualTo(JedisConverters.toBytes("(1"));
	}

	@Test // DATAREDIS-352
	void boundaryToBytesForZRangeByShouldThrowExceptionWhenBoundaryHoldsUnknownType() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> JedisConverters.boundaryToBytesForZRange(Range.Bound.exclusive(new Date()), null));
	}

	@Test // DATAREDIS-316, DATAREDIS-749
	void toSetCommandExPxOptionShouldReturnEXforSeconds() {
		assertThat(toString(JedisConverters.toSetCommandExPxArgument(Expiration.seconds(100)))).isEqualTo("ex 100");
	}

	@Test // DATAREDIS-316, DATAREDIS-749
	void toSetCommandExPxOptionShouldReturnEXforMilliseconds() {
		assertThat(toString(JedisConverters.toSetCommandExPxArgument(Expiration.milliseconds(100)))).isEqualTo("px 100");
	}

	@Test // GH-2050
	void convertsExpirationToSetPXAT() {

		SetParams mockSetParams = mock(SetParams.class);

		doReturn(mockSetParams).when(mockSetParams).pxAt(anyLong());

		Expiration expiration = Expiration.unixTimestamp(10, TimeUnit.MILLISECONDS);

		assertThat(JedisConverters.toSetCommandExPxArgument(expiration, mockSetParams)).isNotNull();

		verify(mockSetParams, times(1)).pxAt(eq(10L));
		verifyNoMoreInteractions(mockSetParams);
	}

	@Test // GH-2050
	void convertsExpirationToSetEXAT() {

		SetParams mockSetParams = mock(SetParams.class);

		doReturn(mockSetParams).when(mockSetParams).exAt(anyLong());

		Expiration expiration = Expiration.unixTimestamp(1, TimeUnit.MINUTES);

		assertThat(JedisConverters.toSetCommandExPxArgument(expiration, mockSetParams)).isNotNull();

		verify(mockSetParams, times(1)).exAt(eq(60L));
		verifyNoMoreInteractions(mockSetParams);
	}

	@Test // DATAREDIS-316, DATAREDIS-749
	void toSetCommandNxXxOptionShouldReturnNXforAbsent() {
		assertThat(toString(JedisConverters.toSetCommandNxXxArgument(SetOption.ifAbsent()))).isEqualTo("nx");
	}

	@Test // DATAREDIS-316, DATAREDIS-749
	void toSetCommandNxXxOptionShouldReturnXXforAbsent() {
		assertThat(toString(JedisConverters.toSetCommandNxXxArgument(SetOption.ifPresent()))).isEqualTo("xx");
	}

	@Test // DATAREDIS-316, DATAREDIS-749
	void toSetCommandNxXxOptionShouldReturnEmptyArrayforUpsert() {
		assertThat(toString(JedisConverters.toSetCommandNxXxArgument(SetOption.upsert()))).isEqualTo("");
	}

	@Test // GH-2050
	void convertsExpirationToGetExEX() {

		GetExParams mockGetExParams = mock(GetExParams.class);

		doReturn(mockGetExParams).when(mockGetExParams).ex(anyLong());

		Expiration expiration = Expiration.seconds(10);

		assertThat(JedisConverters.toGetExParams(expiration, mockGetExParams)).isNotNull();

		verify(mockGetExParams, times(1)).ex(eq(10L));
		verifyNoMoreInteractions(mockGetExParams);
	}

	@Test // GH-2050
	void convertsExpirationWithTimeUnitToGetExEX() {

		GetExParams mockGetExParams = mock(GetExParams.class);

		doReturn(mockGetExParams).when(mockGetExParams).ex(anyLong());

		Expiration expiration = Expiration.from(1, TimeUnit.MINUTES);

		assertThat(JedisConverters.toGetExParams(expiration, mockGetExParams)).isNotNull();

		verify(mockGetExParams, times(1)).ex(eq(60L)); // seconds
		verifyNoMoreInteractions(mockGetExParams);
	}

	@Test // GH-2050
	void convertsExpirationToGetExPEX() {

		GetExParams mockGetExParams = mock(GetExParams.class);

		doReturn(mockGetExParams).when(mockGetExParams).px(anyLong());

		Expiration expiration = Expiration.milliseconds(10L);

		assertThat(JedisConverters.toGetExParams(expiration, mockGetExParams)).isNotNull();

		verify(mockGetExParams, times(1)).px(eq(10L));
		verifyNoMoreInteractions(mockGetExParams);
	}

	@Test // GH-2050
	void convertsExpirationToGetExEXAT() {

		GetExParams mockGetExParams = mock(GetExParams.class);

		doReturn(mockGetExParams).when(mockGetExParams).exAt(anyLong());

		Expiration expiration = Expiration.unixTimestamp(10, TimeUnit.SECONDS);

		assertThat(JedisConverters.toGetExParams(expiration, mockGetExParams)).isNotNull();

		verify(mockGetExParams, times(1)).exAt(eq(10L));
		verifyNoMoreInteractions(mockGetExParams);
	}

	@Test // GH-2050
	void convertsExpirationWithTimeUnitToGetExEXAT() {

		GetExParams mockGetExParams = mock(GetExParams.class);

		doReturn(mockGetExParams).when(mockGetExParams).exAt(anyLong());

		Expiration expiration = Expiration.unixTimestamp(1, TimeUnit.MINUTES);

		assertThat(JedisConverters.toGetExParams(expiration, mockGetExParams)).isNotNull();

		verify(mockGetExParams, times(1)).exAt(eq(60L));
		verifyNoMoreInteractions(mockGetExParams);
	}

	@Test // GH-2050
	void convertsExpirationToGetExPXAT() {

		GetExParams mockGetExParams = mock(GetExParams.class);

		doReturn(mockGetExParams).when(mockGetExParams).pxAt(anyLong());

		Expiration expiration = Expiration.unixTimestamp(10, TimeUnit.MILLISECONDS);

		assertThat(JedisConverters.toGetExParams(expiration, mockGetExParams)).isNotNull();

		verify(mockGetExParams, times(1)).pxAt(eq(10L));
		verifyNoMoreInteractions(mockGetExParams);
	}

	private void verifyRedisServerInfo(RedisServer server, Map<String, String> values) {

		for (Map.Entry<String, String> entry : values.entrySet()) {
			assertThat(server.get(entry.getKey())).isEqualTo(entry.getValue());
		}
	}

	private static String toString(SetParams setParams) {

		StringBuilder stringBuilder = new StringBuilder();

		stringBuilder.append(toString((Protocol.Keyword) ReflectionTestUtils.getField(setParams, "existance")));
		stringBuilder.append(toString((Protocol.Keyword) ReflectionTestUtils.getField(setParams, "expiration")));

		Long expirationValue = (Long) ReflectionTestUtils.getField(setParams, "expirationValue");

		if (expirationValue != null) {
			stringBuilder.append(" ").append(expirationValue);
		}

		return stringBuilder.toString().trim();
	}

	private static String toString(@Nullable Enum<?> value) {
		return value != null ? value.name().toLowerCase() : "";
	}

	private Map<String, String> getRedisServerInfoMap(String name, int port) {
		Map<String, String> map = new HashMap<>();
		map.put("name", name);
		map.put("ip", "127.0.0.1");
		map.put("port", Integer.toString(port));
		map.put("runid", "768c2926e5148d208713bf17cd5821e10f5388e2");
		map.put("flags", "master");
		map.put("pending-commands", "0");
		map.put("last-ping-sent", "0");
		map.put("last-ok-ping-reply", "534");
		map.put("last-ping-reply", "534");
		map.put("down-after-milliseconds", "30000");
		map.put("info-refresh", "147");
		map.put("role-reported", "master");
		map.put("role-reported-time", "41248339");
		map.put("config-epoch", "7");
		map.put("num-slaves", "2");
		map.put("num-other-sentinels", "2");
		map.put("quorum", "2");
		map.put("failover-timeout", "180000");
		map.put("parallel-syncs", "1");
		return map;
	}

	@Nested
	class ToHGetExParamsShould {

		@Test
		void notSetAnyFieldsForNullExpiration() {

			assertThatParamsHasExpiration(JedisConverters.toHGetExParams(null), null, null);
		}

		@Test
		void setPersistForNonExpiringExpiration() {

			assertThatParamsHasExpiration(JedisConverters.toHGetExParams(Expiration.persistent()), Protocol.Keyword.PERSIST,
					null);
		}

		@Test
		void setPxForExpirationWithMillisTimeUnit() {

			HGetExParams params = JedisConverters.toHGetExParams(Expiration.from(30_000, TimeUnit.MILLISECONDS));
			assertThatParamsHasExpiration(params, Protocol.Keyword.PX, 30_000L);
		}

		@Test
		void setPxAtForExpirationWithMillisUnixTimestamp() {

			long fourHoursFromNowMillis = Instant.now().plus(4L, ChronoUnit.HOURS).toEpochMilli();
			HGetExParams params = JedisConverters
					.toHGetExParams(Expiration.unixTimestamp(fourHoursFromNowMillis, TimeUnit.MILLISECONDS));
			assertThatParamsHasExpiration(params, Protocol.Keyword.PXAT, fourHoursFromNowMillis);
		}

		@Test
		void setExForExpirationWithNonMillisTimeUnit() {

			HGetExParams params = JedisConverters.toHGetExParams(Expiration.from(30, TimeUnit.SECONDS));
			assertThatParamsHasExpiration(params, Protocol.Keyword.EX, 30L);
		}

		@Test
		void setExAtForExpirationWithNonMillisUnixTimestamp() {

			long fourHoursFromNowSecs = Instant.now().plus(4L, ChronoUnit.HOURS).getEpochSecond();
			HGetExParams params = JedisConverters
					.toHGetExParams(Expiration.unixTimestamp(fourHoursFromNowSecs, TimeUnit.SECONDS));
			assertThatParamsHasExpiration(params, Protocol.Keyword.EXAT, fourHoursFromNowSecs);
		}

		private void assertThatParamsHasExpiration(HGetExParams params, Protocol.Keyword expirationType,
				Long expirationValue) {
			assertThat(params).extracting("expiration", "expirationValue").containsExactly(expirationType, expirationValue);
		}
	}

	@Nested
	class ToHSetExParamsShould {

		@Test
		void setFnxForNoneExistCondition() {

			HSetExParams params = JedisConverters.toHSetExParams(RedisHashCommands.HashFieldSetOption.IF_NONE_EXIST, null);
			assertThatParamsHasExistance(params, Protocol.Keyword.FNX);
		}

		@Test
		void setFxxForAllExistCondition() {

			HSetExParams params = JedisConverters.toHSetExParams(RedisHashCommands.HashFieldSetOption.IF_ALL_EXIST, null);
			assertThatParamsHasExistance(params, Protocol.Keyword.FXX);
		}

		@Test
		void notSetFnxNorFxxForUpsertCondition() {

			HSetExParams params = JedisConverters.toHSetExParams(RedisHashCommands.HashFieldSetOption.UPSERT, null);
			assertThatParamsHasExistance(params, null);
		}

		@Test
		void notSetAnyTimeFieldsForNullExpiration() {

			HSetExParams params = JedisConverters.toHSetExParams(RedisHashCommands.HashFieldSetOption.UPSERT, null);
			assertThatParamsHasExpiration(params, null, null);
		}

		@Test
		void notSetAnyTimeFieldsForNonExpiringExpiration() {

			HSetExParams params = JedisConverters.toHSetExParams(RedisHashCommands.HashFieldSetOption.UPSERT,
					Expiration.persistent());
			assertThatParamsHasExpiration(params, null, null);
		}

		@Test
		void setKeepTtlForKeepTtlExpiration() {

			HSetExParams params = JedisConverters.toHSetExParams(RedisHashCommands.HashFieldSetOption.UPSERT,
					Expiration.keepTtl());
			assertThatParamsHasExpiration(params, Protocol.Keyword.KEEPTTL, null);
		}

		@Test
		void setPxForExpirationWithMillisTimeUnit() {

			Expiration expiration = Expiration.from(30_000, TimeUnit.MILLISECONDS);
			assertThatParamsHasExpiration(
					JedisConverters.toHSetExParams(RedisHashCommands.HashFieldSetOption.UPSERT, expiration), Protocol.Keyword.PX,
					30_000L);
		}

		@Test
		void setPxAtForExpirationWithMillisUnixTimestamp() {

			long fourHoursFromNowMillis = Instant.now().plus(4L, ChronoUnit.HOURS).toEpochMilli();
			Expiration expiration = Expiration.unixTimestamp(fourHoursFromNowMillis, TimeUnit.MILLISECONDS);
			assertThatParamsHasExpiration(
					JedisConverters.toHSetExParams(RedisHashCommands.HashFieldSetOption.UPSERT, expiration),
					Protocol.Keyword.PXAT, fourHoursFromNowMillis);
		}

		@Test
		void setExForExpirationWithNonMillisTimeUnit() {

			Expiration expiration = Expiration.from(30, TimeUnit.SECONDS);
			assertThatParamsHasExpiration(
					JedisConverters.toHSetExParams(RedisHashCommands.HashFieldSetOption.UPSERT, expiration), Protocol.Keyword.EX,
					30L);
		}

		@Test
		void setExAtForExpirationWithNonMillisUnixTimestamp() {

			long fourHoursFromNowSecs = Instant.now().plus(4L, ChronoUnit.HOURS).getEpochSecond();
			Expiration expiration = Expiration.unixTimestamp(fourHoursFromNowSecs, TimeUnit.SECONDS);
			assertThatParamsHasExpiration(
					JedisConverters.toHSetExParams(RedisHashCommands.HashFieldSetOption.UPSERT, expiration),
					Protocol.Keyword.EXAT, fourHoursFromNowSecs);
		}

		private void assertThatParamsHasExistance(HSetExParams params, Protocol.Keyword existance) {
			assertThat(params).extracting("existance").isEqualTo(existance);
		}

		private void assertThatParamsHasExpiration(HSetExParams params, Protocol.Keyword expirationType,
				Long expirationValue) {
			assertThat(params).extracting("expiration", "expirationValue").containsExactly(expirationType, expirationValue);
		}
	}


}
