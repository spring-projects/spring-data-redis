/*
 * Copyright 2014-present the original author or authors.
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
package org.springframework.data.redis.connection.lettuce;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.redis.connection.ClusterTestVariables.*;
import static org.springframework.data.redis.connection.lettuce.LettuceCommandArgsComparator.*;
import static org.springframework.test.util.ReflectionTestUtils.*;

import io.lettuce.core.GetExArgs;
import io.lettuce.core.Limit;
import io.lettuce.core.RedisCredentials;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SetArgs;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.XTrimArgs;
import io.lettuce.core.cluster.models.partitions.Partitions;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode.NodeFlag;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisClusterNode.Flag;
import org.springframework.data.redis.connection.RedisClusterNode.LinkState;
import org.springframework.data.redis.connection.RedisHashCommands;
import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.RedisStreamCommands.StreamDeletionPolicy;
import org.springframework.data.redis.connection.RedisStreamCommands.TrimOptions;
import org.springframework.data.redis.connection.RedisStreamCommands.XAddOptions;
import org.springframework.data.redis.connection.RedisStreamCommands.XDelOptions;
import org.springframework.data.redis.connection.RedisStreamCommands.XTrimOptions;
import org.springframework.data.redis.connection.RedisStringCommands.SetOption;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.core.types.RedisClientInfo;

/**
 * Unit tests for {@link LettuceConverters}.
 *
 * @author Christoph Strobl
 * @author Vikas Garg
 */
class LettuceConvertersUnitTests {

	private static final String CLIENT_ALL_SINGLE_LINE_RESPONSE = "addr=127.0.0.1:60311 fd=6 name= age=4059 idle=0 flags=N db=0 sub=0 psub=0 multi=-1 qbuf=0 qbuf-free=32768 obl=0 oll=0 omem=0 events=r cmd=client";

	private static final String MASTER_NAME = "mymaster";

	@Test // DATAREDIS-268
	void convertingEmptyStringToListOfRedisClientInfoShouldReturnEmptyList() {
		assertThat(LettuceConverters.toListOfRedisClientInformation(""))
				.isEqualTo(Collections.<RedisClientInfo> emptyList());
	}

	@Test // DATAREDIS-268
	void convertingNullToListOfRedisClientInfoShouldReturnEmptyList() {
		assertThat(LettuceConverters.toListOfRedisClientInformation(null))
				.isEqualTo(Collections.<RedisClientInfo> emptyList());
	}

	@Test // DATAREDIS-268
	void convertingMultipleLiesToListOfRedisClientInfoReturnsListCorrectly() {

		StringBuilder sb = new StringBuilder();
		sb.append(CLIENT_ALL_SINGLE_LINE_RESPONSE);
		sb.append("\r\n");
		sb.append(CLIENT_ALL_SINGLE_LINE_RESPONSE);

		assertThat(LettuceConverters.toListOfRedisClientInformation(sb.toString()).size()).isEqualTo(2);
	}

	@Test // DATAREDIS-315
	void partitionsToClusterNodesShouldReturnEmptyCollectionWhenPartitionsDoesNotContainElements() {
		assertThat(LettuceConverters.partitionsToClusterNodes(new Partitions())).isNotNull();
	}

	@Test // DATAREDIS-315
	void partitionsToClusterNodesShouldConvertPartitionCorrectly() {

		Partitions partitions = new Partitions();

		io.lettuce.core.cluster.models.partitions.RedisClusterNode partition = new io.lettuce.core.cluster.models.partitions.RedisClusterNode();
		partition.setNodeId(CLUSTER_NODE_1.getId());
		partition.setConnected(true);
		partition.setFlags(new HashSet<>(Arrays.asList(NodeFlag.MASTER, NodeFlag.MYSELF)));
		partition.setUri(RedisURI.create("redis://" + CLUSTER_HOST + ":" + MASTER_NODE_1_PORT));
		partition.setSlots(Arrays.asList(1, 2, 3, 4, 5));

		partitions.add(partition);

		List<RedisClusterNode> nodes = LettuceConverters.partitionsToClusterNodes(partitions);
		assertThat(nodes.size()).isEqualTo(1);

		RedisClusterNode node = nodes.get(0);
		assertThat(node.getHost()).isEqualTo(CLUSTER_HOST);
		assertThat(node.getPort()).isEqualTo(MASTER_NODE_1_PORT);
		assertThat(node.getFlags()).contains(Flag.MASTER, Flag.MYSELF);
		assertThat(node.getId()).isEqualTo(CLUSTER_NODE_1.getId());
		assertThat(node.getLinkState()).isEqualTo(LinkState.CONNECTED);
		assertThat(node.getSlotRange().getSlots()).contains(1, 2, 3, 4, 5);
	}

	@Test // DATAREDIS-316
	void toSetArgsShouldReturnEmptyArgsForNullValues() {

		SetArgs args = LettuceConverters.toSetArgs(null, null);

		assertThat(getField(args, "ex")).isNull();
		assertThat(getField(args, "px")).isNull();
		assertThat((Boolean) getField(args, "nx")).isEqualTo(Boolean.FALSE);
		assertThat((Boolean) getField(args, "xx")).isEqualTo(Boolean.FALSE);
	}

	@Test // DATAREDIS-316
	void toSetArgsShouldNotSetExOrPxForPersistent() {

		SetArgs args = LettuceConverters.toSetArgs(Expiration.persistent(), null);

		assertThat(getField(args, "ex")).isNull();
		assertThat(getField(args, "px")).isNull();
		assertThat((Boolean) getField(args, "nx")).isEqualTo(Boolean.FALSE);
		assertThat((Boolean) getField(args, "xx")).isEqualTo(Boolean.FALSE);
	}

	@Test // DATAREDIS-316
	void toSetArgsShouldSetExForSeconds() {

		SetArgs args = LettuceConverters.toSetArgs(Expiration.seconds(10), null);

		assertThat((Long) getField(args, "ex")).isEqualTo(10L);
		assertThat(getField(args, "px")).isNull();
		assertThat((Boolean) getField(args, "nx")).isEqualTo(Boolean.FALSE);
		assertThat((Boolean) getField(args, "xx")).isEqualTo(Boolean.FALSE);
	}

	@Test // GH-2050
	void convertsExpirationToSetPXAT() {

		assertThatCommandArgument(LettuceConverters.toSetArgs(Expiration.unixTimestamp(10, TimeUnit.MILLISECONDS), null))
				.isEqualTo(SetArgs.Builder.pxAt(10));
	}

	@Test // GH-2050
	void convertsExpirationToSetEXAT() {

		assertThatCommandArgument(LettuceConverters.toSetArgs(Expiration.unixTimestamp(1, TimeUnit.MINUTES), null))
				.isEqualTo(SetArgs.Builder.exAt(60));
	}

	@Test // DATAREDIS-316
	void toSetArgsShouldSetPxForMilliseconds() {

		SetArgs args = LettuceConverters.toSetArgs(Expiration.milliseconds(100), null);

		assertThat(getField(args, "ex")).isNull();
		assertThat((Long) getField(args, "px")).isEqualTo(100L);
		assertThat((Boolean) getField(args, "nx")).isEqualTo(Boolean.FALSE);
		assertThat((Boolean) getField(args, "xx")).isEqualTo(Boolean.FALSE);
	}

	@Test // DATAREDIS-316
	void toSetArgsShouldSetNxForAbsent() {

		SetArgs args = LettuceConverters.toSetArgs(null, SetOption.ifAbsent());

		assertThat(getField(args, "ex")).isNull();
		assertThat(getField(args, "px")).isNull();
		assertThat((Boolean) getField(args, "nx")).isEqualTo(Boolean.TRUE);
		assertThat((Boolean) getField(args, "xx")).isEqualTo(Boolean.FALSE);
	}

	@Test // DATAREDIS-316
	void toSetArgsShouldSetXxForPresent() {

		SetArgs args = LettuceConverters.toSetArgs(null, SetOption.ifPresent());

		assertThat(getField(args, "ex")).isNull();
		assertThat(getField(args, "px")).isNull();
		assertThat((Boolean) getField(args, "nx")).isEqualTo(Boolean.FALSE);
		assertThat((Boolean) getField(args, "xx")).isEqualTo(Boolean.TRUE);
	}

	@Test // DATAREDIS-316
	void toSetArgsShouldNotSetNxOrXxForUpsert() {

		SetArgs args = LettuceConverters.toSetArgs(null, SetOption.upsert());

		assertThat(getField(args, "ex")).isNull();
		assertThat(getField(args, "px")).isNull();
		assertThat((Boolean) getField(args, "nx")).isEqualTo(Boolean.FALSE);
		assertThat((Boolean) getField(args, "xx")).isEqualTo(Boolean.FALSE);
	}

	@Test // DATAREDIS-981
	void toLimit() {

		Limit limit = LettuceConverters.toLimit(org.springframework.data.redis.connection.Limit.unlimited());
		assertThat(limit.isLimited()).isFalse();
		assertThat(limit.getCount()).isEqualTo(-1L);

		limit = LettuceConverters.toLimit(org.springframework.data.redis.connection.Limit.limit().count(-1));
		assertThat(limit.isLimited()).isTrue();
		assertThat(limit.getCount()).isEqualTo(-1L);

		limit = LettuceConverters.toLimit(org.springframework.data.redis.connection.Limit.limit().count(5));
		assertThat(limit.isLimited()).isTrue();
		assertThat(limit.getCount()).isEqualTo(5L);
	}

	@Test // GH-2050
	void convertsExpirationToGetExEX() {

		assertThatCommandArgument(LettuceConverters.toGetExArgs(Expiration.seconds(10))).isEqualTo(new GetExArgs().ex(10));
	}

	@Test // GH-2050
	void convertsExpirationWithTimeUnitToGetExEX() {

		assertThatCommandArgument(LettuceConverters.toGetExArgs(Expiration.from(1, TimeUnit.MINUTES)))
				.isEqualTo(new GetExArgs().ex(60));
	}

	@Test // GH-2050
	void convertsExpirationToGetExPEX() {

		assertThatCommandArgument(LettuceConverters.toGetExArgs(Expiration.milliseconds(10)))
				.isEqualTo(new GetExArgs().px(10));
	}

	@Test // GH-2050
	void convertsExpirationToGetExEXAT() {

		assertThatCommandArgument(LettuceConverters.toGetExArgs(Expiration.unixTimestamp(10, TimeUnit.SECONDS)))
				.isEqualTo(new GetExArgs().exAt(10));
	}

	@Test // GH-2050
	void convertsExpirationWithTimeUnitToGetExEXAT() {

		assertThatCommandArgument(LettuceConverters.toGetExArgs(Expiration.unixTimestamp(1, TimeUnit.MINUTES)))
				.isEqualTo(new GetExArgs().exAt(60));
	}

	@Test // GH-2050
	void convertsExpirationToGetExPXAT() {

		assertThatCommandArgument(LettuceConverters.toGetExArgs(Expiration.unixTimestamp(10, TimeUnit.MILLISECONDS)))
				.isEqualTo(new GetExArgs().pxAt(10));
	}

	@Test // GH-2218
	void sentinelConfigurationWithAuth() {

		RedisPassword dataPassword = RedisPassword.of("data-secret");
		RedisPassword sentinelPassword = RedisPassword.of("sentinel-secret");

		RedisSentinelConfiguration sentinelConfiguration = new RedisSentinelConfiguration().master(MASTER_NAME)
				.sentinel("127.0.0.1", 26379).sentinel("127.0.0.1", 26380);
		sentinelConfiguration.setUsername("app");
		sentinelConfiguration.setPassword(dataPassword);

		sentinelConfiguration.setSentinelUsername("admin");
		sentinelConfiguration.setSentinelPassword(sentinelPassword);

		RedisURI redisURI = LettuceConverters.sentinelConfigurationToRedisURI(sentinelConfiguration);
		RedisCredentials redisCredentials = redisURI.getCredentialsProvider().resolveCredentials().block();
		Assertions.assertNotNull(redisCredentials);
		assertThat(redisCredentials.getUsername()).isEqualTo("app");
		assertThat(redisCredentials.getPassword()).isEqualTo(dataPassword.get());

		redisURI.getSentinels().forEach(sentinel -> {
			RedisCredentials sentinelCredentials = sentinel.getCredentialsProvider().resolveCredentials().block();
			Assertions.assertNotNull(sentinelCredentials);
			assertThat(sentinelCredentials.getUsername()).isEqualTo("admin");
			assertThat(sentinelCredentials.getPassword()).isEqualTo(sentinelPassword.get());
		});
	}

	@Test // GH-2218
	void sentinelConfigurationSetSentinelPasswordIfUsernameNotPresent() {

		RedisPassword password = RedisPassword.of("88888888-8x8-getting-creative-now");

		RedisSentinelConfiguration sentinelConfiguration = new RedisSentinelConfiguration().master(MASTER_NAME)
				.sentinel("127.0.0.1", 26379).sentinel("127.0.0.1", 26380);
		sentinelConfiguration.setUsername("app");
		sentinelConfiguration.setPassword(password);
		sentinelConfiguration.setSentinelPassword(password);

		RedisURI redisURI = LettuceConverters.sentinelConfigurationToRedisURI(sentinelConfiguration);
		RedisCredentials redisCredentials = redisURI.getCredentialsProvider().resolveCredentials().block();
		Assertions.assertNotNull(redisCredentials);
		assertThat(redisCredentials.getUsername()).isEqualTo("app");

		redisURI.getSentinels().forEach(sentinel -> {
			RedisCredentials sentinelCredentials = sentinel.getCredentialsProvider().resolveCredentials().block();
			Assertions.assertNotNull(sentinelCredentials);
			assertThat(sentinelCredentials.getUsername()).isNull();
			assertThat(sentinelCredentials.getPassword()).isNotNull();
		});
	}

	@Test // GH-2218
	void sentinelConfigurationShouldNotSetSentinelAuthIfUsernameIsPresentWithNoPassword() {

		RedisPassword password = RedisPassword.of("88888888-8x8-getting-creative-now");

		RedisSentinelConfiguration sentinelConfiguration = new RedisSentinelConfiguration().master(MASTER_NAME)
				.sentinel("127.0.0.1", 26379).sentinel("127.0.0.1", 26380);
		sentinelConfiguration.setUsername("app");
		sentinelConfiguration.setPassword(password);
		sentinelConfiguration.setSentinelUsername("admin");

		RedisURI redisURI = LettuceConverters.sentinelConfigurationToRedisURI(sentinelConfiguration);
		RedisCredentials credentials = redisURI.getCredentialsProvider().resolveCredentials().block();
		Assertions.assertNotNull(credentials);
		assertThat(credentials.getUsername()).isEqualTo("app");

		redisURI.getSentinels().forEach(sentinel -> {
			RedisCredentials sentinelCredentials = sentinel.getCredentialsProvider().resolveCredentials().block();
			Assertions.assertNotNull(sentinelCredentials);
			assertThat(sentinelCredentials.getUsername()).isNull();
			assertThat(sentinelCredentials.getPassword()).isNull();
		});
	}

	@Nested // GH-3211
	class ToHGetExArgsShould {

		@Test
		void notSetAnyFieldsForNullExpiration() {

			assertThat(LettuceConverters.toHGetExArgs(null)).extracting("ex", "exAt", "px", "pxAt", "persist")
					.containsExactly(null, null, null, null, Boolean.FALSE);
		}

		@Test
		void setPersistForNonExpiringExpiration() {

			assertThat(LettuceConverters.toHGetExArgs(Expiration.persistent())).extracting("persist").isEqualTo(Boolean.TRUE);
		}

		@Test
		void setPxForExpirationWithMillisTimeUnit() {

			assertThat(LettuceConverters.toHGetExArgs(Expiration.from(30_000, TimeUnit.MILLISECONDS))).extracting("px")
					.isEqualTo(30_000L);
		}

		@Test
		void setPxAtForExpirationWithMillisUnixTimestamp() {

			long fourHoursFromNowMillis = Instant.now().plus(4L, ChronoUnit.HOURS).toEpochMilli();
			assertThat(
					LettuceConverters.toHGetExArgs(Expiration.unixTimestamp(fourHoursFromNowMillis, TimeUnit.MILLISECONDS)))
					.extracting("pxAt").isEqualTo(fourHoursFromNowMillis);
		}

		@Test
		void setExForExpirationWithNonMillisTimeUnit() {

			assertThat(LettuceConverters.toHGetExArgs(Expiration.from(30, TimeUnit.SECONDS))).extracting("ex").isEqualTo(30L);
		}

		@Test
		void setExAtForExpirationWithNonMillisUnixTimestamp() {

			long fourHoursFromNowSecs = Instant.now().plus(4L, ChronoUnit.HOURS).getEpochSecond();
			assertThat(LettuceConverters.toHGetExArgs(Expiration.unixTimestamp(fourHoursFromNowSecs, TimeUnit.SECONDS)))
					.extracting("exAt").isEqualTo(fourHoursFromNowSecs);
		}
	}

	@Nested
	class ToHSetExArgsShould {

		@Test
		void setFnxForNoneExistCondition() {

			assertThat(LettuceConverters.toHSetExArgs(RedisHashCommands.HashFieldSetOption.IF_NONE_EXIST, null))
					.extracting("fnx").isEqualTo(Boolean.TRUE);
		}

		@Test
		void setFxxForAllExistCondition() {

			assertThat(LettuceConverters.toHSetExArgs(RedisHashCommands.HashFieldSetOption.IF_ALL_EXIST, null))
					.extracting("fxx").isEqualTo(Boolean.TRUE);
		}

		@Test
		void notSetFnxNorFxxForUpsertCondition() {

			assertThat(LettuceConverters.toHSetExArgs(RedisHashCommands.HashFieldSetOption.UPSERT, null))
					.extracting("fnx", "fxx").containsExactly(Boolean.FALSE, Boolean.FALSE);
		}

		@Test
		void notSetAnyTimeFieldsForNullExpiration() {

			assertThat(LettuceConverters.toHSetExArgs(RedisHashCommands.HashFieldSetOption.UPSERT, null))
					.extracting("ex", "exAt", "px", "pxAt").containsExactly(null, null, null, null);
		}

		@Test
		void notSetAnyTimeFieldsForNonExpiringExpiration() {

			assertThat(LettuceConverters.toHSetExArgs(RedisHashCommands.HashFieldSetOption.UPSERT, Expiration.persistent()))
					.extracting("ex", "exAt", "px", "pxAt").containsExactly(null, null, null, null);
		}

		@Test
		void setKeepTtlForKeepTtlExpiration() {

			assertThat(LettuceConverters.toHSetExArgs(RedisHashCommands.HashFieldSetOption.UPSERT, Expiration.keepTtl()))
					.extracting("keepttl").isEqualTo(Boolean.TRUE);
		}

		@Test
		void setPxForExpirationWithMillisTimeUnit() {

			assertThat(LettuceConverters.toHSetExArgs(RedisHashCommands.HashFieldSetOption.UPSERT,
					Expiration.from(30_000, TimeUnit.MILLISECONDS))).extracting("px").isEqualTo(30_000L);
		}

		@Test
		void setPxAtForExpirationWithMillisUnixTimestamp() {

			long fourHoursFromNowMillis = Instant.now().plus(4L, ChronoUnit.HOURS).toEpochMilli();
			Expiration expiration = Expiration.unixTimestamp(fourHoursFromNowMillis, TimeUnit.MILLISECONDS);
			assertThat(LettuceConverters.toHSetExArgs(RedisHashCommands.HashFieldSetOption.UPSERT, expiration))
					.extracting("pxAt").isEqualTo(fourHoursFromNowMillis);
		}

		@Test
		void setExForExpirationWithNonMillisTimeUnit() {

			assertThat(LettuceConverters.toHSetExArgs(RedisHashCommands.HashFieldSetOption.UPSERT,
					Expiration.from(30, TimeUnit.SECONDS))).extracting("ex").isEqualTo(30L);
		}

		@Test
		void setExAtForExpirationWithNonMillisUnixTimestamp() {

			long fourHoursFromNowSecs = Instant.now().plus(4L, ChronoUnit.HOURS).getEpochSecond();
			Expiration expiration = Expiration.unixTimestamp(fourHoursFromNowSecs, TimeUnit.SECONDS);
			assertThat(LettuceConverters.toHSetExArgs(RedisHashCommands.HashFieldSetOption.UPSERT, expiration))
					.extracting("exAt").isEqualTo(fourHoursFromNowSecs);
		}
	}

	@Nested // GH-3232
	class ToXAddArgsShould {

		@Test
		void convertXAddOptionsWithMaxlen() {

			RecordId recordId = RecordId.autoGenerate();
			XAddOptions options = XAddOptions.maxlen(100);

			XAddArgs args = StreamConverters.toXAddArgs(recordId, options);

			assertThat(args).extracting("maxlen").isEqualTo(100L);
		}

		@Test
		void convertXAddOptionsWithMinId() {

			RecordId recordId = RecordId.autoGenerate();
			XAddOptions options = XAddOptions.trim(TrimOptions.minId(RecordId.of("1234567890-0")));

			XAddArgs args = StreamConverters.toXAddArgs(recordId, options);

			assertThat(getField(args, "minid")).isEqualTo("1234567890-0");
		}

		@Test
		void convertXAddOptionsWithApproximateTrimming() {

			RecordId recordId = RecordId.autoGenerate();
			XAddOptions options = XAddOptions.maxlen(100).approximateTrimming(true);

			XAddArgs args = StreamConverters.toXAddArgs(recordId, options);

			assertThat(args).extracting("approximateTrimming").isEqualTo(true);
		}

		@Test
		void convertXAddOptionsWithExactTrimming() {

			RecordId recordId = RecordId.autoGenerate();
			XAddOptions options = XAddOptions.trim(TrimOptions.maxLen(100).exact());

			XAddArgs args = StreamConverters.toXAddArgs(recordId, options);

			assertThat(args).extracting("exactTrimming").isEqualTo(true);
		}

		@Test
		void convertXAddOptionsWithLimit() {

			RecordId recordId = RecordId.autoGenerate();
			XAddOptions options = XAddOptions.trim(TrimOptions.maxLen(100).approximate().limit(50));

			XAddArgs args = StreamConverters.toXAddArgs(recordId, options);

			assertThat(args).extracting("limit").isEqualTo(50L);
		}

		@Test
		void convertXAddOptionsWithDeletionPolicy() {

			RecordId recordId = RecordId.autoGenerate();
			XAddOptions options = XAddOptions.trim(TrimOptions.maxLen(100).deletionPolicy(StreamDeletionPolicy.keep()));

			XAddArgs args = StreamConverters.toXAddArgs(recordId, options);

			assertThat(args).extracting("trimmingMode").isEqualTo(io.lettuce.core.StreamDeletionPolicy.KEEP_REFERENCES);
		}

		@Test
		void convertXAddOptionsWithRecordId() {

			RecordId recordId = RecordId.of("1234567890-0");
			XAddOptions options = XAddOptions.none();

			XAddArgs args = StreamConverters.toXAddArgs(recordId, options);

			assertThat(getField(args, "id")).isEqualTo("1234567890-0");
		}
	}

	@Nested // GH-3232
	class ToXTrimArgsShould {

		@Test
		void convertXTrimOptionsWithMaxlen() {

			XTrimOptions options = XTrimOptions.trim(TrimOptions.maxLen(100));

			XTrimArgs args = StreamConverters.toXTrimArgs(options);

			assertThat(args).extracting("maxlen").isEqualTo(100L);
		}

		@Test
		void convertXTrimOptionsWithMinId() {

			XTrimOptions options = XTrimOptions.trim(TrimOptions.minId(RecordId.of("1234567890-0")));

			XTrimArgs args = StreamConverters.toXTrimArgs(options);

			assertThat(getField(args, "minId")).isEqualTo("1234567890-0");
		}

		@Test
		void convertXTrimOptionsWithApproximateTrimming() {

			XTrimOptions options = XTrimOptions.trim(TrimOptions.maxLen(100).approximate());

			XTrimArgs args = StreamConverters.toXTrimArgs(options);

			assertThat(args).extracting("approximateTrimming").isEqualTo(true);
		}

		@Test
		void convertXTrimOptionsWithExactTrimming() {

			XTrimOptions options = XTrimOptions.trim(TrimOptions.maxLen(100).exact());

			XTrimArgs args = StreamConverters.toXTrimArgs(options);

			assertThat(args).extracting("exactTrimming").isEqualTo(true);
		}

		@Test
		void convertXTrimOptionsWithLimit() {

			XTrimOptions options = XTrimOptions.trim(TrimOptions.maxLen(100).approximate().limit(50));

			XTrimArgs args = StreamConverters.toXTrimArgs(options);

			assertThat(args).extracting("limit").isEqualTo(50L);
		}

		@Test
		void convertXTrimOptionsWithDeletionPolicy() {

			XTrimOptions options = XTrimOptions.trim(TrimOptions.maxLen(100).deletionPolicy(StreamDeletionPolicy.keep()));

			XTrimArgs args = StreamConverters.toXTrimArgs(options);

			assertThat(args).extracting("trimmingMode").isEqualTo(io.lettuce.core.StreamDeletionPolicy.KEEP_REFERENCES);
		}
	}

	@Nested // GH-3232
	class ToXDelArgsShould {

		@Test
		void convertDefaultOptions() {

			XDelOptions options = XDelOptions.defaults();

			io.lettuce.core.StreamDeletionPolicy policy = StreamConverters.toXDelArgs(options);

			assertThat(policy).isEqualTo(io.lettuce.core.StreamDeletionPolicy.KEEP_REFERENCES);
		}

		@Test
		void convertKeepReferencesPolicy() {

			XDelOptions options = XDelOptions.deletionPolicy(StreamDeletionPolicy.keep());

			io.lettuce.core.StreamDeletionPolicy policy = StreamConverters.toXDelArgs(options);

			assertThat(policy).isEqualTo(io.lettuce.core.StreamDeletionPolicy.KEEP_REFERENCES);
		}

		@Test
		void convertDeleteReferencesPolicy() {

			XDelOptions options = XDelOptions.deletionPolicy(StreamDeletionPolicy.delete());

			io.lettuce.core.StreamDeletionPolicy policy = StreamConverters.toXDelArgs(options);

			assertThat(policy).isEqualTo(io.lettuce.core.StreamDeletionPolicy.DELETE_REFERENCES);
		}

		@Test
		void convertAcknowledgedPolicy() {

			XDelOptions options = XDelOptions.deletionPolicy(StreamDeletionPolicy.removeAcknowledged());

			io.lettuce.core.StreamDeletionPolicy policy = StreamConverters.toXDelArgs(options);

			assertThat(policy).isEqualTo(io.lettuce.core.StreamDeletionPolicy.ACKNOWLEDGED);
		}
	}
}
