/*
 * Copyright 2015-2025 the original author or authors.
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
import static org.assertj.core.data.Offset.offset;
import static org.springframework.data.redis.connection.BitFieldSubCommands.*;
import static org.springframework.data.redis.connection.BitFieldSubCommands.BitFieldIncrBy.Overflow.*;
import static org.springframework.data.redis.connection.BitFieldSubCommands.BitFieldType.*;
import static org.springframework.data.redis.connection.ClusterTestVariables.*;
import static org.springframework.data.redis.connection.RedisGeoCommands.DistanceUnit.*;
import static org.springframework.data.redis.connection.RedisGeoCommands.GeoRadiusCommandArgs.*;
import static org.springframework.data.redis.connection.RedisListCommands.*;
import static org.springframework.data.redis.connection.RedisZSetCommands.*;
import static org.springframework.data.redis.core.ScanOptions.*;

import redis.clients.jedis.ConnectionPool;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.domain.Range.Bound;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.*;
import org.springframework.data.redis.connection.Limit;
import org.springframework.data.redis.connection.RedisClusterNode.SlotRange;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation;
import org.springframework.data.redis.connection.RedisServerCommands.FlushOption;
import org.springframework.data.redis.connection.RedisStringCommands.BitOperation;
import org.springframework.data.redis.connection.RedisStringCommands.SetOption;
import org.springframework.data.redis.connection.ValueEncoding.RedisValueEncoding;
import org.springframework.data.redis.connection.zset.DefaultTuple;
import org.springframework.data.redis.connection.zset.Tuple;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.script.DigestUtils;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.test.condition.EnabledOnCommand;
import org.springframework.data.redis.test.condition.EnabledOnRedisClusterAvailable;
import org.springframework.data.redis.test.extension.JedisExtension;
import org.springframework.data.redis.test.util.HexStringUtils;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Pavel Khokhlov
 * @author Dennis Neufeld
 * @author Tihomir Mateev
 */
@EnabledOnRedisClusterAvailable
@ExtendWith(JedisExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class JedisClusterConnectionTests implements ClusterConnectionTests {

	static final List<HostAndPort> CLUSTER_NODES = Arrays.asList(new HostAndPort(CLUSTER_HOST, MASTER_NODE_1_PORT),
			new HostAndPort(CLUSTER_HOST, MASTER_NODE_2_PORT), new HostAndPort(CLUSTER_HOST, MASTER_NODE_3_PORT));

	private static final byte[] KEY_1_BYTES = JedisConverters.toBytes(KEY_1);
	private static final byte[] KEY_2_BYTES = JedisConverters.toBytes(KEY_2);
	private static final byte[] KEY_3_BYTES = JedisConverters.toBytes(KEY_3);

	private static final byte[] SAME_SLOT_KEY_1_BYTES = JedisConverters.toBytes(SAME_SLOT_KEY_1);
	private static final byte[] SAME_SLOT_KEY_2_BYTES = JedisConverters.toBytes(SAME_SLOT_KEY_2);
	private static final byte[] SAME_SLOT_KEY_3_BYTES = JedisConverters.toBytes(SAME_SLOT_KEY_3);

	private static final byte[] VALUE_1_BYTES = JedisConverters.toBytes(VALUE_1);
	private static final byte[] VALUE_2_BYTES = JedisConverters.toBytes(VALUE_2);
	private static final byte[] VALUE_3_BYTES = JedisConverters.toBytes(VALUE_3);

	private static final GeoLocation<byte[]> ARIGENTO = new GeoLocation<>("arigento".getBytes(Charset.forName("UTF-8")),
			POINT_ARIGENTO);
	private static final GeoLocation<byte[]> CATANIA = new GeoLocation<>("catania".getBytes(Charset.forName("UTF-8")),
			POINT_CATANIA);
	private static final GeoLocation<byte[]> PALERMO = new GeoLocation<>("palermo".getBytes(Charset.forName("UTF-8")),
			POINT_PALERMO);

	private final JedisCluster nativeConnection;
	private final JedisClusterConnection clusterConnection;

	public JedisClusterConnectionTests(JedisCluster nativeConnection) {
		this.nativeConnection = nativeConnection;
		clusterConnection = new JedisClusterConnection(this.nativeConnection);
	}

	@BeforeEach
	void beforeEach() throws IOException {

		for (ConnectionPool pool : nativeConnection.getClusterNodes().values()) {
			try (Jedis jedis = new Jedis(pool.getResource())) {
				jedis.flushAll();
			} catch (Exception ignore) {
				// ignore since we cannot remove data from replicas
			}
		}
	}

	@Test // DATAREDIS-315
	public void appendShouldAddValueCorrectly() {

		clusterConnection.append(KEY_1_BYTES, VALUE_1_BYTES);
		clusterConnection.append(KEY_1_BYTES, VALUE_2_BYTES);

		assertThat(nativeConnection.get(KEY_1)).isEqualTo(VALUE_1.concat(VALUE_2));
	}

	@Test // DATAREDIS-315
	public void bRPopLPushShouldWork() {

		nativeConnection.lpush(KEY_1_BYTES, VALUE_1_BYTES, VALUE_2_BYTES);

		assertThat(clusterConnection.bRPopLPush(0, KEY_1_BYTES, KEY_2_BYTES)).isEqualTo(VALUE_1_BYTES);
		assertThat(nativeConnection.exists(KEY_2_BYTES)).isTrue();
	}

	@Test // DATAREDIS-315
	public void bRPopLPushShouldWorkOnSameSlotKeys() {

		nativeConnection.lpush(SAME_SLOT_KEY_1_BYTES, VALUE_1_BYTES, VALUE_2_BYTES);

		assertThat(clusterConnection.bRPopLPush(0, SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES)).isEqualTo(VALUE_1_BYTES);
		assertThat(nativeConnection.exists(SAME_SLOT_KEY_2_BYTES)).isTrue();
	}

	@Test // DATAREDIS-315
	public void bitCountShouldWorkCorrectly() {

		nativeConnection.setbit(KEY_1, 0, true);
		nativeConnection.setbit(KEY_1, 1, false);

		assertThat(clusterConnection.bitCount(KEY_1_BYTES)).isEqualTo(1L);
	}

	@Test // DATAREDIS-315
	public void bitCountWithRangeShouldWorkCorrectly() {

		nativeConnection.setbit(KEY_1, 0, true);
		nativeConnection.setbit(KEY_1, 1, false);
		nativeConnection.setbit(KEY_1, 2, true);
		nativeConnection.setbit(KEY_1, 3, false);
		nativeConnection.setbit(KEY_1, 4, true);

		assertThat(clusterConnection.bitCount(KEY_1_BYTES, 0, 3)).isEqualTo(3L);
	}

	@Test // DATAREDIS-315
	public void bitOpShouldThrowExceptionWhenKeysDoNotMapToSameSlot() {
		assertThatExceptionOfType(DataAccessException.class)
				.isThrownBy(() -> clusterConnection.bitOp(BitOperation.AND, KEY_1_BYTES, KEY_2_BYTES, KEY_3_BYTES));
	}

	@Test // DATAREDIS-315
	void bitOpShouldWorkCorrectly() {

		nativeConnection.set(SAME_SLOT_KEY_1, "foo");
		nativeConnection.set(SAME_SLOT_KEY_2, "bar");

		clusterConnection.bitOp(BitOperation.AND, SAME_SLOT_KEY_3_BYTES, SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES);

		assertThat(nativeConnection.get(SAME_SLOT_KEY_3)).isEqualTo("bab");
	}

	@Test // DATAREDIS-315
	public void blPopShouldPopElementCorrectly() {

		nativeConnection.lpush(KEY_1_BYTES, VALUE_1_BYTES, VALUE_2_BYTES);
		nativeConnection.lpush(KEY_2_BYTES, VALUE_3_BYTES);

		assertThat(clusterConnection.bLPop(100, KEY_1_BYTES, KEY_2_BYTES).size()).isEqualTo(2);
	}

	@Test // DATAREDIS-315
	public void blPopShouldPopElementCorrectlyWhenKeyOnSameSlot() {

		nativeConnection.lpush(SAME_SLOT_KEY_1_BYTES, VALUE_1_BYTES, VALUE_2_BYTES);
		nativeConnection.lpush(SAME_SLOT_KEY_2_BYTES, VALUE_3_BYTES);

		assertThat(clusterConnection.bLPop(100, SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES).size()).isEqualTo(2);
	}

	@Test // DATAREDIS-315
	public void brPopShouldPopElementCorrectly() {

		nativeConnection.lpush(KEY_1_BYTES, VALUE_1_BYTES, VALUE_2_BYTES);
		nativeConnection.lpush(KEY_2_BYTES, VALUE_3_BYTES);

		assertThat(clusterConnection.bRPop(100, KEY_1_BYTES, KEY_2_BYTES).size()).isEqualTo(2);
	}

	@Test // DATAREDIS-315
	public void brPopShouldPopElementCorrectlyWhenKeyOnSameSlot() {

		nativeConnection.lpush(SAME_SLOT_KEY_1_BYTES, VALUE_1_BYTES, VALUE_2_BYTES);
		nativeConnection.lpush(SAME_SLOT_KEY_2_BYTES, VALUE_3_BYTES);

		assertThat(clusterConnection.bRPop(100, SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES).size()).isEqualTo(2);
	}

	@Test // DATAREDIS-315
	public void clientListShouldGetInfosForAllClients() {
		assertThat(clusterConnection.getClientList().isEmpty()).isFalse();
	}

	@Test // DATAREDIS-315
	public void clusterGetMasterReplicaMapShouldListMastersAndReplicasCorrectly() {

		Map<RedisClusterNode, Collection<RedisClusterNode>> masterReplicaMap = clusterConnection
				.clusterGetMasterReplicaMap();

		assertThat(masterReplicaMap).isNotNull();
		assertThat(masterReplicaMap.size()).isEqualTo(3);
		assertThat(masterReplicaMap.get(new RedisClusterNode(CLUSTER_HOST, MASTER_NODE_1_PORT)))
				.contains(new RedisClusterNode(CLUSTER_HOST, REPLICAOF_NODE_1_PORT));
		assertThat(masterReplicaMap.get(new RedisClusterNode(CLUSTER_HOST, MASTER_NODE_2_PORT)).isEmpty()).isTrue();
		assertThat(masterReplicaMap.get(new RedisClusterNode(CLUSTER_HOST, MASTER_NODE_3_PORT)).isEmpty()).isTrue();
	}

	@Test // DATAREDIS-315
	public void clusterGetReplicasShouldReturnReplicaCorrectly() {

		Set<RedisClusterNode> replicas = clusterConnection
				.clusterGetReplicas(new RedisClusterNode(CLUSTER_HOST, MASTER_NODE_1_PORT));

		assertThat(replicas.size()).isEqualTo(1);
		assertThat(replicas).contains(new RedisClusterNode(CLUSTER_HOST, REPLICAOF_NODE_1_PORT));
	}

	@Test // DATAREDIS-315
	public void countKeysShouldReturnNumberOfKeysInSlot() {

		nativeConnection.set(SAME_SLOT_KEY_1, VALUE_1);
		nativeConnection.set(SAME_SLOT_KEY_2, VALUE_2);

		assertThat(clusterConnection.clusterCountKeysInSlot(ClusterSlotHashUtil.calculateSlot(SAME_SLOT_KEY_1)))
				.isEqualTo(2L);
	}

	@Test // DATAREDIS-315
	public void dbSizeForSpecificNodeShouldGetNodeDbSize() {

		nativeConnection.set(KEY_1, VALUE_1);
		nativeConnection.set(KEY_2, VALUE_2);

		assertThat(clusterConnection.dbSize(new RedisClusterNode("127.0.0.1", 7379, SlotRange.empty()))).isEqualTo(1L);
		assertThat(clusterConnection.dbSize(new RedisClusterNode("127.0.0.1", 7380, SlotRange.empty()))).isEqualTo(1L);
		assertThat(clusterConnection.dbSize(new RedisClusterNode("127.0.0.1", 7381, SlotRange.empty()))).isEqualTo(0L);
	}

	@Test // DATAREDIS-315
	public void dbSizeShouldReturnCummulatedDbSize() {

		nativeConnection.set(KEY_1, VALUE_1);
		nativeConnection.set(KEY_2, VALUE_2);

		assertThat(clusterConnection.dbSize()).isEqualTo(2L);
	}

	@Test // DATAREDIS-315
	public void decrByShouldDecreaseValueCorrectly() {

		nativeConnection.set(KEY_1, "5");

		assertThat(clusterConnection.decrBy(KEY_1_BYTES, 4)).isEqualTo(1L);
	}

	@Test // DATAREDIS-315
	public void decrShouldDecreaseValueCorrectly() {

		nativeConnection.set(KEY_1, "5");

		assertThat(clusterConnection.decr(KEY_1_BYTES)).isEqualTo(4L);
	}

	@Test // DATAREDIS-315
	public void delShouldRemoveMultipleKeysCorrectly() {

		nativeConnection.set(KEY_1, VALUE_1);
		nativeConnection.set(KEY_2, VALUE_2);

		clusterConnection.del(KEY_1_BYTES, KEY_2_BYTES);

		assertThat(nativeConnection.get(KEY_1)).isNull();
		assertThat(nativeConnection.get(KEY_2)).isNull();
	}

	@Test // DATAREDIS-315
	public void delShouldRemoveMultipleKeysOnSameSlotCorrectly() {

		nativeConnection.set(SAME_SLOT_KEY_1, VALUE_1);
		nativeConnection.set(SAME_SLOT_KEY_2, VALUE_2);

		clusterConnection.del(SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES);

		assertThat(nativeConnection.get(SAME_SLOT_KEY_1)).isNull();
		assertThat(nativeConnection.get(SAME_SLOT_KEY_2)).isNull();
	}

	@Test // DATAREDIS-315
	public void delShouldRemoveSingleKeyCorrectly() {

		nativeConnection.set(KEY_1, VALUE_1);

		clusterConnection.del(KEY_1_BYTES);

		assertThat(nativeConnection.get(KEY_1)).isNull();
	}

	@Test // DATAREDIS-315
	public void discardShouldThrowException() {
		assertThatExceptionOfType(DataAccessException.class).isThrownBy(clusterConnection::discard);
	}

	@Test // DATAREDIS-315
	public void dumpAndRestoreShouldWorkCorrectly() {

		nativeConnection.set(KEY_1, VALUE_1);

		byte[] dumpedValue = clusterConnection.dump(KEY_1_BYTES);
		clusterConnection.restore(KEY_2_BYTES, 0, dumpedValue);

		assertThat(nativeConnection.get(KEY_2)).isEqualTo(VALUE_1);
	}

	@Test // DATAREDIS-696
	public void dumpAndRestoreWithReplaceOptionShouldWorkCorrectly() {

		nativeConnection.set(KEY_1, VALUE_1);

		byte[] dumpedValue = clusterConnection.keyCommands().dump(KEY_1_BYTES);

		nativeConnection.set(KEY_1, VALUE_2);

		clusterConnection.keyCommands().restore(KEY_1_BYTES, 0, dumpedValue, true);

		assertThat(nativeConnection.get(KEY_1)).isEqualTo(VALUE_1);
	}

	@Test // DATAREDIS-315
	public void echoShouldReturnInputCorrectly() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
				.isThrownBy(() -> clusterConnection.echo(VALUE_1_BYTES));
	}

	@Test // DATAREDIS-315
	public void execShouldThrowException() {
		assertThatExceptionOfType(DataAccessException.class).isThrownBy(clusterConnection::exec);
	}

	@Test // DATAREDIS-689
	void executeWithArgs() {

		assertThat(clusterConnection.execute("SET", KEY_1_BYTES, VALUE_1_BYTES)).isEqualTo("OK".getBytes());

		assertThat(nativeConnection.get(KEY_1)).isEqualTo(VALUE_1);
	}

	@Test // DATAREDIS-689
	void executeWithKeyAndArgs() {

		Object result = clusterConnection.execute("SET", KEY_1_BYTES, Collections.singletonList(VALUE_1_BYTES));
		assertThat(result).isEqualTo("OK".getBytes());

		assertThat(nativeConnection.get(KEY_1)).isEqualTo(VALUE_1);
	}

	@Test // DATAREDIS-689
	void executeWithNoKeyAndArgsThrowsException() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> clusterConnection.execute("KEYS", (byte[]) null, Collections.singletonList("*".getBytes())));
	}

	@Test // DATAREDIS-529
	public void existsShouldCountSameKeyMultipleTimes() {

		nativeConnection.set(KEY_1, "true");

		assertThat(clusterConnection.keyCommands().exists(KEY_1_BYTES, KEY_1_BYTES)).isEqualTo(2L);
	}

	@Test // DATAREDIS-529
	public void existsWithMultipleKeysShouldConsiderAbsentKeys() {

		assertThat(clusterConnection.keyCommands().exists("no-exist-1".getBytes(), "no-exist-2".getBytes())).isEqualTo(0L);
	}

	@Test // DATAREDIS-529
	public void existsWithMultipleKeysShouldReturnResultCorrectly() {

		nativeConnection.set(KEY_1, "true");
		nativeConnection.set(KEY_2, "true");
		nativeConnection.set(KEY_3, "true");

		assertThat(clusterConnection.keyCommands().exists(KEY_1_BYTES, KEY_2_BYTES, KEY_3_BYTES, "nonexistent".getBytes()))
				.isEqualTo(3L);
	}

	@Test // DATAREDIS-315
	public void expireAtShouldBeSetCorrectly() {

		nativeConnection.set(KEY_1, VALUE_1);

		clusterConnection.expireAt(KEY_1_BYTES, System.currentTimeMillis() / 1000 + 5000);

		assertThat(nativeConnection.ttl(JedisConverters.toString(KEY_1_BYTES))).isGreaterThan(1);
	}

	@Test // GH-3114
	@EnabledOnCommand("SPUBLISH") // Redis 7.0
	public void expireAtWithConditionShouldBeSetCorrectly() {

		nativeConnection.set(KEY_1, VALUE_1);

		assertThat(clusterConnection.expireAt(KEY_1_BYTES, System.currentTimeMillis() / 1000 + 5000,
				ExpirationOptions.Condition.XX)).isFalse();
		assertThat(clusterConnection.expireAt(KEY_1_BYTES, System.currentTimeMillis() / 1000 + 5000,
				ExpirationOptions.Condition.NX)).isTrue();
		assertThat(clusterConnection.expireAt(KEY_1_BYTES, System.currentTimeMillis() / 1000 + 15000,
				ExpirationOptions.Condition.LT)).isFalse();

		assertThat(nativeConnection.ttl(JedisConverters.toString(KEY_1_BYTES))).isGreaterThan(1);
	}

	@Test // DATAREDIS-315
	public void expireShouldBeSetCorrectly() {

		nativeConnection.set(KEY_1, VALUE_1);

		clusterConnection.expire(KEY_1_BYTES, 5);

		assertThat(nativeConnection.ttl(JedisConverters.toString(KEY_1_BYTES))).isGreaterThan(1);
	}

	@Test // GH-3114
	@EnabledOnCommand("SPUBLISH") // Redis 7.0
	public void expireWithConditionShouldBeSetCorrectly() {

		nativeConnection.set(KEY_1, VALUE_1);

		assertThat(clusterConnection.expire(KEY_1_BYTES, 15, ExpirationOptions.Condition.XX)).isFalse();
		assertThat(clusterConnection.expire(KEY_1_BYTES, 15, ExpirationOptions.Condition.NX)).isTrue();
		assertThat(clusterConnection.expire(KEY_1_BYTES, 15, ExpirationOptions.Condition.LT)).isFalse();

		assertThat(nativeConnection.ttl(JedisConverters.toString(KEY_1_BYTES))).isGreaterThan(1);
	}

	@Test // DATAREDIS-315
	public void flushDbOnSingleNodeShouldFlushOnlyGivenNodesDb() {

		nativeConnection.set(KEY_1, VALUE_1);
		nativeConnection.set(KEY_2, VALUE_2);

		clusterConnection.flushDb(new RedisClusterNode("127.0.0.1", 7379, SlotRange.empty()));

		assertThat(nativeConnection.get(KEY_1)).isNotNull();
		assertThat(nativeConnection.get(KEY_2)).isNull();
	}

	@Test // GH-2187
	public void flushDbSyncOnSingleNodeShouldFlushOnlyGivenNodesDb() {

		nativeConnection.set(KEY_1, VALUE_1);
		nativeConnection.set(KEY_2, VALUE_2);

		clusterConnection.flushDb(new RedisClusterNode("127.0.0.1", 7379, SlotRange.empty()), FlushOption.SYNC);

		assertThat(nativeConnection.get(KEY_1)).isNotNull();
		assertThat(nativeConnection.get(KEY_2)).isNull();
	}

	@Test // GH-2187
	public void flushDbAsyncOnSingleNodeShouldFlushOnlyGivenNodesDb() {

		nativeConnection.set(KEY_1, VALUE_1);
		nativeConnection.set(KEY_2, VALUE_2);

		clusterConnection.flushDb(new RedisClusterNode("127.0.0.1", 7379, SlotRange.empty()), FlushOption.ASYNC);

		assertThat(nativeConnection.get(KEY_1)).isNotNull();
		assertThat(nativeConnection.get(KEY_2)).isNull();
	}

	@Test // DATAREDIS-315
	public void flushDbShouldFlushAllClusterNodes() {

		nativeConnection.set(KEY_1, VALUE_1);
		nativeConnection.set(KEY_2, VALUE_2);

		clusterConnection.flushDb();

		assertThat(nativeConnection.get(KEY_1)).isNull();
		assertThat(nativeConnection.get(KEY_2)).isNull();
	}

	@Test // GH-2187
	public void flushDbSyncShouldFlushAllClusterNodes() {

		nativeConnection.set(KEY_1, VALUE_1);
		nativeConnection.set(KEY_2, VALUE_2);

		clusterConnection.flushDb(FlushOption.SYNC);

		assertThat(nativeConnection.get(KEY_1)).isNull();
		assertThat(nativeConnection.get(KEY_2)).isNull();
	}

	@Test // GH-2187
	public void flushDbAsyncShouldFlushAllClusterNodes() {

		nativeConnection.set(KEY_1, VALUE_1);
		nativeConnection.set(KEY_2, VALUE_2);

		clusterConnection.flushDb(FlushOption.ASYNC);

		assertThat(nativeConnection.get(KEY_1)).isNull();
		assertThat(nativeConnection.get(KEY_2)).isNull();
	}

	@Test // GH-2187
	public void flushAllOnSingleNodeShouldFlushOnlyGivenNodesDb() {

		nativeConnection.set(KEY_1, VALUE_1);
		nativeConnection.set(KEY_2, VALUE_2);

		clusterConnection.flushAll(new RedisClusterNode("127.0.0.1", 7379, SlotRange.empty()));

		assertThat(nativeConnection.get(KEY_1)).isNotNull();
		assertThat(nativeConnection.get(KEY_2)).isNull();
	}

	@Test // GH-2187
	public void flushAllSyncOnSingleNodeShouldFlushOnlyGivenNodesDb() {

		nativeConnection.set(KEY_1, VALUE_1);
		nativeConnection.set(KEY_2, VALUE_2);

		clusterConnection.flushAll(new RedisClusterNode("127.0.0.1", 7379, SlotRange.empty()), FlushOption.SYNC);

		assertThat(nativeConnection.get(KEY_1)).isNotNull();
		assertThat(nativeConnection.get(KEY_2)).isNull();
	}

	@Test // GH-2187
	public void flushAllAsyncOnSingleNodeShouldFlushOnlyGivenNodesDb() {

		nativeConnection.set(KEY_1, VALUE_1);
		nativeConnection.set(KEY_2, VALUE_2);

		clusterConnection.flushAll(new RedisClusterNode("127.0.0.1", 7379, SlotRange.empty()), FlushOption.ASYNC);

		assertThat(nativeConnection.get(KEY_1)).isNotNull();
		assertThat(nativeConnection.get(KEY_2)).isNull();
	}

	@Test // GH-2187
	public void flushAllShouldFlushAllClusterNodes() {

		nativeConnection.set(KEY_1, VALUE_1);
		nativeConnection.set(KEY_2, VALUE_2);

		clusterConnection.flushAll();

		assertThat(nativeConnection.get(KEY_1)).isNull();
		assertThat(nativeConnection.get(KEY_2)).isNull();
	}

	@Test // GH-2187
	public void flushAllSyncShouldFlushAllClusterNodes() {

		nativeConnection.set(KEY_1, VALUE_1);
		nativeConnection.set(KEY_2, VALUE_2);

		clusterConnection.flushAll(FlushOption.SYNC);

		assertThat(nativeConnection.get(KEY_1)).isNull();
		assertThat(nativeConnection.get(KEY_2)).isNull();
	}

	@Test // GH-2187
	public void flushAllAsyncShouldFlushAllClusterNodes() {

		nativeConnection.set(KEY_1, VALUE_1);
		nativeConnection.set(KEY_2, VALUE_2);

		clusterConnection.flushAll(FlushOption.ASYNC);

		assertThat(nativeConnection.get(KEY_1)).isNull();
		assertThat(nativeConnection.get(KEY_2)).isNull();
	}

	@Test // DATAREDIS-438
	public void geoAddMultipleGeoLocations() {
		assertThat(clusterConnection.geoAdd(KEY_1_BYTES, Arrays.asList(PALERMO, ARIGENTO, CATANIA, PALERMO))).isEqualTo(3L);
	}

	@Test // DATAREDIS-438
	public void geoAddSingleGeoLocation() {
		assertThat(clusterConnection.geoAdd(KEY_1_BYTES, PALERMO)).isEqualTo(1L);
	}

	@Test // DATAREDIS-438
	public void geoDist() {

		nativeConnection.geoadd(KEY_1_BYTES, PALERMO.getPoint().getX(), PALERMO.getPoint().getY(), PALERMO.getName());
		nativeConnection.geoadd(KEY_1_BYTES, ARIGENTO.getPoint().getX(), ARIGENTO.getPoint().getY(), ARIGENTO.getName());
		nativeConnection.geoadd(KEY_1_BYTES, CATANIA.getPoint().getX(), CATANIA.getPoint().getY(), CATANIA.getName());

		Distance distance = clusterConnection.geoDist(KEY_1_BYTES, PALERMO.getName(), CATANIA.getName());
		assertThat(distance.getValue()).isCloseTo(166274.15156960033D, offset(0.005));
		assertThat(distance.getUnit()).isEqualTo("m");
	}

	@Test // DATAREDIS-438
	public void geoDistWithMetric() {

		nativeConnection.geoadd(KEY_1_BYTES, PALERMO.getPoint().getX(), PALERMO.getPoint().getY(), PALERMO.getName());
		nativeConnection.geoadd(KEY_1_BYTES, ARIGENTO.getPoint().getX(), ARIGENTO.getPoint().getY(), ARIGENTO.getName());
		nativeConnection.geoadd(KEY_1_BYTES, CATANIA.getPoint().getX(), CATANIA.getPoint().getY(), CATANIA.getName());

		Distance distance = clusterConnection.geoDist(KEY_1_BYTES, PALERMO.getName(), CATANIA.getName(), KILOMETERS);
		assertThat(distance.getValue()).isCloseTo(166.27415156960033D, offset(0.005));
		assertThat(distance.getUnit()).isEqualTo("km");

	}

	@Test // DATAREDIS-438
	public void geoHash() {

		nativeConnection.geoadd(KEY_1_BYTES, PALERMO.getPoint().getX(), PALERMO.getPoint().getY(), PALERMO.getName());
		nativeConnection.geoadd(KEY_1_BYTES, CATANIA.getPoint().getX(), CATANIA.getPoint().getY(), CATANIA.getName());

		List<String> result = clusterConnection.geoHash(KEY_1_BYTES, PALERMO.getName(), CATANIA.getName());
		assertThat(result).containsExactly("sqc8b49rny0", "sqdtr74hyu0");
	}

	@Test // DATAREDIS-438
	public void geoHashNonExisting() {

		nativeConnection.geoadd(KEY_1_BYTES, PALERMO.getPoint().getX(), PALERMO.getPoint().getY(), PALERMO.getName());
		nativeConnection.geoadd(KEY_1_BYTES, CATANIA.getPoint().getX(), CATANIA.getPoint().getY(), CATANIA.getName());

		List<String> result = clusterConnection.geoHash(KEY_1_BYTES, PALERMO.getName(), ARIGENTO.getName(),
				CATANIA.getName());
		assertThat(result).containsExactly("sqc8b49rny0", (String) null, "sqdtr74hyu0");
	}

	@Test // DATAREDIS-438
	public void geoPosition() {

		nativeConnection.geoadd(KEY_1_BYTES, PALERMO.getPoint().getX(), PALERMO.getPoint().getY(), PALERMO.getName());
		nativeConnection.geoadd(KEY_1_BYTES, CATANIA.getPoint().getX(), CATANIA.getPoint().getY(), CATANIA.getName());

		List<Point> positions = clusterConnection.geoPos(KEY_1_BYTES, PALERMO.getName(), CATANIA.getName());

		assertThat(positions.get(0).getX()).isCloseTo(POINT_PALERMO.getX(), offset(0.005));
		assertThat(positions.get(0).getY()).isCloseTo(POINT_PALERMO.getY(), offset(0.005));

		assertThat(positions.get(1).getX()).isCloseTo(POINT_CATANIA.getX(), offset(0.005));
		assertThat(positions.get(1).getY()).isCloseTo(POINT_CATANIA.getY(), offset(0.005));
	}

	@Test // DATAREDIS-438
	public void geoPositionNonExisting() {

		nativeConnection.geoadd(KEY_1_BYTES, PALERMO.getPoint().getX(), PALERMO.getPoint().getY(), PALERMO.getName());
		nativeConnection.geoadd(KEY_1_BYTES, CATANIA.getPoint().getX(), CATANIA.getPoint().getY(), CATANIA.getName());

		List<Point> positions = clusterConnection.geoPos(KEY_1_BYTES, PALERMO.getName(), ARIGENTO.getName(),
				CATANIA.getName());

		assertThat(positions.get(0).getX()).isCloseTo(POINT_PALERMO.getX(), offset(0.005));
		assertThat(positions.get(0).getY()).isCloseTo(POINT_PALERMO.getY(), offset(0.005));

		assertThat(positions.get(1)).isNull();

		assertThat(positions.get(2).getX()).isCloseTo(POINT_CATANIA.getX(), offset(0.005));
		assertThat(positions.get(2).getY()).isCloseTo(POINT_CATANIA.getY(), offset(0.005));
	}

	@Test // DATAREDIS-438
	public void geoRadiusByMemberShouldApplyLimit() {

		nativeConnection.geoadd(KEY_1_BYTES, PALERMO.getPoint().getX(), PALERMO.getPoint().getY(), PALERMO.getName());
		nativeConnection.geoadd(KEY_1_BYTES, ARIGENTO.getPoint().getX(), ARIGENTO.getPoint().getY(), ARIGENTO.getName());
		nativeConnection.geoadd(KEY_1_BYTES, CATANIA.getPoint().getX(), CATANIA.getPoint().getY(), CATANIA.getName());

		GeoResults<GeoLocation<byte[]>> result = clusterConnection.geoRadiusByMember(KEY_1_BYTES, PALERMO.getName(),
				new Distance(200, KILOMETERS), newGeoRadiusArgs().limit(2));

		assertThat(result.getContent()).hasSize(2);
	}

	@Test // DATAREDIS-438
	public void geoRadiusByMemberShouldReturnDistanceCorrectly() {

		nativeConnection.geoadd(KEY_1_BYTES, PALERMO.getPoint().getX(), PALERMO.getPoint().getY(), PALERMO.getName());
		nativeConnection.geoadd(KEY_1_BYTES, ARIGENTO.getPoint().getX(), ARIGENTO.getPoint().getY(), ARIGENTO.getName());
		nativeConnection.geoadd(KEY_1_BYTES, CATANIA.getPoint().getX(), CATANIA.getPoint().getY(), CATANIA.getName());

		GeoResults<GeoLocation<byte[]>> result = clusterConnection.geoRadiusByMember(KEY_1_BYTES, PALERMO.getName(),
				new Distance(100, KILOMETERS), newGeoRadiusArgs().includeDistance());

		assertThat(result.getContent()).hasSize(2);
		assertThat(result.getContent().get(0).getDistance().getValue()).isCloseTo(90.978D, offset(0.005));
		assertThat(result.getContent().get(0).getDistance().getUnit()).isEqualTo("km");
	}

	@Test // DATAREDIS-438
	public void geoRadiusByMemberShouldReturnMembersCorrectly() {

		nativeConnection.geoadd(KEY_1_BYTES, PALERMO.getPoint().getX(), PALERMO.getPoint().getY(), PALERMO.getName());
		nativeConnection.geoadd(KEY_1_BYTES, ARIGENTO.getPoint().getX(), ARIGENTO.getPoint().getY(), ARIGENTO.getName());
		nativeConnection.geoadd(KEY_1_BYTES, CATANIA.getPoint().getX(), CATANIA.getPoint().getY(), CATANIA.getName());

		GeoResults<GeoLocation<byte[]>> result = clusterConnection.geoRadiusByMember(KEY_1_BYTES, PALERMO.getName(),
				new Distance(100, KILOMETERS), newGeoRadiusArgs().sortAscending());

		assertThat(result.getContent().get(0).getContent().getName()).isEqualTo(PALERMO.getName());
		assertThat(result.getContent().get(1).getContent().getName()).isEqualTo(ARIGENTO.getName());
	}

	@Test // DATAREDIS-438
	public void geoRadiusShouldApplyLimit() {

		nativeConnection.geoadd(KEY_1_BYTES, PALERMO.getPoint().getX(), PALERMO.getPoint().getY(), PALERMO.getName());
		nativeConnection.geoadd(KEY_1_BYTES, ARIGENTO.getPoint().getX(), ARIGENTO.getPoint().getY(), ARIGENTO.getName());
		nativeConnection.geoadd(KEY_1_BYTES, CATANIA.getPoint().getX(), CATANIA.getPoint().getY(), CATANIA.getName());

		GeoResults<GeoLocation<byte[]>> result = clusterConnection.geoRadius(KEY_1_BYTES,
				new Circle(new Point(15D, 37D), new Distance(200D, KILOMETERS)), newGeoRadiusArgs().limit(2));

		assertThat(result.getContent()).hasSize(2);
	}

	@Test // DATAREDIS-438
	public void geoRadiusShouldReturnDistanceCorrectly() {

		nativeConnection.geoadd(KEY_1_BYTES, PALERMO.getPoint().getX(), PALERMO.getPoint().getY(), PALERMO.getName());
		nativeConnection.geoadd(KEY_1_BYTES, ARIGENTO.getPoint().getX(), ARIGENTO.getPoint().getY(), ARIGENTO.getName());
		nativeConnection.geoadd(KEY_1_BYTES, CATANIA.getPoint().getX(), CATANIA.getPoint().getY(), CATANIA.getName());

		GeoResults<GeoLocation<byte[]>> result = clusterConnection.geoRadius(KEY_1_BYTES,
				new Circle(new Point(15D, 37D), new Distance(200D, KILOMETERS)), newGeoRadiusArgs().includeDistance());

		assertThat(result.getContent()).hasSize(3);
		assertThat(result.getContent().get(0).getDistance().getValue()).isCloseTo(130.423D, offset(0.005));
		assertThat(result.getContent().get(0).getDistance().getUnit()).isEqualTo("km");
	}

	@Test // DATAREDIS-438
	public void geoRadiusShouldReturnMembersCorrectly() {

		nativeConnection.geoadd(KEY_1_BYTES, PALERMO.getPoint().getX(), PALERMO.getPoint().getY(), PALERMO.getName());
		nativeConnection.geoadd(KEY_1_BYTES, ARIGENTO.getPoint().getX(), ARIGENTO.getPoint().getY(), ARIGENTO.getName());
		nativeConnection.geoadd(KEY_1_BYTES, CATANIA.getPoint().getX(), CATANIA.getPoint().getY(), CATANIA.getName());

		GeoResults<GeoLocation<byte[]>> result = clusterConnection.geoRadius(KEY_1_BYTES,
				new Circle(new Point(15D, 37D), new Distance(150D, KILOMETERS)));
		assertThat(result.getContent()).hasSize(2);
	}

	@Test // DATAREDIS-438
	public void geoRemoveDeletesMembers() {

		nativeConnection.geoadd(KEY_1_BYTES, PALERMO.getPoint().getX(), PALERMO.getPoint().getY(), PALERMO.getName());
		nativeConnection.geoadd(KEY_1_BYTES, ARIGENTO.getPoint().getX(), ARIGENTO.getPoint().getY(), ARIGENTO.getName());
		nativeConnection.geoadd(KEY_1_BYTES, CATANIA.getPoint().getX(), CATANIA.getPoint().getY(), CATANIA.getName());

		assertThat(clusterConnection.geoRemove(KEY_1_BYTES, ARIGENTO.getName())).isEqualTo(1L);
	}

	@Test // DATAREDIS-315
	public void getBitShouldWorkCorrectly() {

		nativeConnection.setbit(KEY_1, 0, true);
		nativeConnection.setbit(KEY_1, 1, false);

		assertThat(clusterConnection.getBit(KEY_1_BYTES, 0)).isTrue();
		assertThat(clusterConnection.getBit(KEY_1_BYTES, 1)).isFalse();
	}

	@Test // DATAREDIS-315
	public void getClusterNodeForKeyShouldReturnNodeCorrectly() {
		assertThat((RedisNode) clusterConnection.clusterGetNodeForKey(KEY_1_BYTES))
				.isEqualTo(new RedisNode("127.0.0.1", 7380));
	}

	@Test // DATAREDIS-315, DATAREDIS-661
	public void getConfigShouldLoadConfigurationOfSpecificNode() {

		Properties result = clusterConnection.getConfig(new RedisClusterNode(CLUSTER_HOST, REPLICAOF_NODE_1_PORT), "*");

		assertThat(result.getProperty("slaveof")).endsWith("7379");
	}

	@Test // DATAREDIS-315, DATAREDIS-661
	public void getConfigShouldLoadCumulatedConfiguration() {

		Properties result = clusterConnection.getConfig("*max-*-entries*");

		// config get *max-*-entries on redis 3.0.7 returns 8 entries per node while on 3.2.0-rc3 returns 6.
		// @link https://github.com/spring-projects/spring-data-redis/pull/187
		assertThat(result.size() % 3).isEqualTo(0);

		for (Object o : result.keySet()) {

			assertThat(o.toString()).startsWith(CLUSTER_HOST);
			assertThat(result.getProperty(o.toString())).doesNotStartWith(CLUSTER_HOST);
		}
	}

	@Test // DATAREDIS-315
	public void getRangeShouldReturnValueCorrectly() {

		nativeConnection.set(KEY_1, VALUE_1);

		assertThat(clusterConnection.getRange(KEY_1_BYTES, 0, 2)).isEqualTo(JedisConverters.toBytes("val"));
	}

	@Test // GH-2050
	@EnabledOnCommand("GETEX")
	public void getExShouldWorkCorrectly() {

		nativeConnection.set(KEY_1, VALUE_1);

		assertThat(clusterConnection.getEx(KEY_1_BYTES, Expiration.seconds(10))).isEqualTo(VALUE_1_BYTES);
		assertThat(clusterConnection.ttl(KEY_1_BYTES)).isGreaterThan(1);
	}

	@Test // GH-2050
	@EnabledOnCommand("GETDEL")
	public void getDelShouldWorkCorrectly() {

		nativeConnection.set(KEY_1, VALUE_1);

		assertThat(clusterConnection.getDel(KEY_1_BYTES)).isEqualTo(VALUE_1_BYTES);
		assertThat(clusterConnection.exists(KEY_1_BYTES)).isFalse();
	}

	@Test // DATAREDIS-315
	public void getSetShouldWorkCorrectly() {

		nativeConnection.set(KEY_1, VALUE_1);

		byte[] valueBeforeSet = clusterConnection.getSet(KEY_1_BYTES, VALUE_2_BYTES);

		assertThat(valueBeforeSet).isEqualTo(VALUE_1_BYTES);
		assertThat(nativeConnection.get(KEY_1)).isEqualTo(VALUE_2);
	}

	@Test // DATAREDIS-315
	public void getShouldReturnValueCorrectly() {

		nativeConnection.set(KEY_1, VALUE_1);

		assertThat(clusterConnection.get(KEY_1_BYTES)).isEqualTo(VALUE_1_BYTES);
	}

	@Test // DATAREDIS-315
	public void hDelShouldRemoveFieldsCorrectly() {

		nativeConnection.hset(KEY_1_BYTES, KEY_2_BYTES, VALUE_1_BYTES);
		nativeConnection.hset(KEY_1_BYTES, KEY_3_BYTES, VALUE_2_BYTES);

		clusterConnection.hDel(KEY_1_BYTES, KEY_2_BYTES);

		assertThat(nativeConnection.hexists(KEY_1_BYTES, KEY_2_BYTES)).isFalse();
		assertThat(nativeConnection.hexists(KEY_1_BYTES, KEY_3_BYTES)).isTrue();
	}

	@Test // DATAREDIS-315
	public void hExistsShouldReturnPresenceOfFieldCorrectly() {

		nativeConnection.hset(KEY_1_BYTES, KEY_2_BYTES, VALUE_1_BYTES);

		assertThat(clusterConnection.hExists(KEY_1_BYTES, KEY_2_BYTES)).isTrue();
		assertThat(clusterConnection.hExists(KEY_1_BYTES, KEY_3_BYTES)).isFalse();
		assertThat(clusterConnection.hExists(JedisConverters.toBytes("foo"), KEY_2_BYTES)).isFalse();
	}

	@Test // DATAREDIS-315
	public void hGetAllShouldRetrieveEntriesCorrectly() {

		Map<byte[], byte[]> hashes = new HashMap<>();
		hashes.put(KEY_2_BYTES, VALUE_1_BYTES);
		hashes.put(KEY_3_BYTES, VALUE_2_BYTES);

		nativeConnection.hmset(KEY_1_BYTES, hashes);

		Map<byte[], byte[]> hGetAll = clusterConnection.hGetAll(KEY_1_BYTES);

		assertThat(hGetAll.containsKey(KEY_2_BYTES)).isTrue();
		assertThat(hGetAll.containsKey(KEY_3_BYTES)).isTrue();
	}

	@Test // DATAREDIS-315
	public void hGetShouldRetrieveValueCorrectly() {

		nativeConnection.hset(KEY_1_BYTES, KEY_2_BYTES, VALUE_1_BYTES);

		assertThat(clusterConnection.hGet(KEY_1_BYTES, KEY_2_BYTES)).isEqualTo(VALUE_1_BYTES);
	}

	@Test // DATAREDIS-315
	public void hIncrByFloatShouldIncreaseFieldCorretly() {

		nativeConnection.hset(KEY_1_BYTES, KEY_2_BYTES, JedisConverters.toBytes(1L));
		nativeConnection.hset(KEY_1_BYTES, KEY_3_BYTES, JedisConverters.toBytes(2L));

		clusterConnection.hIncrBy(KEY_1_BYTES, KEY_3_BYTES, 3.5D);

		assertThat(nativeConnection.hget(KEY_1_BYTES, KEY_3_BYTES)).isEqualTo(JedisConverters.toBytes(5.5D));
	}

	@Test // DATAREDIS-315
	public void hIncrByShouldIncreaseFieldCorretly() {

		nativeConnection.hset(KEY_1_BYTES, KEY_2_BYTES, JedisConverters.toBytes(1L));
		nativeConnection.hset(KEY_1_BYTES, KEY_3_BYTES, JedisConverters.toBytes(2L));

		clusterConnection.hIncrBy(KEY_1_BYTES, KEY_3_BYTES, 3);

		assertThat(nativeConnection.hget(KEY_1_BYTES, KEY_3_BYTES)).isEqualTo(JedisConverters.toBytes(5L));
	}

	@Test // DATAREDIS-315
	public void hKeysShouldRetrieveKeysCorrectly() {

		nativeConnection.hset(KEY_1_BYTES, KEY_2_BYTES, VALUE_1_BYTES);
		nativeConnection.hset(KEY_1_BYTES, KEY_3_BYTES, VALUE_2_BYTES);

		assertThat(clusterConnection.hKeys(KEY_1_BYTES)).contains(KEY_2_BYTES, KEY_3_BYTES);
	}

	@Test // DATAREDIS-315
	public void hLenShouldRetrieveSizeCorrectly() {

		nativeConnection.hset(KEY_1_BYTES, KEY_2_BYTES, VALUE_1_BYTES);
		nativeConnection.hset(KEY_1_BYTES, KEY_3_BYTES, VALUE_2_BYTES);

		assertThat(clusterConnection.hLen(KEY_1_BYTES)).isEqualTo(2L);
	}

	@Test // DATAREDIS-315
	public void hMGetShouldRetrieveValueCorrectly() {

		nativeConnection.hset(KEY_1_BYTES, KEY_2_BYTES, VALUE_1_BYTES);
		nativeConnection.hset(KEY_1_BYTES, KEY_3_BYTES, VALUE_2_BYTES);

		assertThat(clusterConnection.hMGet(KEY_1_BYTES, KEY_2_BYTES, KEY_3_BYTES)).contains(VALUE_1_BYTES, VALUE_2_BYTES);
	}

	@Test // DATAREDIS-315
	public void hMSetShouldAddValuesCorrectly() {

		Map<byte[], byte[]> hashes = new HashMap<>();
		hashes.put(KEY_2_BYTES, VALUE_1_BYTES);
		hashes.put(KEY_3_BYTES, VALUE_2_BYTES);

		clusterConnection.hMSet(KEY_1_BYTES, hashes);

		assertThat(nativeConnection.hmget(KEY_1_BYTES, KEY_2_BYTES, KEY_3_BYTES)).contains(VALUE_1_BYTES, VALUE_2_BYTES);
	}

	@Test // DATAREDIS-479
	public void hScanShouldReadEntireValueRange() {

		int nrOfValues = 321;
		for (int i = 0; i < nrOfValues; i++) {
			nativeConnection.hset(KEY_1_BYTES, JedisConverters.toBytes("key" + i), JedisConverters.toBytes("value-" + i));
		}

		Cursor<Map.Entry<byte[], byte[]>> cursor = clusterConnection.hScan(KEY_1_BYTES,
				scanOptions().match("key*").build());

		int i = 0;
		while (cursor.hasNext()) {

			cursor.next();
			i++;
		}

		assertThat(i).isEqualTo(nrOfValues);
	}

	@Test // DATAREDIS-315
	public void hSetNXShouldNotSetValueWhenAlreadyExists() {

		nativeConnection.hset(KEY_1_BYTES, KEY_2_BYTES, VALUE_1_BYTES);

		clusterConnection.hSetNX(KEY_1_BYTES, KEY_2_BYTES, VALUE_2_BYTES);

		assertThat(nativeConnection.hget(KEY_1_BYTES, KEY_2_BYTES)).isEqualTo(VALUE_1_BYTES);
	}

	@Test // DATAREDIS-315
	public void hSetNXShouldSetValueCorrectly() {

		clusterConnection.hSetNX(KEY_1_BYTES, KEY_2_BYTES, VALUE_1_BYTES);

		assertThat(nativeConnection.hget(KEY_1_BYTES, KEY_2_BYTES)).isEqualTo(VALUE_1_BYTES);
	}

	@Test // DATAREDIS-315
	public void hSetShouldSetValueCorrectly() {

		clusterConnection.hSet(KEY_1_BYTES, KEY_2_BYTES, VALUE_1_BYTES);

		assertThat(nativeConnection.hget(KEY_1_BYTES, KEY_2_BYTES)).isEqualTo(VALUE_1_BYTES);
	}

	@Test // DATAREDIS-698
	public void hStrLenReturnsFieldLength() {

		nativeConnection.hset(KEY_1, KEY_2, VALUE_3);

		assertThat(clusterConnection.hashCommands().hStrLen(KEY_1_BYTES, KEY_2_BYTES))
				.isEqualTo(Long.valueOf(VALUE_3.length()));
	}

	@Test // DATAREDIS-698
	public void hStrLenReturnsZeroWhenFieldDoesNotExist() {

		nativeConnection.hset(KEY_1, KEY_2, VALUE_3);

		assertThat(clusterConnection.hashCommands().hStrLen(KEY_1_BYTES, KEY_3_BYTES)).isEqualTo(0L);
	}

	@Test // DATAREDIS-698
	public void hStrLenReturnsZeroWhenKeyDoesNotExist() {
		assertThat(clusterConnection.hashCommands().hStrLen(KEY_1_BYTES, KEY_1_BYTES)).isEqualTo(0L);
	}

	@Test // GH-3054
	@EnabledOnCommand("HEXPIRE")
	public void hExpireReturnsSuccessAndSetsTTL() {

		nativeConnection.hset(KEY_1, KEY_2, VALUE_3);

		assertThat(clusterConnection.hashCommands().hExpire(KEY_1_BYTES, 5L, KEY_2_BYTES)).contains(1L);
		assertThat(clusterConnection.hashCommands().hTtl(KEY_1_BYTES, KEY_2_BYTES))
				.allSatisfy(val -> assertThat(val).isBetween(0L, 5L));
	}

	@Test // GH-3054
	@EnabledOnCommand("HEXPIRE")
	public void hExpireReturnsMinusTwoWhenFieldDoesNotExist() {

		nativeConnection.hset(KEY_1, KEY_2, VALUE_3);
		// missing field
		assertThat(clusterConnection.hashCommands().hExpire(KEY_1_BYTES, 5L, KEY_1_BYTES)).contains(-2L);
		// missing key
		assertThat(clusterConnection.hashCommands().hExpire(KEY_2_BYTES, 5L, KEY_2_BYTES)).contains(-2L);
	}

	@Test // GH-3054
	@EnabledOnCommand("HEXPIRE")
	public void hExpireReturnsTwoWhenZeroProvided() {

		nativeConnection.hset(KEY_1, KEY_2, VALUE_3);

		assertThat(clusterConnection.hashCommands().hExpire(KEY_1_BYTES, 0L, KEY_2_BYTES)).contains(2L);
	}

	@Test // GH-3054
	@EnabledOnCommand("HEXPIRE")
	public void hpExpireReturnsSuccessAndSetsTTL() {

		nativeConnection.hset(KEY_1, KEY_2, VALUE_3);

		assertThat(clusterConnection.hashCommands().hpExpire(KEY_1_BYTES, 5000L, KEY_2_BYTES)).contains(1L);
		assertThat(clusterConnection.hashCommands().hTtl(KEY_1_BYTES, TimeUnit.MILLISECONDS, KEY_2_BYTES))
				.allSatisfy(val -> assertThat(val).isBetween(0L, 5000L));
	}

	@Test // GH-3054
	@EnabledOnCommand("HEXPIRE")
	public void hpExpireReturnsMinusTwoWhenFieldDoesNotExist() {

		nativeConnection.hset(KEY_1, KEY_2, VALUE_3);
		// missing field
		assertThat(clusterConnection.hashCommands().hpExpire(KEY_1_BYTES, 5L, KEY_1_BYTES)).contains(-2L);
		// missing key
		assertThat(clusterConnection.hashCommands().hpExpire(KEY_2_BYTES, 5L, KEY_2_BYTES)).contains(-2L);
	}

	@Test // GH-3054
	@EnabledOnCommand("HEXPIRE")
	public void hpExpireReturnsTwoWhenZeroProvided() {

		nativeConnection.hset(KEY_1, KEY_2, VALUE_3);

		assertThat(clusterConnection.hashCommands().hpExpire(KEY_1_BYTES, 0L, KEY_2_BYTES)).contains(2L);
	}

	@Test // GH-3054
	@EnabledOnCommand("HEXPIRE")
	public void hExpireAtReturnsSuccessAndSetsTTL() {

		nativeConnection.hset(KEY_1, KEY_2, VALUE_3);
		long inFiveSeconds = Instant.now().plusSeconds(5L).getEpochSecond();

		assertThat(clusterConnection.hashCommands().hExpireAt(KEY_1_BYTES, inFiveSeconds, KEY_2_BYTES)).contains(1L);
		assertThat(clusterConnection.hashCommands().hTtl(KEY_1_BYTES, KEY_2_BYTES))
				.allSatisfy(val -> assertThat(val).isBetween(0L, 5L));
	}

	@Test // GH-3054
	@EnabledOnCommand("HEXPIRE")
	public void hExpireAtReturnsMinusTwoWhenFieldDoesNotExist() {

		nativeConnection.hset(KEY_1, KEY_2, VALUE_3);
		long inFiveSeconds = Instant.now().plusSeconds(5L).getEpochSecond();

		// missing field
		assertThat(clusterConnection.hashCommands().hExpireAt(KEY_1_BYTES, inFiveSeconds, KEY_1_BYTES)).contains(-2L);
		// missing key
		assertThat(clusterConnection.hashCommands().hExpireAt(KEY_2_BYTES, inFiveSeconds, KEY_2_BYTES)).contains(-2L);
	}

	@Test // GH-3054
	@EnabledOnCommand("HEXPIRE")
	public void hExpireAdReturnsTwoWhenZeroProvided() {

		nativeConnection.hset(KEY_1, KEY_2, VALUE_3);

		assertThat(clusterConnection.hashCommands().hExpireAt(KEY_1_BYTES, 0L, KEY_2_BYTES)).contains(2L);
	}

	@Test // GH-3054
	@EnabledOnCommand("HEXPIRE")
	public void hpExpireAtReturnsSuccessAndSetsTTL() {

		nativeConnection.hset(KEY_1, KEY_2, VALUE_3);
		long inFiveSeconds = Instant.now().plusSeconds(5L).toEpochMilli();

		assertThat(clusterConnection.hashCommands().hpExpireAt(KEY_1_BYTES, inFiveSeconds, KEY_2_BYTES)).contains(1L);
		assertThat(clusterConnection.hashCommands().hTtl(KEY_1_BYTES, TimeUnit.MILLISECONDS, KEY_2_BYTES))
				.allSatisfy(val -> assertThat(val).isBetween(0L, 5000L));
	}

	@Test // GH-3054
	@EnabledOnCommand("HEXPIRE")
	public void hpExpireAtReturnsMinusTwoWhenFieldDoesNotExist() {

		nativeConnection.hset(KEY_1, KEY_2, VALUE_3);
		long inFiveSeconds = Instant.now().plusSeconds(5L).toEpochMilli();

		// missing field
		assertThat(clusterConnection.hashCommands().hpExpireAt(KEY_1_BYTES, inFiveSeconds, KEY_1_BYTES)).contains(-2L);
		// missing key
		assertThat(clusterConnection.hashCommands().hpExpireAt(KEY_2_BYTES, inFiveSeconds, KEY_2_BYTES)).contains(-2L);
	}

	@Test // GH-3054
	@EnabledOnCommand("HEXPIRE")
	public void hpExpireAdReturnsTwoWhenZeroProvided() {

		nativeConnection.hset(KEY_1, KEY_2, VALUE_3);

		assertThat(clusterConnection.hashCommands().hpExpireAt(KEY_1_BYTES, 0L, KEY_2_BYTES)).contains(2L);
	}

	@Test // GH-3054
	@EnabledOnCommand("HEXPIRE")
	public void hPersistReturnsSuccessAndPersistsField() {

		nativeConnection.hset(KEY_1, KEY_2, VALUE_3);

		assertThat(clusterConnection.hashCommands().hExpire(KEY_1_BYTES, 5L, KEY_2_BYTES)).contains(1L);
		assertThat(clusterConnection.hashCommands().hPersist(KEY_1_BYTES, KEY_2_BYTES)).contains(1L);
		assertThat(clusterConnection.hashCommands().hTtl(KEY_1_BYTES, KEY_2_BYTES)).contains(-1L);
	}

	@Test // GH-3054
	@EnabledOnCommand("HEXPIRE")
	public void hPersistReturnsMinusOneWhenFieldDoesNotHaveExpiration() {

		nativeConnection.hset(KEY_1, KEY_2, VALUE_3);
		assertThat(clusterConnection.hashCommands().hPersist(KEY_1_BYTES, KEY_2_BYTES)).contains(-1L);
	}

	@Test // GH-3054
	@EnabledOnCommand("HEXPIRE")
	public void hPersistReturnsMinusTwoWhenFieldOrKeyMissing() {

		nativeConnection.hset(KEY_1, KEY_2, VALUE_3);

		assertThat(clusterConnection.hashCommands().hPersist(KEY_1_BYTES, KEY_1_BYTES)).contains(-2L);
		assertThat(clusterConnection.hashCommands().hPersist(KEY_3_BYTES, KEY_2_BYTES)).contains(-2L);

	}

	@Test // GH-3054
	@EnabledOnCommand("HEXPIRE")
	public void hTtlReturnsMinusOneWhenFieldHasNoExpiration() {

		nativeConnection.hset(KEY_1, KEY_2, VALUE_3);

		assertThat(clusterConnection.hashCommands().hTtl(KEY_1_BYTES, KEY_2_BYTES)).contains(-1L);
	}

	@Test // GH-3054
	@EnabledOnCommand("HEXPIRE")
	public void hTtlReturnsMinusTwoWhenFieldOrKeyMissing() {

		assertThat(clusterConnection.hashCommands().hTtl(KEY_1_BYTES, KEY_1_BYTES)).contains(-2L);
		assertThat(clusterConnection.hashCommands().hTtl(KEY_3_BYTES, KEY_2_BYTES)).contains(-2L);
	}

	@Test // GH-3211
	@EnabledOnCommand("HGETDEL")
	public void hGetDelReturnsValueAndDeletesField() {

		nativeConnection.hset(KEY_1, KEY_2, VALUE_1);
		nativeConnection.hset(KEY_1, KEY_3, VALUE_2);

		List<byte[]> result = clusterConnection.hashCommands().hGetDel(KEY_1_BYTES, KEY_2_BYTES);
		assertThat(result).hasSize(1);
		assertThat(result.get(0)).isEqualTo(VALUE_1_BYTES);
		assertThat(clusterConnection.hashCommands().hExists(KEY_1_BYTES, KEY_2_BYTES)).isFalse();
		assertThat(clusterConnection.hashCommands().hExists(KEY_1_BYTES, KEY_3_BYTES)).isTrue();
	}

	@Test // GH-3211
	@EnabledOnCommand("HGETDEL")
	public void hGetDelReturnsNullWhenFieldDoesNotExist() {

		nativeConnection.hset(KEY_1, KEY_2, VALUE_1);

		List<byte[]> result = clusterConnection.hashCommands().hGetDel(KEY_1_BYTES, KEY_3_BYTES);
		assertThat(result).hasSize(1);
		assertThat(result.get(0)).isNull();
		assertThat(clusterConnection.hashCommands().hExists(KEY_1_BYTES, KEY_2_BYTES)).isTrue();
	}

	@Test // GH-3211
	@EnabledOnCommand("HGETDEL")
	public void hGetDelReturnsNullWhenKeyDoesNotExist() {

		List<byte[]> result = clusterConnection.hashCommands().hGetDel(KEY_1_BYTES, KEY_2_BYTES);
		assertThat(result).hasSize(1);
		assertThat(result.get(0)).isNull();
	}

	@Test // GH-3211
	@EnabledOnCommand("HGETDEL")
	public void hGetDelMultipleFieldsReturnsValuesAndDeletesFields() {

		nativeConnection.hset(KEY_1, KEY_2, VALUE_1);
		nativeConnection.hset(KEY_1, KEY_3, VALUE_2);
		nativeConnection.hset(KEY_1, "field3", "value3");

		List<byte[]> result = clusterConnection.hashCommands().hGetDel(KEY_1_BYTES, KEY_2_BYTES, KEY_3_BYTES);

		assertThat(result).hasSize(2);
		assertThat(result.get(0)).isEqualTo(VALUE_1_BYTES);
		assertThat(result.get(1)).isEqualTo(VALUE_2_BYTES);

		assertThat(clusterConnection.hashCommands().hExists(KEY_1_BYTES, KEY_2_BYTES)).isFalse();
		assertThat(clusterConnection.hashCommands().hExists(KEY_1_BYTES, KEY_3_BYTES)).isFalse();
		assertThat(clusterConnection.hashCommands().hExists(KEY_1_BYTES, "field3".getBytes())).isTrue();
	}

	@Test // GH-3211
	@EnabledOnCommand("HGETDEL")
	public void hGetDelMultipleFieldsWithNonExistentFields() {

		nativeConnection.hset(KEY_1, KEY_2, VALUE_1);

		List<byte[]> result = clusterConnection.hashCommands().hGetDel(KEY_1_BYTES, KEY_2_BYTES, KEY_3_BYTES);

		assertThat(result).hasSize(2);
		assertThat(result.get(0)).isEqualTo(VALUE_1_BYTES);
		assertThat(result.get(1)).isNull();

		assertThat(clusterConnection.hashCommands().hExists(KEY_1_BYTES, KEY_2_BYTES)).isFalse();
	}

	@Test // GH-3211
	@EnabledOnCommand("HGETDEL")
	public void hGetDelDeletesKeyWhenAllFieldsRemoved() {

		nativeConnection.hset(KEY_1, KEY_2, VALUE_1);
		nativeConnection.hset(KEY_1, KEY_3, VALUE_2);

		List<byte[]> result = clusterConnection.hashCommands().hGetDel(KEY_1_BYTES, KEY_2_BYTES, KEY_3_BYTES);

		assertThat(result).hasSize(2);
		assertThat(result.get(0)).isEqualTo(VALUE_1_BYTES);
		assertThat(result.get(1)).isEqualTo(VALUE_2_BYTES);

		assertThat(clusterConnection.hashCommands().hExists(KEY_1_BYTES, KEY_2_BYTES)).isFalse();
		assertThat(clusterConnection.hashCommands().hExists(KEY_1_BYTES, KEY_3_BYTES)).isFalse();
	}

	@Test // GH-3211
	@EnabledOnCommand("HGETEX")
	public void hGetExReturnsValueAndSetsExpiration() {

		nativeConnection.hset(KEY_1, KEY_2, VALUE_1);
		nativeConnection.hset(KEY_1, KEY_3, VALUE_2);

		List<byte[]> result = clusterConnection.hashCommands().hGetEx(KEY_1_BYTES, Expiration.seconds(60), KEY_2_BYTES);
		assertThat(result).hasSize(1);
		assertThat(result.get(0)).isEqualTo(VALUE_1_BYTES);
		assertThat(clusterConnection.hashCommands().hExists(KEY_1_BYTES, KEY_2_BYTES)).isTrue();
		assertThat(clusterConnection.hashCommands().hExists(KEY_1_BYTES, KEY_3_BYTES)).isTrue();
	}

	@Test // GH-3211
	@EnabledOnCommand("HGETEX")
	public void hGetExReturnsNullWhenFieldDoesNotExist() {

		nativeConnection.hset(KEY_1, KEY_2, VALUE_1);

		List<byte[]> result = clusterConnection.hashCommands().hGetEx(KEY_1_BYTES, Expiration.seconds(60), KEY_3_BYTES);
		assertThat(result).hasSize(1);
		assertThat(result.get(0)).isNull();
		assertThat(clusterConnection.hashCommands().hExists(KEY_1_BYTES, KEY_2_BYTES)).isTrue();
	}

	@Test // GH-3211
	@EnabledOnCommand("HGETEX")
	public void hGetExReturnsNullWhenKeyDoesNotExist() {

		List<byte[]> result = clusterConnection.hashCommands().hGetEx(KEY_1_BYTES, Expiration.seconds(60), KEY_2_BYTES);
		assertThat(result).hasSize(1);
		assertThat(result.get(0)).isNull();
	}

	@Test // GH-3211
	@EnabledOnCommand("HGETEX")
	public void hGetExMultipleFieldsReturnsValuesAndSetsExpiration() {

		nativeConnection.hset(KEY_1, KEY_2, VALUE_1);
		nativeConnection.hset(KEY_1, KEY_3, VALUE_2);
		nativeConnection.hset(KEY_1, "field3", "value3");

		List<byte[]> result = clusterConnection.hashCommands().hGetEx(KEY_1_BYTES, Expiration.seconds(120), KEY_2_BYTES, KEY_3_BYTES);

		assertThat(result).hasSize(2);
		assertThat(result.get(0)).isEqualTo(VALUE_1_BYTES);
		assertThat(result.get(1)).isEqualTo(VALUE_2_BYTES);
		assertThat(clusterConnection.hashCommands().hExists(KEY_1_BYTES, KEY_2_BYTES)).isTrue();
		assertThat(clusterConnection.hashCommands().hExists(KEY_1_BYTES, KEY_3_BYTES)).isTrue();
		assertThat(clusterConnection.hashCommands().hExists(KEY_1_BYTES, "field3".getBytes())).isTrue();
	}

	@Test // GH-3211
	@EnabledOnCommand("HGETEX")
	public void hGetExMultipleFieldsWithNonExistentFields() {

		nativeConnection.hset(KEY_1, KEY_2, VALUE_1);

		List<byte[]> result = clusterConnection.hashCommands().hGetEx(KEY_1_BYTES, Expiration.seconds(60), KEY_2_BYTES, KEY_3_BYTES);

		assertThat(result).hasSize(2);
		assertThat(result.get(0)).isEqualTo(VALUE_1_BYTES);
		assertThat(result.get(1)).isNull();
		assertThat(clusterConnection.hashCommands().hExists(KEY_1_BYTES, KEY_2_BYTES)).isTrue();
	}

	@Test // GH-3211
	@EnabledOnCommand("HSETEX")
	public void hSetExUpsertConditionSetsFieldsWithExpiration() {

		Map<byte[], byte[]> fieldMap = Map.of(KEY_2_BYTES, VALUE_1_BYTES, KEY_3_BYTES, VALUE_2_BYTES);
		Boolean result = clusterConnection.hashCommands().hSetEx(KEY_1_BYTES, fieldMap, RedisHashCommands.HashFieldSetOption.upsert(), Expiration.seconds(60));

		assertThat(result).isTrue();
		assertThat(clusterConnection.hashCommands().hExists(KEY_1_BYTES, KEY_2_BYTES)).isTrue();
		assertThat(clusterConnection.hashCommands().hExists(KEY_1_BYTES, KEY_3_BYTES)).isTrue();
		assertThat(clusterConnection.hashCommands().hGet(KEY_1_BYTES, KEY_2_BYTES)).isEqualTo(VALUE_1_BYTES);
		assertThat(clusterConnection.hashCommands().hGet(KEY_1_BYTES, KEY_3_BYTES)).isEqualTo(VALUE_2_BYTES);
	}

	@Test // GH-3211
	@EnabledOnCommand("HSETEX")
	public void hSetExIfNoneExistConditionSucceedsWhenNoFieldsExist() {

		Map<byte[], byte[]> fieldMap = Map.of(KEY_2_BYTES, VALUE_1_BYTES, KEY_3_BYTES, VALUE_2_BYTES);
		Boolean result = clusterConnection.hashCommands().hSetEx(KEY_1_BYTES, fieldMap, RedisHashCommands.HashFieldSetOption.ifNoneExist(), Expiration.seconds(60));

		assertThat(result).isTrue();
		assertThat(clusterConnection.hashCommands().hExists(KEY_1_BYTES, KEY_2_BYTES)).isTrue();
		assertThat(clusterConnection.hashCommands().hExists(KEY_1_BYTES, KEY_3_BYTES)).isTrue();
		assertThat(clusterConnection.hashCommands().hGet(KEY_1_BYTES, KEY_2_BYTES)).isEqualTo(VALUE_1_BYTES);
		assertThat(clusterConnection.hashCommands().hGet(KEY_1_BYTES, KEY_3_BYTES)).isEqualTo(VALUE_2_BYTES);
	}

	@Test // GH-3211
	@EnabledOnCommand("HSETEX")
	public void hSetExIfNoneExistConditionFailsWhenSomeFieldsExist() {

		nativeConnection.hset(KEY_1, KEY_2, VALUE_1);

		Map<byte[], byte[]> fieldMap = Map.of(KEY_2_BYTES, VALUE_2_BYTES, KEY_3_BYTES, VALUE_2_BYTES);
		Boolean result = clusterConnection.hashCommands().hSetEx(KEY_1_BYTES, fieldMap, RedisHashCommands.HashFieldSetOption.ifNoneExist(), Expiration.seconds(60));

		assertThat(result).isFalse();
		assertThat(clusterConnection.hashCommands().hGet(KEY_1_BYTES, KEY_2_BYTES)).isEqualTo(VALUE_1_BYTES); // unchanged
		assertThat(clusterConnection.hashCommands().hExists(KEY_1_BYTES, KEY_3_BYTES)).isFalse(); // not set
	}

	@Test // GH-3211
	@EnabledOnCommand("HSETEX")
	public void hSetExIfAllExistConditionSucceedsWhenAllFieldsExist() {

		nativeConnection.hset(KEY_1, KEY_2, VALUE_1);
		nativeConnection.hset(KEY_1, KEY_3, VALUE_2);

		Map<byte[], byte[]> fieldMap = Map.of(KEY_2_BYTES, "new-value-1".getBytes(), KEY_3_BYTES, "new-value-2".getBytes());
		Boolean result = clusterConnection.hashCommands().hSetEx(KEY_1_BYTES, fieldMap, RedisHashCommands.HashFieldSetOption.ifAllExist(), Expiration.seconds(60));

		assertThat(result).isTrue();
		assertThat(clusterConnection.hashCommands().hGet(KEY_1_BYTES, KEY_2_BYTES)).isEqualTo("new-value-1".getBytes()); // updated
		assertThat(clusterConnection.hashCommands().hGet(KEY_1_BYTES, KEY_3_BYTES)).isEqualTo("new-value-2".getBytes()); // updated
	}

	@Test // GH-3211
	@EnabledOnCommand("HSETEX")
	public void hSetExIfAllExistConditionFailsWhenSomeFieldsMissing() {

		nativeConnection.hset(KEY_1, KEY_2, VALUE_1);

		Map<byte[], byte[]> fieldMap = Map.of(KEY_2_BYTES, VALUE_2_BYTES, KEY_3_BYTES, VALUE_2_BYTES);
		Boolean result = clusterConnection.hashCommands().hSetEx(KEY_1_BYTES, fieldMap, RedisHashCommands.HashFieldSetOption.ifAllExist(), Expiration.seconds(60));

		assertThat(result).isFalse();
		assertThat(clusterConnection.hashCommands().hGet(KEY_1_BYTES, KEY_2_BYTES)).isEqualTo(VALUE_1_BYTES); // unchanged
		assertThat(clusterConnection.hashCommands().hExists(KEY_1_BYTES, KEY_3_BYTES)).isFalse(); // not set
	}

	@Test // DATAREDIS-315
	public void hValsShouldRetrieveValuesCorrectly() {

		nativeConnection.hset(KEY_1_BYTES, KEY_2_BYTES, VALUE_1_BYTES);
		nativeConnection.hset(KEY_1_BYTES, KEY_3_BYTES, VALUE_2_BYTES);

		assertThat(clusterConnection.hashCommands().hVals(KEY_1_BYTES)).contains(VALUE_1_BYTES, VALUE_2_BYTES);
	}

	@Test // DATAREDIS-315
	public void incrByFloatShouldIncreaseValueCorrectly() {

		nativeConnection.set(KEY_1, "1");

		assertThat(clusterConnection.incrBy(KEY_1_BYTES, 5.5D)).isEqualTo(6.5D);
	}

	@Test // DATAREDIS-315
	public void incrByShouldIncreaseValueCorrectly() {

		nativeConnection.set(KEY_1, "1");

		assertThat(clusterConnection.incrBy(KEY_1_BYTES, 5)).isEqualTo(6L);
	}

	@Test // DATAREDIS-315
	public void incrShouldIncreaseValueCorrectly() {

		nativeConnection.set(KEY_1, "1");

		assertThat(clusterConnection.incr(KEY_1_BYTES)).isEqualTo(2L);
	}

	@Test // DATAREDIS-315
	public void infoShouldCollectInfoForSpecificNode() {

		Properties properties = clusterConnection.info(new RedisClusterNode(CLUSTER_HOST, MASTER_NODE_2_PORT));

		assertThat(properties.getProperty("tcp_port")).isEqualTo(Integer.toString(MASTER_NODE_2_PORT));
	}

	@Test // DATAREDIS-315
	public void infoShouldCollectInfoForSpecificNodeAndSection() {

		Properties properties = clusterConnection.info(new RedisClusterNode(CLUSTER_HOST, MASTER_NODE_2_PORT), "server");

		assertThat(properties.getProperty("tcp_port")).isEqualTo(Integer.toString(MASTER_NODE_2_PORT));
		assertThat(properties.getProperty("used_memory")).isNull();
	}

	@Test // DATAREDIS-315, DATAREDIS-685
	public void infoShouldCollectionInfoFromAllClusterNodes() {

		Properties singleNodeInfo = clusterConnection.serverCommands().info(new RedisClusterNode("127.0.0.1", 7380));
		assertThat(Double.valueOf(clusterConnection.serverCommands().info().size())).isCloseTo(singleNodeInfo.size() * 3,
				offset(12d));
	}

	@Test // DATAREDIS-315
	public void keysShouldReturnAllKeys() {

		nativeConnection.set(KEY_1, VALUE_1);
		nativeConnection.set(KEY_2, VALUE_2);

		assertThat(clusterConnection.keys(JedisConverters.toBytes("*"))).contains(KEY_1_BYTES, KEY_2_BYTES);
	}

	@Test // DATAREDIS-315
	public void keysShouldReturnAllKeysForSpecificNode() {

		nativeConnection.set(KEY_1, VALUE_1);
		nativeConnection.set(KEY_2, VALUE_2);

		Set<byte[]> keysOnNode = clusterConnection.keys(new RedisClusterNode("127.0.0.1", 7379, SlotRange.empty()),
				JedisConverters.toBytes("*"));

		assertThat(keysOnNode).contains(KEY_2_BYTES).doesNotContain(KEY_1_BYTES);
	}

	@Test // DATAREDIS-635
	public void scanShouldReturnAllKeys() {

		nativeConnection.set(KEY_1, VALUE_1);
		nativeConnection.set(KEY_2, VALUE_2);

		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
				.isThrownBy(() -> clusterConnection.scan(ScanOptions.NONE));
	}

	@Override // DATAREDIS-635
	public void scanShouldReturnAllKeysForSpecificNode() {

		nativeConnection.set(KEY_1, VALUE_1);
		nativeConnection.set(KEY_2, VALUE_2);

		Cursor<byte[]> cursor = clusterConnection.scan(new RedisClusterNode("127.0.0.1", 7379, SlotRange.empty()), NONE);

		List<byte[]> keysOnNode = new ArrayList<>();
		cursor.forEachRemaining(keysOnNode::add);

		assertThat(keysOnNode).contains(KEY_2_BYTES).doesNotContain(KEY_1_BYTES);
	}

	@Test // DATAREDIS-315
	public void lIndexShouldGetElementAtIndexCorrectly() {

		nativeConnection.rpush(KEY_1, VALUE_1, VALUE_2, "foo", "bar");

		assertThat(clusterConnection.lIndex(KEY_1_BYTES, 1)).isEqualTo(VALUE_2_BYTES);
	}

	@Test // DATAREDIS-315
	public void lInsertShouldAddElementAtPositionCorrectly() {

		nativeConnection.rpush(KEY_1, VALUE_1, VALUE_2, "foo", "bar");

		clusterConnection.lInsert(KEY_1_BYTES, Position.AFTER, VALUE_2_BYTES, JedisConverters.toBytes("booh"));

		assertThat(nativeConnection.lrange(KEY_1, 0, -1).get(2)).isEqualTo("booh");
	}

	@Test // GH-2039
	@EnabledOnCommand("LMOVE")
	public void lMoveShouldMoveElementsCorrectly() {

		nativeConnection.rpush(SAME_SLOT_KEY_1, VALUE_1, VALUE_2, VALUE_3);

		assertThat(clusterConnection.lMove(SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES, Direction.RIGHT, Direction.LEFT))
				.isEqualTo(VALUE_3_BYTES);
		assertThat(clusterConnection.lMove(SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES, Direction.RIGHT, Direction.LEFT))
				.isEqualTo(VALUE_2_BYTES);

		assertThat(nativeConnection.lrange(SAME_SLOT_KEY_1, 0, -1)).containsExactly(VALUE_1);
		assertThat(nativeConnection.lrange(SAME_SLOT_KEY_2, 0, -1)).containsExactly(VALUE_2, VALUE_3);
	}

	@Test // GH-2039
	@EnabledOnCommand("BLMOVE")
	public void blMoveShouldMoveElementsCorrectly() {

		nativeConnection.rpush(SAME_SLOT_KEY_1, VALUE_2, VALUE_3);

		assertThat(clusterConnection.lMove(SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES, Direction.RIGHT, Direction.LEFT))
				.isEqualTo(VALUE_3_BYTES);
		assertThat(clusterConnection.lMove(SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES, Direction.RIGHT, Direction.LEFT))
				.isEqualTo(VALUE_2_BYTES);
		assertThat(
				clusterConnection.bLMove(SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES, Direction.RIGHT, Direction.LEFT, 0.01))
				.isNull();

		assertThat(nativeConnection.lrange(SAME_SLOT_KEY_1, 0, -1)).isEmpty();
		assertThat(nativeConnection.lrange(SAME_SLOT_KEY_2, 0, -1)).containsExactly(VALUE_2, VALUE_3);
	}

	@Test // DATAREDIS-315
	public void lLenShouldCountValuesCorrectly() {

		nativeConnection.lpush(KEY_1, VALUE_1, VALUE_2);

		assertThat(clusterConnection.lLen(KEY_1_BYTES)).isEqualTo(2L);
	}

	@Test // DATAREDIS-315
	public void lPopShouldReturnElementCorrectly() {

		nativeConnection.rpush(KEY_1, VALUE_1, VALUE_2);

		assertThat(clusterConnection.lPop(KEY_1_BYTES)).isEqualTo(VALUE_1_BYTES);
	}

	@Test // DATAREDIS-315
	public void lPushNXShouldNotAddValuesWhenKeyDoesNotExist() {

		clusterConnection.lPushX(KEY_1_BYTES, VALUE_1_BYTES);

		assertThat(nativeConnection.exists(KEY_1)).isFalse();
	}

	@Test // DATAREDIS-315
	public void lPushShouldAddValuesCorrectly() {

		clusterConnection.lPush(KEY_1_BYTES, VALUE_1_BYTES, VALUE_2_BYTES);

		assertThat(nativeConnection.lrange(KEY_1, 0, -1)).contains(VALUE_1, VALUE_2);
	}

	@Test // DATAREDIS-315
	public void lRangeShouldGetValuesCorrectly() {

		nativeConnection.lpush(KEY_1, VALUE_1, VALUE_2);

		assertThat(clusterConnection.lRange(KEY_1_BYTES, 0L, -1L)).contains(VALUE_1_BYTES, VALUE_2_BYTES);
	}

	@Test // DATAREDIS-315
	public void lRemShouldRemoveElementAtPositionCorrectly() {

		nativeConnection.rpush(KEY_1, VALUE_1, VALUE_2, "foo", "bar");

		clusterConnection.lRem(KEY_1_BYTES, 1L, VALUE_1_BYTES);

		assertThat(nativeConnection.llen(KEY_1)).isEqualTo(3L);
	}

	@Test // DATAREDIS-315
	public void lSetShouldSetElementAtPositionCorrectly() {

		nativeConnection.rpush(KEY_1, VALUE_1, VALUE_2, "foo", "bar");

		clusterConnection.lSet(KEY_1_BYTES, 1L, VALUE_1_BYTES);

		assertThat(nativeConnection.lrange(KEY_1, 0, -1).get(1)).isEqualTo(VALUE_1);
	}

	@Test // DATAREDIS-315
	public void lTrimShouldTrimListCorrectly() {

		nativeConnection.lpush(KEY_1, VALUE_1, VALUE_2, "foo", "bar");

		clusterConnection.lTrim(KEY_1_BYTES, 2, 3);

		assertThat(nativeConnection.lrange(KEY_1, 0, -1)).contains(VALUE_1, VALUE_2);
	}

	@Test // DATAREDIS-315
	public void mGetShouldReturnCorrectlyWhenKeysDoNotMapToSameSlot() {

		nativeConnection.set(KEY_1_BYTES, VALUE_1_BYTES);
		nativeConnection.set(KEY_2_BYTES, VALUE_2_BYTES);

		assertThat(clusterConnection.mGet(KEY_1_BYTES, KEY_2_BYTES)).containsExactly(VALUE_1_BYTES, VALUE_2_BYTES);
	}

	@Test // DATAREDIS-756
	public void mGetShouldReturnMultipleSameKeysWhenKeysDoNotMapToSameSlot() {

		nativeConnection.set(KEY_1_BYTES, VALUE_1_BYTES);
		nativeConnection.set(KEY_2_BYTES, VALUE_2_BYTES);
		nativeConnection.set(KEY_3_BYTES, VALUE_3_BYTES);

		List<byte[]> result = clusterConnection.mGet(KEY_1_BYTES, KEY_2_BYTES, KEY_3_BYTES, KEY_1_BYTES);
		assertThat(result).containsExactly(VALUE_1_BYTES, VALUE_2_BYTES, VALUE_3_BYTES, VALUE_1_BYTES);
	}

	@Test // DATAREDIS-315
	public void mGetShouldReturnCorrectlyWhenKeysMapToSameSlot() {

		nativeConnection.set(SAME_SLOT_KEY_1_BYTES, VALUE_1_BYTES);
		nativeConnection.set(SAME_SLOT_KEY_2_BYTES, VALUE_2_BYTES);

		assertThat(clusterConnection.mGet(SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES)).containsExactly(VALUE_1_BYTES,
				VALUE_2_BYTES);
	}

	@Test // DATAREDIS-756
	public void mGetShouldReturnMultipleSameKeysWhenKeysMapToSameSlot() {

		nativeConnection.set(SAME_SLOT_KEY_1_BYTES, VALUE_1_BYTES);
		nativeConnection.set(SAME_SLOT_KEY_2_BYTES, VALUE_2_BYTES);

		List<byte[]> result = clusterConnection.mGet(SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES, SAME_SLOT_KEY_1_BYTES);
		assertThat(result).containsExactly(VALUE_1_BYTES, VALUE_2_BYTES, VALUE_1_BYTES);
	}

	@Test // DATAREDIS-315
	public void mSetNXShouldReturnFalseIfNotAllKeysSet() {

		nativeConnection.set(KEY_2_BYTES, VALUE_3_BYTES);
		Map<byte[], byte[]> map = new LinkedHashMap<>();
		map.put(KEY_1_BYTES, VALUE_1_BYTES);
		map.put(KEY_2_BYTES, VALUE_2_BYTES);

		assertThat(clusterConnection.mSetNX(map)).isFalse();

		assertThat(nativeConnection.get(KEY_1)).isEqualTo(VALUE_1);
		assertThat(nativeConnection.get(KEY_2)).isEqualTo(VALUE_3);
	}

	@Test // DATAREDIS-315
	public void mSetNXShouldReturnTrueIfAllKeysSet() {

		Map<byte[], byte[]> map = new LinkedHashMap<>();
		map.put(KEY_1_BYTES, VALUE_1_BYTES);
		map.put(KEY_2_BYTES, VALUE_2_BYTES);

		assertThat(clusterConnection.mSetNX(map)).isTrue();

		assertThat(nativeConnection.get(KEY_1)).isEqualTo(VALUE_1);
		assertThat(nativeConnection.get(KEY_2)).isEqualTo(VALUE_2);
	}

	@Test // DATAREDIS-315
	public void mSetNXShouldWorkForOnSameSlotKeys() {

		Map<byte[], byte[]> map = new LinkedHashMap<>();
		map.put(SAME_SLOT_KEY_1_BYTES, VALUE_1_BYTES);
		map.put(SAME_SLOT_KEY_2_BYTES, VALUE_2_BYTES);

		assertThat(clusterConnection.mSetNX(map)).isTrue();

		assertThat(nativeConnection.get(SAME_SLOT_KEY_1)).isEqualTo(VALUE_1);
		assertThat(nativeConnection.get(SAME_SLOT_KEY_2)).isEqualTo(VALUE_2);
	}

	@Test // DATAREDIS-315
	public void mSetShouldWorkWhenKeysDoNotMapToSameSlot() {

		Map<byte[], byte[]> map = new LinkedHashMap<>();
		map.put(KEY_1_BYTES, VALUE_1_BYTES);
		map.put(KEY_2_BYTES, VALUE_2_BYTES);

		clusterConnection.mSet(map);

		assertThat(nativeConnection.get(KEY_1)).isEqualTo(VALUE_1);
		assertThat(nativeConnection.get(KEY_2)).isEqualTo(VALUE_2);
	}

	@Test // DATAREDIS-315
	public void mSetShouldWorkWhenKeysMapToSameSlot() {

		Map<byte[], byte[]> map = new LinkedHashMap<>();
		map.put(SAME_SLOT_KEY_1_BYTES, VALUE_1_BYTES);
		map.put(SAME_SLOT_KEY_2_BYTES, VALUE_2_BYTES);

		clusterConnection.mSet(map);

		assertThat(nativeConnection.get(SAME_SLOT_KEY_1)).isEqualTo(VALUE_1);
		assertThat(nativeConnection.get(SAME_SLOT_KEY_2)).isEqualTo(VALUE_2);
	}

	@Test // DATAREDIS-315
	public void moveShouldNotBeSupported() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
				.isThrownBy(() -> clusterConnection.move(KEY_1_BYTES, 3));
	}

	@Test // DATAREDIS-315
	public void multiShouldThrowException() {
		assertThatExceptionOfType(DataAccessException.class).isThrownBy(clusterConnection::multi);
	}

	@Test // DATAREDIS-315
	public void pExpireAtShouldBeSetCorrectly() {

		nativeConnection.set(KEY_1, VALUE_1);

		clusterConnection.pExpireAt(KEY_1_BYTES, System.currentTimeMillis() + 5000);

		assertThat(nativeConnection.ttl(JedisConverters.toString(KEY_1_BYTES))).isGreaterThan(1);
	}

	@Test // GH-3114
	@EnabledOnCommand("SPUBLISH") // Redis 7.0
	public void pExpireAtWithConditionShouldBeSetCorrectly() {

		nativeConnection.set(KEY_1, VALUE_1);

		assertThat(
				clusterConnection.pExpireAt(KEY_1_BYTES, System.currentTimeMillis() + 5000, ExpirationOptions.Condition.XX))
				.isFalse();
		assertThat(
				clusterConnection.pExpireAt(KEY_1_BYTES, System.currentTimeMillis() + 5000, ExpirationOptions.Condition.NX))
				.isTrue();
		assertThat(
				clusterConnection.pExpireAt(KEY_1_BYTES, System.currentTimeMillis() + 15000, ExpirationOptions.Condition.LT))
				.isFalse();

		assertThat(nativeConnection.ttl(JedisConverters.toString(KEY_1_BYTES))).isGreaterThan(1);
	}

	@Test // DATAREDIS-315
	public void pExpireShouldBeSetCorrectly() {

		nativeConnection.set(KEY_1, VALUE_1);

		clusterConnection.pExpire(KEY_1_BYTES, 5000);

		assertThat(nativeConnection.ttl(JedisConverters.toString(KEY_1_BYTES))).isGreaterThan(1);
	}

	@Test // GH-3114
	@EnabledOnCommand("SPUBLISH") // Redis 7.0
	public void pExpireWithConditionShouldBeSetCorrectly() {

		nativeConnection.set(KEY_1, VALUE_1);

		assertThat(clusterConnection.pExpire(KEY_1_BYTES, 15000, ExpirationOptions.Condition.XX)).isFalse();
		assertThat(clusterConnection.pExpire(KEY_1_BYTES, 15000, ExpirationOptions.Condition.NX)).isTrue();
		assertThat(clusterConnection.pExpire(KEY_1_BYTES, 15000, ExpirationOptions.Condition.LT)).isFalse();

		assertThat(nativeConnection.ttl(JedisConverters.toString(KEY_1_BYTES))).isGreaterThan(1);
	}

	@Test // DATAREDIS-315
	public void pSetExShouldSetValueCorrectly() {

		clusterConnection.pSetEx(KEY_1_BYTES, 5000, VALUE_1_BYTES);

		assertThat(nativeConnection.get(KEY_1)).isEqualTo(VALUE_1);
		assertThat(nativeConnection.ttl(KEY_1)).isGreaterThan(1);
	}

	@Test // DATAREDIS-315
	public void pTtlShouldReturValueCorrectly() {

		nativeConnection.set(KEY_1, VALUE_1);
		nativeConnection.expire(KEY_1, 5);

		assertThat(clusterConnection.pTtl(KEY_1_BYTES)).isGreaterThan(1);
	}

	@Test // DATAREDIS-315
	public void pTtlShouldReturnMinusOneWhenKeyDoesNotHaveExpirationSet() {

		nativeConnection.set(KEY_1, VALUE_1);

		assertThat(clusterConnection.pTtl(KEY_1_BYTES)).isEqualTo(-1L);
	}

	@Test // DATAREDIS-315
	public void pTtlShouldReturnMinusTwoWhenKeyDoesNotExist() {
		assertThat(clusterConnection.pTtl(KEY_1_BYTES)).isEqualTo(-2L);
	}

	@Test // DATAREDIS-526
	public void pTtlWithTimeUnitShouldReturnMinusTwoWhenKeyDoesNotExist() {
		assertThat(clusterConnection.pTtl(KEY_1_BYTES, TimeUnit.HOURS)).isEqualTo(-2L);
	}

	@Test // DATAREDIS-315
	public void persistShouldRemoveTTL() {

		nativeConnection.setex(KEY_1_BYTES, 10, VALUE_1_BYTES);

		assertThat(clusterConnection.persist(KEY_1_BYTES)).isTrue();
		assertThat(nativeConnection.ttl(KEY_1_BYTES)).isEqualTo(-1L);
	}

	@Test // DATAREDIS-315
	public void pfAddShouldAddValuesCorrectly() {

		clusterConnection.pfAdd(KEY_1_BYTES, VALUE_1_BYTES, VALUE_2_BYTES, VALUE_3_BYTES);

		assertThat(nativeConnection.pfcount(KEY_1_BYTES)).isEqualTo(3L);
	}

	@Test // DATAREDIS-315
	public void pfCountShouldAllowCountingOnSameSlotKeys() {

		nativeConnection.pfadd(SAME_SLOT_KEY_1, VALUE_1, VALUE_2);
		nativeConnection.pfadd(SAME_SLOT_KEY_2, VALUE_2, VALUE_3);

		assertThat(clusterConnection.pfCount(SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES)).isEqualTo(3L);
	}

	@Test // DATAREDIS-315
	public void pfCountShouldAllowCountingOnSingleKey() {

		nativeConnection.pfadd(KEY_1, VALUE_1, VALUE_2, VALUE_3);

		assertThat(clusterConnection.pfCount(KEY_1_BYTES)).isEqualTo(3L);
	}

	@Test // DATAREDIS-315
	public void pfCountShouldThrowErrorCountingOnDifferentSlotKeys() {

		nativeConnection.pfadd(KEY_1, VALUE_1, VALUE_2);
		nativeConnection.pfadd(KEY_2, VALUE_2, VALUE_3);

		assertThatExceptionOfType(DataAccessException.class)
				.isThrownBy(() -> clusterConnection.pfCount(KEY_1_BYTES, KEY_2_BYTES));
	}

	@Test // DATAREDIS-315
	public void pfMergeShouldThrowErrorOnDifferentSlotKeys() {
		assertThatExceptionOfType(DataAccessException.class)
				.isThrownBy(() -> clusterConnection.pfMerge(KEY_3_BYTES, KEY_1_BYTES, KEY_2_BYTES));
	}

	@Test // DATAREDIS-315
	public void pfMergeShouldWorkWhenAllKeysMapToSameSlot() {

		nativeConnection.pfadd(SAME_SLOT_KEY_1, VALUE_1, VALUE_2);
		nativeConnection.pfadd(SAME_SLOT_KEY_2, VALUE_2, VALUE_3);

		clusterConnection.pfMerge(SAME_SLOT_KEY_3_BYTES, SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES);

		assertThat(nativeConnection.pfcount(SAME_SLOT_KEY_3)).isEqualTo(3L);
	}

	@Test // DATAREDIS-315
	public void pingShouldRetrunPong() {
		assertThat(clusterConnection.ping()).isEqualTo("PONG");
	}

	@Test // DATAREDIS-315
	public void pingShouldRetrunPongForExistingNode() {
		assertThat(clusterConnection.ping(new RedisClusterNode("127.0.0.1", 7379, SlotRange.empty()))).isEqualTo("PONG");
	}

	@Test // DATAREDIS-315
	public void pingShouldThrowExceptionWhenNodeNotKnownToCluster() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> clusterConnection.ping(new RedisClusterNode("127.0.0.1", 1234, null)));
	}

	@Test // DATAREDIS-315
	public void rPopLPushShouldWorkWhenDoNotMapToSameSlot() {

		nativeConnection.lpush(KEY_1_BYTES, VALUE_1_BYTES, VALUE_2_BYTES);

		assertThat(clusterConnection.rPopLPush(KEY_1_BYTES, KEY_2_BYTES)).isEqualTo(VALUE_1_BYTES);
		assertThat(nativeConnection.exists(KEY_2_BYTES)).isTrue();
	}

	@Test // DATAREDIS-315
	public void rPopLPushShouldWorkWhenKeysOnSameSlot() {

		nativeConnection.lpush(SAME_SLOT_KEY_1_BYTES, VALUE_1_BYTES, VALUE_2_BYTES);

		assertThat(clusterConnection.rPopLPush(SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES)).isEqualTo(VALUE_1_BYTES);
		assertThat(nativeConnection.exists(SAME_SLOT_KEY_2_BYTES)).isTrue();
	}

	@Test // DATAREDIS-315
	public void rPopShouldReturnElementCorrectly() {

		nativeConnection.rpush(KEY_1, VALUE_1, VALUE_2);

		assertThat(clusterConnection.rPop(KEY_1_BYTES)).isEqualTo(VALUE_2_BYTES);
	}

	@Test // DATAREDIS-315
	public void rPushNXShouldNotAddValuesWhenKeyDoesNotExist() {

		clusterConnection.rPushX(KEY_1_BYTES, VALUE_1_BYTES);

		assertThat(nativeConnection.exists(KEY_1)).isFalse();
	}

	@Test // DATAREDIS-315
	public void rPushShouldAddValuesCorrectly() {

		clusterConnection.rPush(KEY_1_BYTES, VALUE_1_BYTES, VALUE_2_BYTES);

		assertThat(nativeConnection.lrange(KEY_1, 0, -1)).contains(VALUE_1, VALUE_2);
	}

	@Test // DATAREDIS-315
	public void randomKeyShouldReturnCorrectlyWhenKeysAvailable() {

		nativeConnection.set(KEY_1, VALUE_1);
		nativeConnection.set(KEY_2, VALUE_2);

		assertThat(clusterConnection.randomKey()).isNotNull();
	}

	@Test // DATAREDIS-315
	public void randomKeyShouldReturnNullWhenNoKeysAvailable() {
		assertThat(clusterConnection.randomKey()).isNull();
	}

	@Test // DATAREDIS-315
	public void rename() {

		nativeConnection.set(KEY_1_BYTES, VALUE_1_BYTES);

		clusterConnection.rename(KEY_1_BYTES, KEY_2_BYTES);

		assertThat(nativeConnection.exists(KEY_1_BYTES)).isFalse();
		assertThat(nativeConnection.get(KEY_2_BYTES)).isEqualTo(VALUE_1_BYTES);
	}

	@Test // DATAREDIS-1190
	public void renameShouldOverwriteTargetKey() {

		nativeConnection.set(KEY_1_BYTES, VALUE_1_BYTES);
		nativeConnection.set(KEY_2_BYTES, VALUE_2_BYTES);

		clusterConnection.rename(KEY_1_BYTES, KEY_2_BYTES);

		assertThat(nativeConnection.exists(KEY_1_BYTES)).isFalse();
		assertThat(nativeConnection.get(KEY_2_BYTES)).isEqualTo(VALUE_1_BYTES);
	}

	@Test // DATAREDIS-315
	public void renameNXWhenOnSameSlot() {

		nativeConnection.set(SAME_SLOT_KEY_1_BYTES, VALUE_1_BYTES);

		assertThat(clusterConnection.renameNX(SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES)).isTrue();

		assertThat(nativeConnection.exists(SAME_SLOT_KEY_1_BYTES)).isFalse();
		assertThat(nativeConnection.get(SAME_SLOT_KEY_2_BYTES)).isEqualTo(VALUE_1_BYTES);
	}

	@Test // DATAREDIS-315
	public void renameNXWhenTargetKeyDoesExist() {

		nativeConnection.set(KEY_1_BYTES, VALUE_1_BYTES);
		nativeConnection.set(KEY_2_BYTES, VALUE_2_BYTES);

		assertThat(clusterConnection.renameNX(KEY_1_BYTES, KEY_2_BYTES)).isFalse();

		assertThat(nativeConnection.get(KEY_1_BYTES)).isEqualTo(VALUE_1_BYTES);
		assertThat(nativeConnection.get(KEY_2_BYTES)).isEqualTo(VALUE_2_BYTES);
	}

	@Test // DATAREDIS-315
	public void renameNXWhenTargetKeyDoesNotExist() {

		nativeConnection.set(KEY_1_BYTES, VALUE_1_BYTES);

		assertThat(clusterConnection.renameNX(KEY_1_BYTES, KEY_2_BYTES)).isTrue();

		assertThat(nativeConnection.exists(KEY_1_BYTES)).isFalse();
		assertThat(nativeConnection.get(KEY_2_BYTES)).isEqualTo(VALUE_1_BYTES);
	}

	@Test // DATAREDIS-315
	public void renameSameKeysOnSameSlot() {

		nativeConnection.set(SAME_SLOT_KEY_1_BYTES, VALUE_1_BYTES);

		clusterConnection.rename(SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES);

		assertThat(nativeConnection.exists(SAME_SLOT_KEY_1_BYTES)).isFalse();
		assertThat(nativeConnection.get(SAME_SLOT_KEY_2_BYTES)).isEqualTo(VALUE_1_BYTES);
	}

	@Test // DATAREDIS-315
	public void sAddShouldAddValueToSetCorrectly() {

		clusterConnection.sAdd(KEY_1_BYTES, VALUE_1_BYTES, VALUE_2_BYTES);

		assertThat(nativeConnection.smembers(KEY_1)).contains(VALUE_1, VALUE_2);
	}

	@Test // DATAREDIS-315
	public void sCardShouldCountValuesInSetCorrectly() {

		nativeConnection.sadd(KEY_1, VALUE_1, VALUE_2);

		assertThat(clusterConnection.sCard(KEY_1_BYTES)).isEqualTo(2L);
	}

	@Test // DATAREDIS-315
	public void sDiffShouldWorkWhenKeysMapToSameSlot() {

		nativeConnection.sadd(SAME_SLOT_KEY_1_BYTES, VALUE_1_BYTES, VALUE_2_BYTES);
		nativeConnection.sadd(SAME_SLOT_KEY_2_BYTES, VALUE_2_BYTES, VALUE_3_BYTES);

		assertThat(clusterConnection.sDiff(SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES)).contains(VALUE_1_BYTES);
	}

	@Test // DATAREDIS-315, DATAREDIS-647
	public void sDiffShouldWorkWhenKeysNotMapToSameSlot() {

		nativeConnection.sadd(KEY_1_BYTES, VALUE_1_BYTES, VALUE_2_BYTES);
		nativeConnection.sadd(KEY_2_BYTES, VALUE_2_BYTES, VALUE_3_BYTES);
		nativeConnection.sadd(KEY_3_BYTES, VALUE_1_BYTES, VALUE_3_BYTES);

		assertThat(clusterConnection.sDiff(KEY_1_BYTES, KEY_2_BYTES)).contains(VALUE_1_BYTES);
		assertThat(clusterConnection.sDiff(KEY_1_BYTES, KEY_2_BYTES, KEY_3_BYTES)).isEmpty();
	}

	@Test // DATAREDIS-315
	public void sDiffStoreShouldWorkWhenKeysMapToSameSlot() {

		nativeConnection.sadd(SAME_SLOT_KEY_1_BYTES, VALUE_1_BYTES, VALUE_2_BYTES);
		nativeConnection.sadd(SAME_SLOT_KEY_2_BYTES, VALUE_2_BYTES, VALUE_3_BYTES);

		clusterConnection.sDiffStore(SAME_SLOT_KEY_3_BYTES, SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES);

		assertThat(nativeConnection.smembers(SAME_SLOT_KEY_3_BYTES)).contains(VALUE_1_BYTES);
	}

	@Test // DATAREDIS-315
	public void sDiffStoreShouldWorkWhenKeysNotMapToSameSlot() {

		nativeConnection.sadd(KEY_1_BYTES, VALUE_1_BYTES, VALUE_2_BYTES);
		nativeConnection.sadd(KEY_2_BYTES, VALUE_2_BYTES, VALUE_3_BYTES);

		clusterConnection.sDiffStore(KEY_3_BYTES, KEY_1_BYTES, KEY_2_BYTES);

		assertThat(nativeConnection.smembers(KEY_3_BYTES)).contains(VALUE_1_BYTES);
	}

	@Test // DATAREDIS-315
	public void sInterShouldWorkForKeysMappingToSameSlot() {

		nativeConnection.sadd(SAME_SLOT_KEY_1_BYTES, VALUE_1_BYTES, VALUE_2_BYTES);
		nativeConnection.sadd(SAME_SLOT_KEY_2_BYTES, VALUE_2_BYTES, VALUE_3_BYTES);

		assertThat(clusterConnection.sInter(SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES)).contains(VALUE_2_BYTES);
	}

	@Test // DATAREDIS-315
	public void sInterShouldWorkForKeysNotMappingToSameSlot() {

		nativeConnection.sadd(KEY_1_BYTES, VALUE_1_BYTES, VALUE_2_BYTES);
		nativeConnection.sadd(KEY_2_BYTES, VALUE_2_BYTES, VALUE_3_BYTES);

		assertThat(clusterConnection.sInter(KEY_1_BYTES, KEY_2_BYTES)).contains(VALUE_2_BYTES);
	}

	@Test // DATAREDIS-315
	public void sInterStoreShouldWorkForKeysMappingToSameSlot() {

		nativeConnection.sadd(SAME_SLOT_KEY_1_BYTES, VALUE_1_BYTES, VALUE_2_BYTES);
		nativeConnection.sadd(SAME_SLOT_KEY_2_BYTES, VALUE_2_BYTES, VALUE_3_BYTES);

		clusterConnection.sInterStore(SAME_SLOT_KEY_3_BYTES, SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES);

		assertThat(nativeConnection.smembers(SAME_SLOT_KEY_3_BYTES)).contains(VALUE_2_BYTES);
	}

	@Test // DATAREDIS-315
	public void sInterStoreShouldWorkForKeysNotMappingToSameSlot() {

		nativeConnection.sadd(KEY_1_BYTES, VALUE_1_BYTES, VALUE_2_BYTES);
		nativeConnection.sadd(KEY_2_BYTES, VALUE_2_BYTES, VALUE_3_BYTES);

		clusterConnection.sInterStore(KEY_3_BYTES, KEY_1_BYTES, KEY_2_BYTES);

		assertThat(nativeConnection.smembers(KEY_3_BYTES)).contains(VALUE_2_BYTES);
	}

	@Test // DATAREDIS-315
	public void sIsMemberShouldReturnFalseIfValueIsMemberOfSet() {

		nativeConnection.sadd(KEY_1, VALUE_1, VALUE_2);

		assertThat(clusterConnection.sIsMember(KEY_1_BYTES, JedisConverters.toBytes("foo"))).isFalse();
	}

	@Test // DATAREDIS-315
	public void sIsMemberShouldReturnTrueIfValueIsMemberOfSet() {

		nativeConnection.sadd(KEY_1, VALUE_1, VALUE_2);

		assertThat(clusterConnection.sIsMember(KEY_1_BYTES, VALUE_1_BYTES)).isTrue();
	}

	@Test // GH-2037
	@EnabledOnCommand("SMISMEMBER")
	public void sMIsMemberShouldReturnCorrectValues() {

		nativeConnection.sadd(KEY_1, VALUE_1, VALUE_2);

		assertThat(clusterConnection.sMIsMember(KEY_1_BYTES, VALUE_1_BYTES, VALUE_2_BYTES, VALUE_3_BYTES))
				.containsExactly(true, true, false);
	}

	@Test // DATAREDIS-315
	public void sMembersShouldReturnValuesContainedInSetCorrectly() {

		nativeConnection.sadd(KEY_1, VALUE_1, VALUE_2);

		assertThat(clusterConnection.sMembers(KEY_1_BYTES)).contains(VALUE_1_BYTES, VALUE_2_BYTES);
	}

	@Test // DATAREDIS-315
	public void sMoveShouldWorkWhenKeysDoNotMapToSameSlot() {

		nativeConnection.sadd(KEY_1_BYTES, VALUE_1_BYTES, VALUE_2_BYTES);
		nativeConnection.sadd(KEY_2_BYTES, VALUE_3_BYTES);

		clusterConnection.sMove(KEY_1_BYTES, KEY_2_BYTES, VALUE_2_BYTES);

		assertThat(nativeConnection.sismember(KEY_1_BYTES, VALUE_2_BYTES)).isFalse();
		assertThat(nativeConnection.sismember(KEY_2_BYTES, VALUE_2_BYTES)).isTrue();
	}

	@Test // DATAREDIS-315
	public void sMoveShouldWorkWhenKeysMapToSameSlot() {

		nativeConnection.sadd(SAME_SLOT_KEY_1_BYTES, VALUE_1_BYTES, VALUE_2_BYTES);
		nativeConnection.sadd(SAME_SLOT_KEY_2_BYTES, VALUE_3_BYTES);

		clusterConnection.sMove(SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES, VALUE_2_BYTES);

		assertThat(nativeConnection.sismember(SAME_SLOT_KEY_1_BYTES, VALUE_2_BYTES)).isFalse();
		assertThat(nativeConnection.sismember(SAME_SLOT_KEY_2_BYTES, VALUE_2_BYTES)).isTrue();
	}

	@Test // DATAREDIS-315
	public void sPopShouldPopValueFromSetCorrectly() {

		nativeConnection.sadd(KEY_1, VALUE_1, VALUE_2);

		assertThat(clusterConnection.sPop(KEY_1_BYTES)).isNotNull();
	}

	@Test // DATAREDIS-668
	void sPopWithCountShouldPopValueFromSetCorrectly() {

		nativeConnection.sadd(KEY_1, VALUE_1, VALUE_2, VALUE_3);

		assertThat(clusterConnection.setCommands().sPop(KEY_1_BYTES, 2)).hasSize(2);
		assertThat(nativeConnection.scard(KEY_1)).isEqualTo(1L);
	}

	@Test // DATAREDIS-315
	public void sRandMamberShouldReturnValueCorrectly() {

		nativeConnection.sadd(KEY_1, VALUE_1, VALUE_2);

		assertThat(clusterConnection.sRandMember(KEY_1_BYTES)).isNotNull();
	}

	@Test // DATAREDIS-315
	public void sRandMamberWithCountShouldReturnValueCorrectly() {

		nativeConnection.sadd(KEY_1, VALUE_1, VALUE_2);

		assertThat(clusterConnection.sRandMember(KEY_1_BYTES, 3)).isNotNull();
	}

	@Test // DATAREDIS-315
	public void sRemShouldRemoveValueFromSetCorrectly() {

		nativeConnection.sadd(KEY_1, VALUE_1, VALUE_2);

		clusterConnection.sRem(KEY_1_BYTES, VALUE_2_BYTES);

		assertThat(nativeConnection.smembers(KEY_1)).contains(VALUE_1);
	}

	@Test // DATAREDIS-315
	public void sUnionShouldWorkForKeysMappingToSameSlot() {

		nativeConnection.sadd(SAME_SLOT_KEY_1_BYTES, VALUE_1_BYTES, VALUE_2_BYTES);
		nativeConnection.sadd(SAME_SLOT_KEY_2_BYTES, VALUE_2_BYTES, VALUE_3_BYTES);

		assertThat(clusterConnection.sUnion(SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES)).contains(VALUE_1_BYTES,
				VALUE_2_BYTES, VALUE_3_BYTES);
	}

	@Test // DATAREDIS-315
	public void sUnionShouldWorkForKeysNotMappingToSameSlot() {

		nativeConnection.sadd(KEY_1_BYTES, VALUE_1_BYTES, VALUE_2_BYTES);
		nativeConnection.sadd(KEY_2_BYTES, VALUE_2_BYTES, VALUE_3_BYTES);

		assertThat(clusterConnection.sUnion(KEY_1_BYTES, KEY_2_BYTES)).contains(VALUE_1_BYTES, VALUE_2_BYTES,
				VALUE_3_BYTES);
	}

	@Test // DATAREDIS-315
	public void sUnionStoreShouldWorkForKeysMappingToSameSlot() {

		nativeConnection.sadd(SAME_SLOT_KEY_1_BYTES, VALUE_1_BYTES, VALUE_2_BYTES);
		nativeConnection.sadd(SAME_SLOT_KEY_2_BYTES, VALUE_2_BYTES, VALUE_3_BYTES);

		clusterConnection.sUnionStore(SAME_SLOT_KEY_3_BYTES, SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES);

		assertThat(nativeConnection.smembers(SAME_SLOT_KEY_3_BYTES)).contains(VALUE_1_BYTES, VALUE_2_BYTES, VALUE_3_BYTES);
	}

	@Test // DATAREDIS-315
	public void sUnionStoreShouldWorkForKeysNotMappingToSameSlot() {

		nativeConnection.sadd(KEY_1_BYTES, VALUE_1_BYTES, VALUE_2_BYTES);
		nativeConnection.sadd(KEY_2_BYTES, VALUE_2_BYTES, VALUE_3_BYTES);

		clusterConnection.sUnionStore(KEY_3_BYTES, KEY_1_BYTES, KEY_2_BYTES);

		assertThat(nativeConnection.smembers(KEY_3_BYTES)).contains(VALUE_1_BYTES, VALUE_2_BYTES, VALUE_3_BYTES);
	}

	@Test // DATAREDIS-315
	public void selectShouldAllowSelectionOfDBIndexZero() {
		clusterConnection.select(0);
	}

	@Test // DATAREDIS-315
	public void selectShouldThrowExceptionWhenSelectingNonZeroDbIndex() {
		assertThatExceptionOfType(DataAccessException.class).isThrownBy(() -> clusterConnection.select(1));
	}

	@Test // DATAREDIS-315
	public void setBitShouldWorkCorrectly() {

		clusterConnection.setBit(KEY_1_BYTES, 0, true);
		clusterConnection.setBit(KEY_1_BYTES, 1, false);

		assertThat(nativeConnection.getbit(KEY_1, 0)).isTrue();
		assertThat(nativeConnection.getbit(KEY_1, 1)).isFalse();
	}

	@Test // DATAREDIS-315
	public void setExShouldSetValueCorrectly() {

		clusterConnection.setEx(KEY_1_BYTES, 5, VALUE_1_BYTES);

		assertThat(nativeConnection.get(KEY_1)).isEqualTo(VALUE_1);
		assertThat(nativeConnection.ttl(KEY_1)).isGreaterThan(1);
	}

	@Test // DATAREDIS-315
	public void setNxShouldNotSetValueWhenAlreadyExistsInDBCorrectly() {

		nativeConnection.set(KEY_1, VALUE_1);

		clusterConnection.setNX(KEY_1_BYTES, VALUE_2_BYTES);

		assertThat(nativeConnection.get(KEY_1)).isEqualTo(VALUE_1);
	}

	@Test // DATAREDIS-315
	public void setNxShouldSetValueCorrectly() {

		clusterConnection.setNX(KEY_1_BYTES, VALUE_1_BYTES);

		assertThat(nativeConnection.get(KEY_1)).isEqualTo(VALUE_1);
	}

	@Test // DATAREDIS-315
	public void setRangeShouldWorkCorrectly() {

		nativeConnection.set(KEY_1, VALUE_1);

		clusterConnection.setRange(KEY_1_BYTES, JedisConverters.toBytes("UE"), 3);

		assertThat(nativeConnection.get(KEY_1)).isEqualTo("valUE1");
	}

	@Test // DATAREDIS-315
	public void setShouldSetValueCorrectly() {

		clusterConnection.set(KEY_1_BYTES, VALUE_1_BYTES);

		assertThat(nativeConnection.get(KEY_1)).isEqualTo(VALUE_1);
	}

	@Test // DATAREDIS-316
	public void setWithExpirationAndIfAbsentShouldNotBeAppliedWhenKeyExists() {

		nativeConnection.set(KEY_1_BYTES, VALUE_1_BYTES);

		clusterConnection.set(KEY_1_BYTES, VALUE_2_BYTES, Expiration.seconds(1), SetOption.ifAbsent());

		assertThat(nativeConnection.exists(KEY_1_BYTES)).isTrue();
		assertThat(nativeConnection.ttl(KEY_1_BYTES)).isEqualTo(-1L);
		assertThat(nativeConnection.get(KEY_1_BYTES)).isEqualTo(VALUE_1_BYTES);
	}

	@Test // DATAREDIS-316
	public void setWithExpirationAndIfAbsentShouldWorkCorrectly() {

		clusterConnection.set(KEY_1_BYTES, VALUE_1_BYTES, Expiration.seconds(1), SetOption.ifAbsent());

		assertThat(nativeConnection.exists(KEY_1_BYTES)).isTrue();
		assertThat(nativeConnection.ttl(KEY_1_BYTES)).isEqualTo(1L);
	}

	@Test // DATAREDIS-316
	public void setWithExpirationAndIfPresentShouldNotBeAppliedWhenKeyDoesNotExists() {

		clusterConnection.set(KEY_1_BYTES, VALUE_1_BYTES, Expiration.seconds(1), SetOption.ifPresent());

		assertThat(nativeConnection.exists(KEY_1_BYTES)).isFalse();
	}

	@Test // DATAREDIS-316
	public void setWithExpirationAndIfPresentShouldWorkCorrectly() {

		nativeConnection.set(KEY_1_BYTES, VALUE_1_BYTES);

		clusterConnection.set(KEY_1_BYTES, VALUE_2_BYTES, Expiration.seconds(1), SetOption.ifPresent());

		assertThat(nativeConnection.exists(KEY_1_BYTES)).isTrue();
		assertThat(nativeConnection.ttl(KEY_1_BYTES)).isEqualTo(1L);
		assertThat(nativeConnection.get(KEY_1_BYTES)).isEqualTo(VALUE_2_BYTES);
	}

	@Test // DATAREDIS-316
	public void setWithExpirationInMillisecondsShouldWorkCorrectly() {

		clusterConnection.set(KEY_1_BYTES, VALUE_1_BYTES, Expiration.milliseconds(500), SetOption.upsert());

		assertThat(nativeConnection.exists(KEY_1_BYTES)).isTrue();
		assertThat(nativeConnection.pttl(KEY_1)).isCloseTo(500L, offset(499L));
	}

	@Test // DATAREDIS-316
	public void setWithExpirationInSecondsShouldWorkCorrectly() {

		clusterConnection.set(KEY_1_BYTES, VALUE_1_BYTES, Expiration.seconds(1), SetOption.upsert());

		assertThat(nativeConnection.exists(KEY_1_BYTES)).isTrue();
		assertThat(nativeConnection.ttl(KEY_1_BYTES)).isEqualTo(1L);
	}

	@Test // DATAREDIS-316
	public void setWithOptionIfAbsentShouldWorkCorrectly() {

		clusterConnection.set(KEY_1_BYTES, VALUE_1_BYTES, Expiration.persistent(), SetOption.ifAbsent());

		assertThat(nativeConnection.exists(KEY_1_BYTES)).isTrue();
		assertThat(nativeConnection.ttl(KEY_1_BYTES)).isEqualTo(-1L);
	}

	@Test // DATAREDIS-316, DATAREDIS-588
	public void setWithOptionIfPresentShouldWorkCorrectly() {

		nativeConnection.set(KEY_1_BYTES, VALUE_1_BYTES);
		clusterConnection.set(KEY_1_BYTES, VALUE_2_BYTES, Expiration.persistent(), SetOption.ifPresent());

		assertThat(nativeConnection.exists(KEY_1_BYTES)).isTrue();
		assertThat(clusterConnection.get(KEY_1_BYTES)).isEqualTo(VALUE_2_BYTES);
		assertThat(nativeConnection.ttl(KEY_1_BYTES)).isEqualTo(-1L);
	}

	@Test // DATAREDIS-315
	public void shouldAllowSettingAndGettingValues() {

		clusterConnection.set(KEY_1_BYTES, VALUE_1_BYTES);
		assertThat(clusterConnection.get(KEY_1_BYTES)).isEqualTo(VALUE_1_BYTES);
	}

	@Test // DATAREDIS-315
	public void sortAndStoreShouldAddSortedValuesValuesCorrectly() {

		nativeConnection.lpush(KEY_1, VALUE_2, VALUE_1);

		assertThat(clusterConnection.sort(KEY_1_BYTES, new DefaultSortParameters().alpha(), KEY_2_BYTES)).isEqualTo(2L);
		assertThat(nativeConnection.exists(KEY_2_BYTES)).isTrue();
	}

	@Test // DATAREDIS-315, GH-2341
	public void sortAndStoreShouldReplaceDestinationList() {

		nativeConnection.lpush(KEY_1, VALUE_2, VALUE_1);
		nativeConnection.lpush(KEY_2_BYTES, VALUE_3_BYTES);

		assertThat(clusterConnection.sort(KEY_1_BYTES, new DefaultSortParameters().alpha(), KEY_2_BYTES)).isEqualTo(2L);
		assertThat(nativeConnection.llen(KEY_2_BYTES)).isEqualTo(2);
	}

	@Test // DATAREDIS-315
	public void sortShouldReturnValuesCorrectly() {

		nativeConnection.lpush(KEY_1, VALUE_2, VALUE_1);

		assertThat(clusterConnection.sort(KEY_1_BYTES, new DefaultSortParameters().alpha())).contains(VALUE_1_BYTES,
				VALUE_2_BYTES);
	}

	@Test // DATAREDIS-315
	public void sscanShouldRetrieveAllValuesInSetCorrectly() {

		for (int i = 0; i < 30; i++) {
			nativeConnection.sadd(KEY_1_BYTES, JedisConverters.toBytes(Integer.valueOf(i)));
		}

		int count = 0;
		Cursor<byte[]> cursor = clusterConnection.sScan(KEY_1_BYTES, ScanOptions.NONE);
		while (cursor.hasNext()) {
			count++;
			cursor.next();
		}

		assertThat(count).isEqualTo(30);
	}

	@Test // DATAREDIS-315
	public void strLenShouldWorkCorrectly() {

		nativeConnection.set(KEY_1, VALUE_1);

		assertThat(clusterConnection.strLen(KEY_1_BYTES)).isEqualTo(6L);
	}

	@Test // DATAREDIS-315
	public void ttlShouldReturnMinusOneWhenKeyDoesNotHaveExpirationSet() {

		nativeConnection.set(KEY_1, VALUE_1);

		assertThat(clusterConnection.ttl(KEY_1_BYTES)).isEqualTo(-1L);
	}

	@Test // DATAREDIS-315
	public void ttlShouldReturnMinusTwoWhenKeyDoesNotExist() {
		assertThat(clusterConnection.ttl(KEY_1_BYTES)).isEqualTo(-2L);
	}

	@Test // DATAREDIS-315
	public void ttlShouldReturnValueCorrectly() {

		nativeConnection.set(KEY_1, VALUE_1);
		nativeConnection.expire(KEY_1, 5);

		assertThat(clusterConnection.ttl(KEY_1_BYTES)).isGreaterThan(1);
	}

	@Test // DATAREDIS-526
	public void ttlWithTimeUnitShouldReturnMinusTwoWhenKeyDoesNotExist() {
		assertThat(clusterConnection.ttl(KEY_1_BYTES, TimeUnit.HOURS)).isEqualTo(-2L);
	}

	@Test // DATAREDIS-315
	public void typeShouldReadKeyTypeCorrectly() {

		nativeConnection.sadd(KEY_1_BYTES, VALUE_1_BYTES);
		nativeConnection.set(KEY_2_BYTES, VALUE_2_BYTES);
		nativeConnection.hmset(KEY_3_BYTES, Collections.singletonMap(KEY_1_BYTES, VALUE_1_BYTES));

		assertThat(clusterConnection.type(KEY_1_BYTES)).isEqualTo(DataType.SET);
		assertThat(clusterConnection.type(KEY_2_BYTES)).isEqualTo(DataType.STRING);
		assertThat(clusterConnection.type(KEY_3_BYTES)).isEqualTo(DataType.HASH);
	}

	@Test // DATAREDIS-315
	public void unwatchShouldThrowException() {
		assertThatExceptionOfType(DataAccessException.class).isThrownBy(clusterConnection::unwatch);
	}

	@Test // DATAREDIS-315
	public void watchShouldThrowException() {
		assertThatExceptionOfType(DataAccessException.class).isThrownBy(clusterConnection::watch);
	}

	@Test // DATAREDIS-674
	void zAddShouldAddMultipleValuesWithScoreCorrectly() {

		Set<Tuple> tuples = new HashSet<>();
		tuples.add(new DefaultTuple(VALUE_1_BYTES, 10D));
		tuples.add(new DefaultTuple(VALUE_2_BYTES, 20D));

		clusterConnection.zAdd(KEY_1_BYTES, tuples);

		assertThat(nativeConnection.zcard(KEY_1_BYTES)).isEqualTo(2L);
	}

	@Test // DATAREDIS-315
	public void zAddShouldAddValueWithScoreCorrectly() {

		clusterConnection.zAdd(KEY_1_BYTES, 10D, VALUE_1_BYTES);
		clusterConnection.zAdd(KEY_1_BYTES, 20D, VALUE_2_BYTES);

		assertThat(nativeConnection.zcard(KEY_1_BYTES)).isEqualTo(2L);
	}

	@Test // DATAREDIS-315
	public void zCardShouldReturnTotalNumberOfValues() {

		nativeConnection.zadd(KEY_1_BYTES, 10D, VALUE_1_BYTES);
		nativeConnection.zadd(KEY_1_BYTES, 20D, VALUE_2_BYTES);
		nativeConnection.zadd(KEY_1_BYTES, 5D, VALUE_3_BYTES);

		assertThat(clusterConnection.zCard(KEY_1_BYTES)).isEqualTo(3L);
	}

	@Test // DATAREDIS-315
	public void zCountShouldCountValuesInRange() {

		nativeConnection.zadd(KEY_1_BYTES, 10D, VALUE_1_BYTES);
		nativeConnection.zadd(KEY_1_BYTES, 20D, VALUE_2_BYTES);
		nativeConnection.zadd(KEY_1_BYTES, 5D, VALUE_3_BYTES);

		assertThat(clusterConnection.zCount(KEY_1_BYTES, 10, 20)).isEqualTo(2L);
	}

	@Test // DATAREDIS-315
	public void zIncrByShouldIncScoreForValueCorrectly() {

		nativeConnection.zadd(KEY_1_BYTES, 10D, VALUE_1_BYTES);
		nativeConnection.zadd(KEY_1_BYTES, 20D, VALUE_2_BYTES);

		clusterConnection.zIncrBy(KEY_1_BYTES, 100D, VALUE_1_BYTES);

		assertThat(nativeConnection.zrank(KEY_1_BYTES, VALUE_1_BYTES)).isEqualTo(1L);
	}

	@Test // GH-2041
	public void zDiffShouldThrowExceptionWhenKeysDoNotMapToSameSlots() {

		assertThatExceptionOfType(DataAccessException.class)
				.isThrownBy(() -> clusterConnection.zDiff(KEY_3_BYTES, KEY_1_BYTES, KEY_2_BYTES));
		assertThatExceptionOfType(DataAccessException.class)
				.isThrownBy(() -> clusterConnection.zDiffStore(KEY_3_BYTES, KEY_1_BYTES, KEY_2_BYTES));
		assertThatExceptionOfType(DataAccessException.class)
				.isThrownBy(() -> clusterConnection.zDiffWithScores(KEY_3_BYTES, KEY_1_BYTES, KEY_2_BYTES));
	}

	@Test // GH-2041
	@EnabledOnCommand("ZDIFF")
	public void zDiffShouldWorkForSameSlotKeys() {

		nativeConnection.zadd(SAME_SLOT_KEY_1_BYTES, 10D, VALUE_1_BYTES);
		nativeConnection.zadd(SAME_SLOT_KEY_1_BYTES, 20D, VALUE_2_BYTES);

		nativeConnection.zadd(SAME_SLOT_KEY_2_BYTES, 20D, VALUE_2_BYTES);

		assertThat(clusterConnection.zDiff(SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES)).contains(VALUE_1_BYTES);
		assertThat(clusterConnection.zDiffWithScores(SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES))
				.contains(new DefaultTuple(VALUE_1_BYTES, 10D));
	}

	@Test // GH-2041
	@EnabledOnCommand("ZDIFFSTORE")
	public void zDiffStoreShouldWorkForSameSlotKeys() {

		nativeConnection.zadd(SAME_SLOT_KEY_1_BYTES, 10D, VALUE_1_BYTES);
		nativeConnection.zadd(SAME_SLOT_KEY_1_BYTES, 20D, VALUE_2_BYTES);

		nativeConnection.zadd(SAME_SLOT_KEY_2_BYTES, 20D, VALUE_2_BYTES);

		clusterConnection.zDiffStore(SAME_SLOT_KEY_3_BYTES, SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES);

		assertThat(nativeConnection.zrange(SAME_SLOT_KEY_3_BYTES, 0, -1)).contains(VALUE_1_BYTES);
	}

	@Test // GH-2042
	public void zInterShouldThrowExceptionWhenKeysDoNotMapToSameSlots() {

		assertThatExceptionOfType(DataAccessException.class)
				.isThrownBy(() -> clusterConnection.zInter(KEY_3_BYTES, KEY_1_BYTES, KEY_2_BYTES));
		assertThatExceptionOfType(DataAccessException.class)
				.isThrownBy(() -> clusterConnection.zInterWithScores(KEY_3_BYTES, KEY_1_BYTES, KEY_2_BYTES));
	}

	@Test // GH-2042
	@EnabledOnCommand("ZINTER")
	public void zInterShouldWorkForSameSlotKeys() {

		nativeConnection.zadd(SAME_SLOT_KEY_1_BYTES, 10D, VALUE_1_BYTES);
		nativeConnection.zadd(SAME_SLOT_KEY_1_BYTES, 20D, VALUE_2_BYTES);

		nativeConnection.zadd(SAME_SLOT_KEY_2_BYTES, 20D, VALUE_2_BYTES);

		assertThat(clusterConnection.zInter(SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES)).contains(VALUE_2_BYTES);
		assertThat(clusterConnection.zInterWithScores(SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES))
				.contains(new DefaultTuple(VALUE_2_BYTES, 40D));
	}

	@Test // GH-2042
	public void zInterStoreShouldThrowExceptionWhenKeysDoNotMapToSameSlots() {

		assertThatExceptionOfType(DataAccessException.class)
				.isThrownBy(() -> clusterConnection.zInterStore(KEY_3_BYTES, KEY_1_BYTES, KEY_2_BYTES));
	}

	@Test // DATAREDIS-315
	public void zInterStoreShouldWorkForSameSlotKeys() {

		nativeConnection.zadd(SAME_SLOT_KEY_1_BYTES, 10D, VALUE_1_BYTES);
		nativeConnection.zadd(SAME_SLOT_KEY_1_BYTES, 20D, VALUE_2_BYTES);

		nativeConnection.zadd(SAME_SLOT_KEY_2_BYTES, 20D, VALUE_2_BYTES);
		nativeConnection.zadd(SAME_SLOT_KEY_2_BYTES, 30D, VALUE_3_BYTES);

		clusterConnection.zInterStore(SAME_SLOT_KEY_3_BYTES, SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES);

		assertThat(nativeConnection.zrange(SAME_SLOT_KEY_3_BYTES, 0, -1)).contains(VALUE_2_BYTES);
	}

	@Test // GH-2007
	@EnabledOnCommand("ZPOPMIN")
	public void zPopMinShouldWorkCorrectly() {

		nativeConnection.zadd(KEY_1_BYTES, 10D, VALUE_1_BYTES);
		nativeConnection.zadd(KEY_1_BYTES, 20D, VALUE_2_BYTES);
		nativeConnection.zadd(KEY_1_BYTES, 30D, VALUE_3_BYTES);

		assertThat(clusterConnection.zPopMin(KEY_1_BYTES)).isEqualTo(new DefaultTuple(VALUE_1_BYTES, 10D));
		assertThat(clusterConnection.zPopMin(KEY_1_BYTES, 2)).containsExactly(new DefaultTuple(VALUE_2_BYTES, 20D),
				new DefaultTuple(VALUE_3_BYTES, 30D));
	}

	@Test // GH-2007
	@EnabledOnCommand("BZPOPMIN")
	public void bzPopMinShouldWorkCorrectly() {

		assertThat(clusterConnection.zSetCommands().zCard(KEY_1_BYTES)).isZero();

		nativeConnection.zadd(KEY_1_BYTES, 10D, VALUE_1_BYTES);
		nativeConnection.zadd(KEY_1_BYTES, 20D, VALUE_2_BYTES);
		nativeConnection.zadd(KEY_1_BYTES, 30D, VALUE_3_BYTES);

		assertThat(clusterConnection.bZPopMin(KEY_1_BYTES, 1, TimeUnit.SECONDS))
				.isEqualTo(new DefaultTuple(VALUE_1_BYTES, 10D));
	}

	@Test // GH-2007
	@EnabledOnCommand("ZPOPMAX")
	public void zPopMaxShouldWorkCorrectly() {

		nativeConnection.zadd(KEY_1_BYTES, 10D, VALUE_1_BYTES);
		nativeConnection.zadd(KEY_1_BYTES, 20D, VALUE_2_BYTES);
		nativeConnection.zadd(KEY_1_BYTES, 30D, VALUE_3_BYTES);

		assertThat(clusterConnection.zPopMax(KEY_1_BYTES)).isEqualTo(new DefaultTuple(VALUE_3_BYTES, 30D));
		assertThat(clusterConnection.zPopMax(KEY_1_BYTES, 2)).containsExactly(new DefaultTuple(VALUE_2_BYTES, 20D),
				new DefaultTuple(VALUE_1_BYTES, 10D));
	}

	@Test // GH-2007
	@EnabledOnCommand("BZPOPMAX")
	public void bzPopMaxShouldWorkCorrectly() {

		assertThat(clusterConnection.zSetCommands().zCard(KEY_1_BYTES)).isZero();

		nativeConnection.zadd(KEY_1_BYTES, 10D, VALUE_1_BYTES);
		nativeConnection.zadd(KEY_1_BYTES, 20D, VALUE_2_BYTES);
		nativeConnection.zadd(KEY_1_BYTES, 30D, VALUE_3_BYTES);

		assertThat(clusterConnection.bZPopMax(KEY_1_BYTES, 1, TimeUnit.SECONDS))
				.isEqualTo(new DefaultTuple(VALUE_3_BYTES, 30D));
	}

	@Test // GH-2049
	@EnabledOnCommand("ZRANDMEMBER")
	public void zRandMemberShouldReturnResultCorrectly() {

		nativeConnection.zadd(KEY_1_BYTES, 10D, VALUE_1_BYTES);
		nativeConnection.zadd(KEY_1_BYTES, 20D, VALUE_2_BYTES);

		assertThat(clusterConnection.zRandMember(KEY_1_BYTES)).isIn(VALUE_1_BYTES, VALUE_2_BYTES);
		assertThat(clusterConnection.zRandMember(KEY_1_BYTES, 2)).hasSize(2).contains(VALUE_1_BYTES, VALUE_2_BYTES);

	}

	@Test // GH-2049
	@EnabledOnCommand("ZRANDMEMBER")
	public void zRandMemberWithScoreShouldReturnResultCorrectly() {

		nativeConnection.zadd(KEY_1_BYTES, 10D, VALUE_1_BYTES);
		nativeConnection.zadd(KEY_1_BYTES, 20D, VALUE_2_BYTES);

		assertThat(clusterConnection.zRandMemberWithScore(KEY_1_BYTES)).isNotNull();
		assertThat(clusterConnection.zRandMemberWithScore(KEY_1_BYTES, 2)).hasSize(2);
	}

	@Test // DATAREDIS-315
	public void zRangeByLexShouldReturnResultCorrectly() {

		nativeConnection.zadd(KEY_1, 0, "a");
		nativeConnection.zadd(KEY_1, 0, "b");
		nativeConnection.zadd(KEY_1, 0, "c");
		nativeConnection.zadd(KEY_1, 0, "d");
		nativeConnection.zadd(KEY_1, 0, "e");
		nativeConnection.zadd(KEY_1, 0, "f");
		nativeConnection.zadd(KEY_1, 0, "g");

		Set<byte[]> values = clusterConnection.zRangeByLex(KEY_1_BYTES, Range.range().lte("c").toRange());

		assertThat(values).contains(JedisConverters.toBytes("a"), JedisConverters.toBytes("b"),
				JedisConverters.toBytes("c"));
		assertThat(values).doesNotContain(JedisConverters.toBytes("d"), JedisConverters.toBytes("e"),
				JedisConverters.toBytes("f"), JedisConverters.toBytes("g"));

		values = clusterConnection.zRangeByLex(KEY_1_BYTES, Range.range().lt("c").toRange());
		assertThat(values).contains(JedisConverters.toBytes("a"), JedisConverters.toBytes("b"));
		assertThat(values).doesNotContain(JedisConverters.toBytes("c"));

		values = clusterConnection.zRangeByLex(KEY_1_BYTES, Range.range().gte("aaa").lt("g").toRange());
		assertThat(values).contains(JedisConverters.toBytes("b"), JedisConverters.toBytes("c"),
				JedisConverters.toBytes("d"), JedisConverters.toBytes("e"), JedisConverters.toBytes("f"));
		assertThat(values).doesNotContain(JedisConverters.toBytes("a"), JedisConverters.toBytes("g"));

		values = clusterConnection.zRangeByLex(KEY_1_BYTES, Range.range().gte("e").toRange());
		assertThat(values).contains(JedisConverters.toBytes("e"), JedisConverters.toBytes("f"),
				JedisConverters.toBytes("g"));
		assertThat(values).doesNotContain(JedisConverters.toBytes("a"), JedisConverters.toBytes("b"),
				JedisConverters.toBytes("c"), JedisConverters.toBytes("d"));
	}

	@Test // GH-1998
	public void zRevRangeByLexShouldReturnValuesCorrectly() {

		nativeConnection.zadd(KEY_1, 0, "a");
		nativeConnection.zadd(KEY_1, 0, "b");
		nativeConnection.zadd(KEY_1, 0, "c");
		nativeConnection.zadd(KEY_1, 0, "d");
		nativeConnection.zadd(KEY_1, 0, "e");
		nativeConnection.zadd(KEY_1, 0, "f");
		nativeConnection.zadd(KEY_1, 0, "g");

		Set<byte[]> values = clusterConnection.zRevRangeByLex(KEY_1_BYTES, Range.range().lte("c").toRange());

		assertThat(values).containsExactly(JedisConverters.toBytes("c"), JedisConverters.toBytes("b"),
				JedisConverters.toBytes("a"));
		assertThat(values).doesNotContain(JedisConverters.toBytes("d"), JedisConverters.toBytes("e"),
				JedisConverters.toBytes("f"), JedisConverters.toBytes("g"));

		values = clusterConnection.zRevRangeByLex(KEY_1_BYTES, Range.range().lt("c").toRange());
		assertThat(values).containsExactly(JedisConverters.toBytes("b"), JedisConverters.toBytes("a"));
		assertThat(values).doesNotContain(JedisConverters.toBytes("c"));

		values = clusterConnection.zRevRangeByLex(KEY_1_BYTES, Range.range().gte("aaa").lt("g").toRange());
		assertThat(values).containsExactly(JedisConverters.toBytes("f"), JedisConverters.toBytes("e"),
				JedisConverters.toBytes("d"), JedisConverters.toBytes("c"), JedisConverters.toBytes("b"));
		assertThat(values).doesNotContain(JedisConverters.toBytes("a"), JedisConverters.toBytes("g"));

		values = clusterConnection.zRevRangeByLex(KEY_1_BYTES, Range.range().lte("d").toRange(),
				Limit.limit().count(2).offset(1));

		assertThat(values).hasSize(2).containsExactly(JedisConverters.toBytes("c"), JedisConverters.toBytes("b"));
		assertThat(values).doesNotContain(JedisConverters.toBytes("a"), JedisConverters.toBytes("d"),
				JedisConverters.toBytes("e"), JedisConverters.toBytes("f"), JedisConverters.toBytes("g"));
	}

	@Test // DATAREDIS-315
	public void zRangeByScoreShouldReturnValuesCorrectly() {

		nativeConnection.zadd(KEY_1_BYTES, 10D, VALUE_1_BYTES);
		nativeConnection.zadd(KEY_1_BYTES, 20D, VALUE_2_BYTES);
		nativeConnection.zadd(KEY_1_BYTES, 5D, VALUE_3_BYTES);

		assertThat(clusterConnection.zRangeByScore(KEY_1_BYTES, 10, 20)).contains(VALUE_1_BYTES, VALUE_2_BYTES);
	}

	@Test // DATAREDIS-315
	public void zRangeByScoreShouldReturnValuesCorrectlyWhenGivenOffsetAndScore() {

		nativeConnection.zadd(KEY_1_BYTES, 10D, VALUE_1_BYTES);
		nativeConnection.zadd(KEY_1_BYTES, 20D, VALUE_2_BYTES);
		nativeConnection.zadd(KEY_1_BYTES, 5D, VALUE_3_BYTES);

		assertThat(clusterConnection.zRangeByScore(KEY_1_BYTES, 10D, 20D, 0L, 1L)).contains(VALUE_1_BYTES);
	}

	@Test // DATAREDIS-315
	public void zRangeByScoreWithScoresShouldReturnValuesAndScoreCorrectly() {

		nativeConnection.zadd(KEY_1_BYTES, 10D, VALUE_1_BYTES);
		nativeConnection.zadd(KEY_1_BYTES, 20D, VALUE_2_BYTES);
		nativeConnection.zadd(KEY_1_BYTES, 5D, VALUE_3_BYTES);

		assertThat(clusterConnection.zRangeByScoreWithScores(KEY_1_BYTES, 10, 20))
				.contains((Tuple) new DefaultTuple(VALUE_1_BYTES, 10D), (Tuple) new DefaultTuple(VALUE_2_BYTES, 20D));
	}

	@Test // DATAREDIS-315
	public void zRangeByScoreWithScoresShouldReturnValuesCorrectlyWhenGivenOffsetAndScore() {

		nativeConnection.zadd(KEY_1_BYTES, 10D, VALUE_1_BYTES);
		nativeConnection.zadd(KEY_1_BYTES, 20D, VALUE_2_BYTES);
		nativeConnection.zadd(KEY_1_BYTES, 5D, VALUE_3_BYTES);

		assertThat(clusterConnection.zRangeByScoreWithScores(KEY_1_BYTES, 10D, 20D, 0L, 1L))
				.contains((Tuple) new DefaultTuple(VALUE_1_BYTES, 10D));
	}

	@Test // DATAREDIS-315
	public void zRangeShouldReturnValuesCorrectly() {

		nativeConnection.zadd(KEY_1_BYTES, 10D, VALUE_1_BYTES);
		nativeConnection.zadd(KEY_1_BYTES, 20D, VALUE_2_BYTES);
		nativeConnection.zadd(KEY_1_BYTES, 5D, VALUE_3_BYTES);

		assertThat(clusterConnection.zRange(KEY_1_BYTES, 1, 2)).contains(VALUE_1_BYTES, VALUE_2_BYTES);
	}

	@Test // DATAREDIS-315
	public void zRangeWithScoresShouldReturnValuesAndScoreCorrectly() {

		nativeConnection.zadd(KEY_1_BYTES, 10D, VALUE_1_BYTES);
		nativeConnection.zadd(KEY_1_BYTES, 20D, VALUE_2_BYTES);
		nativeConnection.zadd(KEY_1_BYTES, 5D, VALUE_3_BYTES);

		assertThat(clusterConnection.zRangeWithScores(KEY_1_BYTES, 1, 2))
				.contains((Tuple) new DefaultTuple(VALUE_1_BYTES, 10D), (Tuple) new DefaultTuple(VALUE_2_BYTES, 20D));
	}

	@Test // DATAREDIS-315
	public void zRankShouldReturnPositionForValueCorrectly() {

		nativeConnection.zadd(KEY_1_BYTES, 10D, VALUE_1_BYTES);
		nativeConnection.zadd(KEY_1_BYTES, 20D, VALUE_2_BYTES);

		assertThat(clusterConnection.zRank(KEY_1_BYTES, VALUE_2_BYTES)).isEqualTo(1L);
	}

	@Test // DATAREDIS-315
	public void zRankShouldReturnReversePositionForValueCorrectly() {

		nativeConnection.zadd(KEY_1_BYTES, 10D, VALUE_1_BYTES);
		nativeConnection.zadd(KEY_1_BYTES, 20D, VALUE_2_BYTES);

		assertThat(clusterConnection.zRevRank(KEY_1_BYTES, VALUE_2_BYTES)).isEqualTo(0L);
	}

	@Test // DATAREDIS-315
	public void zRemRangeByScoreShouldRemoveValues() {

		nativeConnection.zadd(KEY_1_BYTES, 10D, VALUE_1_BYTES);
		nativeConnection.zadd(KEY_1_BYTES, 20D, VALUE_2_BYTES);
		nativeConnection.zadd(KEY_1_BYTES, 30D, VALUE_3_BYTES);

		clusterConnection.zRemRangeByScore(KEY_1_BYTES, 15D, 25D);

		assertThat(nativeConnection.zcard(KEY_1_BYTES)).isEqualTo(2L);
		assertThat(nativeConnection.zrange(KEY_1_BYTES, 0, -1)).contains(VALUE_1_BYTES, VALUE_3_BYTES);
	}

	@Test // DATAREDIS-315
	public void zRemRangeShouldRemoveValues() {

		nativeConnection.zadd(KEY_1_BYTES, 10D, VALUE_1_BYTES);
		nativeConnection.zadd(KEY_1_BYTES, 20D, VALUE_2_BYTES);
		nativeConnection.zadd(KEY_1_BYTES, 30D, VALUE_3_BYTES);

		clusterConnection.zRemRange(KEY_1_BYTES, 1, 2);

		assertThat(nativeConnection.zcard(KEY_1_BYTES)).isEqualTo(1L);
		assertThat(nativeConnection.zrange(KEY_1_BYTES, 0, -1)).contains(VALUE_1_BYTES);
	}

	@Test // DATAREDIS-315
	public void zRemShouldRemoveValueWithScoreCorrectly() {

		nativeConnection.zadd(KEY_1_BYTES, 10D, VALUE_1_BYTES);
		nativeConnection.zadd(KEY_1_BYTES, 20D, VALUE_2_BYTES);

		clusterConnection.zRem(KEY_1_BYTES, VALUE_1_BYTES);

		assertThat(nativeConnection.zcard(KEY_1_BYTES)).isEqualTo(1L);
	}

	@Test // DATAREDIS-315
	public void zRevRangeByScoreShouldReturnValuesCorrectly() {

		nativeConnection.zadd(KEY_1_BYTES, 10D, VALUE_1_BYTES);
		nativeConnection.zadd(KEY_1_BYTES, 20D, VALUE_2_BYTES);
		nativeConnection.zadd(KEY_1_BYTES, 5D, VALUE_3_BYTES);

		assertThat(clusterConnection.zRevRangeByScore(KEY_1_BYTES, 10D, 20D)).contains(VALUE_2_BYTES, VALUE_1_BYTES);
	}

	@Test // DATAREDIS-315
	public void zRevRangeByScoreShouldReturnValuesCorrectlyWhenGivenOffsetAndScore() {

		nativeConnection.zadd(KEY_1_BYTES, 10D, VALUE_1_BYTES);
		nativeConnection.zadd(KEY_1_BYTES, 20D, VALUE_2_BYTES);
		nativeConnection.zadd(KEY_1_BYTES, 5D, VALUE_3_BYTES);

		assertThat(clusterConnection.zRevRangeByScore(KEY_1_BYTES, 10D, 20D, 0L, 1L)).contains(VALUE_2_BYTES);
	}

	@Test // DATAREDIS-315
	public void zRevRangeByScoreWithScoresShouldReturnValuesAndScoreCorrectly() {

		nativeConnection.zadd(KEY_1_BYTES, 10D, VALUE_1_BYTES);
		nativeConnection.zadd(KEY_1_BYTES, 20D, VALUE_2_BYTES);
		nativeConnection.zadd(KEY_1_BYTES, 5D, VALUE_3_BYTES);

		assertThat(clusterConnection.zRevRangeByScoreWithScores(KEY_1_BYTES, 10D, 20D))
				.contains((Tuple) new DefaultTuple(VALUE_2_BYTES, 20D), (Tuple) new DefaultTuple(VALUE_1_BYTES, 10D));
	}

	@Test // DATAREDIS-315
	public void zRevRangeByScoreWithScoresShouldReturnValuesCorrectlyWhenGivenOffsetAndScore() {

		nativeConnection.zadd(KEY_1_BYTES, 10D, VALUE_1_BYTES);
		nativeConnection.zadd(KEY_1_BYTES, 20D, VALUE_2_BYTES);
		nativeConnection.zadd(KEY_1_BYTES, 5D, VALUE_3_BYTES);

		assertThat(clusterConnection.zRevRangeByScoreWithScores(KEY_1_BYTES, 10D, 20D, 0L, 1L))
				.contains((Tuple) new DefaultTuple(VALUE_2_BYTES, 20D));
	}

	@Test // DATAREDIS-315
	public void zRevRangeShouldReturnValuesCorrectly() {

		nativeConnection.zadd(KEY_1_BYTES, 10D, VALUE_1_BYTES);
		nativeConnection.zadd(KEY_1_BYTES, 20D, VALUE_2_BYTES);
		nativeConnection.zadd(KEY_1_BYTES, 5D, VALUE_3_BYTES);

		assertThat(clusterConnection.zRevRange(KEY_1_BYTES, 1, 2)).contains(VALUE_3_BYTES, VALUE_1_BYTES);
	}

	@Test // DATAREDIS-315
	public void zRevRangeWithScoresShouldReturnValuesAndScoreCorrectly() {

		nativeConnection.zadd(KEY_1_BYTES, 10D, VALUE_1_BYTES);
		nativeConnection.zadd(KEY_1_BYTES, 20D, VALUE_2_BYTES);
		nativeConnection.zadd(KEY_1_BYTES, 5D, VALUE_3_BYTES);

		assertThat(clusterConnection.zRevRangeWithScores(KEY_1_BYTES, 1, 2))
				.contains((Tuple) new DefaultTuple(VALUE_3_BYTES, 5D), (Tuple) new DefaultTuple(VALUE_1_BYTES, 10D));
	}

	@Test // DATAREDIS-479
	public void zScanShouldReadEntireValueRange() {

		int nrOfValues = 321;
		for (int i = 0; i < nrOfValues; i++) {
			nativeConnection.zadd(KEY_1_BYTES, i, JedisConverters.toBytes("value-" + i));
		}

		Cursor<Tuple> tuples = clusterConnection.zScan(KEY_1_BYTES, ScanOptions.NONE);

		int count = 0;
		while (tuples.hasNext()) {

			tuples.next();
			count++;
		}

		assertThat(count).isEqualTo(nrOfValues);
	}

	@Test // DATAREDIS-315
	public void zScoreShouldRetrieveScoreForValue() {

		nativeConnection.zadd(KEY_1_BYTES, 10D, VALUE_1_BYTES);
		nativeConnection.zadd(KEY_1_BYTES, 20D, VALUE_2_BYTES);

		assertThat(clusterConnection.zScore(KEY_1_BYTES, VALUE_2_BYTES)).isEqualTo(20D);
	}

	@Test // GH-2038
	@EnabledOnCommand("ZMSCORE")
	public void zMScoreShouldRetrieveScoreForValues() {

		nativeConnection.zadd(KEY_1_BYTES, 10D, VALUE_1_BYTES);
		nativeConnection.zadd(KEY_1_BYTES, 20D, VALUE_2_BYTES);

		assertThat(clusterConnection.zMScore(KEY_1_BYTES, VALUE_1_BYTES, VALUE_2_BYTES)).containsSequence(10D, 20D);
	}

	@Test // GH-2042
	public void zUnionShouldThrowExceptionWhenKeysDoNotMapToSameSlots() {
		assertThatExceptionOfType(DataAccessException.class)
				.isThrownBy(() -> clusterConnection.zUnion(KEY_3_BYTES, KEY_1_BYTES, KEY_2_BYTES));
		assertThatExceptionOfType(DataAccessException.class)
				.isThrownBy(() -> clusterConnection.zUnionWithScores(KEY_3_BYTES, KEY_1_BYTES, KEY_2_BYTES));
	}

	@Test // GH-2042
	@EnabledOnCommand("ZUNION")
	public void zUnionShouldWorkForSameSlotKeys() {

		nativeConnection.zadd(SAME_SLOT_KEY_1_BYTES, 10D, VALUE_1_BYTES);
		nativeConnection.zadd(SAME_SLOT_KEY_1_BYTES, 30D, VALUE_3_BYTES);
		nativeConnection.zadd(SAME_SLOT_KEY_2_BYTES, 20D, VALUE_2_BYTES);

		assertThat(clusterConnection.zUnion(SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES)).contains(VALUE_1_BYTES,
				VALUE_2_BYTES, VALUE_3_BYTES);
		assertThat(clusterConnection.zUnionWithScores(SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES)).contains(
				new DefaultTuple(VALUE_1_BYTES, 10D), new DefaultTuple(VALUE_2_BYTES, 20D),
				new DefaultTuple(VALUE_3_BYTES, 30D));
	}

	@Test // DATAREDIS-315
	public void zUnionStoreShouldThrowExceptionWhenKeysDoNotMapToSameSlots() {
		assertThatExceptionOfType(DataAccessException.class)
				.isThrownBy(() -> clusterConnection.zUnionStore(KEY_3_BYTES, KEY_1_BYTES, KEY_2_BYTES));
	}

	@Test // DATAREDIS-315
	public void zUnionStoreShouldWorkForSameSlotKeys() {

		nativeConnection.zadd(SAME_SLOT_KEY_1_BYTES, 10D, VALUE_1_BYTES);
		nativeConnection.zadd(SAME_SLOT_KEY_1_BYTES, 30D, VALUE_3_BYTES);
		nativeConnection.zadd(SAME_SLOT_KEY_2_BYTES, 20D, VALUE_2_BYTES);

		clusterConnection.zUnionStore(SAME_SLOT_KEY_3_BYTES, SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES);

		assertThat(nativeConnection.zrange(SAME_SLOT_KEY_3_BYTES, 0, -1)).contains(VALUE_1_BYTES, VALUE_2_BYTES,
				VALUE_3_BYTES);
	}

	@Test // DATAREDIS-694
	void touchReturnsNrOfKeysTouched() {

		nativeConnection.set(KEY_1, VALUE_1);
		nativeConnection.set(KEY_2, VALUE_1);

		assertThat(clusterConnection.keyCommands().touch(KEY_1_BYTES, KEY_2_BYTES, KEY_3_BYTES)).isEqualTo(2L);
	}

	@Test // DATAREDIS-694
	void touchReturnsZeroIfNoKeysTouched() {
		assertThat(clusterConnection.keyCommands().touch(KEY_1_BYTES)).isEqualTo(0L);
	}

	@Test // DATAREDIS-693
	void unlinkReturnsNrOfKeysTouched() {

		nativeConnection.set(KEY_1, VALUE_1);
		nativeConnection.set(KEY_2, VALUE_1);

		assertThat(clusterConnection.keyCommands().unlink(KEY_1_BYTES, KEY_2_BYTES, KEY_3_BYTES)).isEqualTo(2L);
	}

	@Test // DATAREDIS-693
	void unlinkReturnsZeroIfNoKeysTouched() {
		assertThat(clusterConnection.keyCommands().unlink(KEY_1_BYTES)).isEqualTo(0L);
	}

	@Test // DATAREDIS-697
	void bitPosShouldReturnPositionCorrectly() {

		nativeConnection.set(KEY_1_BYTES, HexStringUtils.hexToBytes("fff000"));

		assertThat(clusterConnection.stringCommands().bitPos(KEY_1_BYTES, false)).isEqualTo(12L);
	}

	@Test // DATAREDIS-697
	void bitPosShouldReturnPositionInRangeCorrectly() {

		nativeConnection.set(KEY_1_BYTES, HexStringUtils.hexToBytes("fff0f0"));

		assertThat(clusterConnection.stringCommands().bitPos(KEY_1_BYTES, true,
				org.springframework.data.domain.Range.of(Bound.inclusive(2L), Bound.unbounded()))).isEqualTo(16L);
	}

	@Test // DATAREDIS-716
	void encodingReturnsCorrectly() {

		nativeConnection.set(KEY_1_BYTES, "1000".getBytes());

		assertThat(clusterConnection.keyCommands().encodingOf(KEY_1_BYTES)).isEqualTo(RedisValueEncoding.INT);
	}

	@Test // DATAREDIS-716
	void encodingReturnsVacantWhenKeyDoesNotExist() {
		assertThat(clusterConnection.keyCommands().encodingOf(KEY_2_BYTES)).isEqualTo(RedisValueEncoding.VACANT);
	}

	@Test // DATAREDIS-716
	void idletimeReturnsCorrectly() {

		nativeConnection.set(KEY_1_BYTES, VALUE_1_BYTES);
		nativeConnection.get(KEY_1_BYTES);

		assertThat(clusterConnection.keyCommands().idletime(KEY_1_BYTES)).isLessThan(Duration.ofSeconds(5));
	}

	@Test // DATAREDIS-716
	void idldetimeReturnsNullWhenKeyDoesNotExist() {
		assertThat(clusterConnection.keyCommands().idletime(KEY_3_BYTES)).isNull();
	}

	@Test // DATAREDIS-716
	void refcountReturnsCorrectly() {

		nativeConnection.lpush(KEY_1_BYTES, VALUE_1_BYTES);

		assertThat(clusterConnection.keyCommands().refcount(KEY_1_BYTES)).isEqualTo(1L);
	}

	@Test // DATAREDIS-716
	void refcountReturnsNullWhenKeyDoesNotExist() {
		assertThat(clusterConnection.keyCommands().refcount(KEY_3_BYTES)).isNull();
	}

	@Test // DATAREDIS-562
	void bitFieldSetShouldWorkCorrectly() {

		assertThat(clusterConnection.stringCommands().bitField(JedisConverters.toBytes(KEY_1),
				create().set(INT_8).valueAt(BitFieldSubCommands.Offset.offset(0L)).to(10L))).containsExactly(0L);
		assertThat(clusterConnection.stringCommands().bitField(JedisConverters.toBytes(KEY_1),
				create().set(INT_8).valueAt(BitFieldSubCommands.Offset.offset(0L)).to(20L))).containsExactly(10L);
	}

	@Test // DATAREDIS-562
	void bitFieldGetShouldWorkCorrectly() {

		assertThat(clusterConnection.stringCommands().bitField(JedisConverters.toBytes(KEY_1),
				create().get(INT_8).valueAt(BitFieldSubCommands.Offset.offset(0L)))).containsExactly(0L);
	}

	@Test // DATAREDIS-562
	void bitFieldIncrByShouldWorkCorrectly() {

		assertThat(clusterConnection.stringCommands().bitField(JedisConverters.toBytes(KEY_1),
				create().incr(INT_8).valueAt(BitFieldSubCommands.Offset.offset(100L)).by(1L))).containsExactly(1L);
	}

	@Test // DATAREDIS-562
	void bitFieldIncrByWithOverflowShouldWorkCorrectly() {

		assertThat(clusterConnection.stringCommands().bitField(JedisConverters.toBytes(KEY_1),
				create().incr(unsigned(2)).valueAt(BitFieldSubCommands.Offset.offset(102L)).overflow(FAIL).by(1L)))
				.containsExactly(1L);
		assertThat(clusterConnection.stringCommands().bitField(JedisConverters.toBytes(KEY_1),
				create().incr(unsigned(2)).valueAt(BitFieldSubCommands.Offset.offset(102L)).overflow(FAIL).by(1L)))
				.containsExactly(2L);
		assertThat(clusterConnection.stringCommands().bitField(JedisConverters.toBytes(KEY_1),
				create().incr(unsigned(2)).valueAt(BitFieldSubCommands.Offset.offset(102L)).overflow(FAIL).by(1L)))
				.containsExactly(3L);
		assertThat(clusterConnection.stringCommands()
				.bitField(JedisConverters.toBytes(KEY_1),
						create().incr(unsigned(2)).valueAt(BitFieldSubCommands.Offset.offset(102L)).overflow(FAIL).by(1L))
				.get(0)).isNull();
	}

	@Test // DATAREDIS-562
	void bitfieldShouldAllowMultipleSubcommands() {

		assertThat(clusterConnection.stringCommands().bitField(JedisConverters.toBytes(KEY_1),
				create().incr(signed(5)).valueAt(BitFieldSubCommands.Offset.offset(100L)).by(1L).get(unsigned(4)).valueAt(0L)))
				.containsExactly(1L, 0L);
	}

	@Test // DATAREDIS-562
	void bitfieldShouldWorkUsingNonZeroBasedOffset() {

		assertThat(
				clusterConnection.stringCommands().bitField(JedisConverters.toBytes(KEY_1),
						create().set(INT_8).valueAt(BitFieldSubCommands.Offset.offset(0L).multipliedByTypeLength()).to(100L)
								.set(INT_8).valueAt(BitFieldSubCommands.Offset.offset(1L).multipliedByTypeLength()).to(200L)))
				.containsExactly(0L, 0L);
		assertThat(
				clusterConnection.stringCommands()
						.bitField(JedisConverters.toBytes(KEY_1),
								create().get(INT_8).valueAt(BitFieldSubCommands.Offset.offset(0L).multipliedByTypeLength()).get(INT_8)
										.valueAt(BitFieldSubCommands.Offset.offset(1L).multipliedByTypeLength())))
				.containsExactly(100L, -56L);
	}

	@Test // DATAREDIS-1005
	void evalShouldRunScript() {

		byte[] keyAndArgs = JedisConverters.toBytes("FOO");
		String luaScript = "return redis.call(\"INCR\", KEYS[1])";
		byte[] luaScriptBin = JedisConverters.toBytes(luaScript);

		Long result = clusterConnection.scriptingCommands().eval(luaScriptBin, ReturnType.VALUE, 1, keyAndArgs);

		assertThat(result).isEqualTo(1L);
	}

	@Test // DATAREDIS-1005
	void scriptLoadShouldLoadScript() {

		String luaScript = "return redis.call(\"INCR\", KEYS[1])";
		String digest = DigestUtils.sha1DigestAsHex(luaScript);
		byte[] luaScriptBin = JedisConverters.toBytes(luaScript);

		String result = clusterConnection.scriptingCommands().scriptLoad(luaScriptBin);

		assertThat(result).isEqualTo(digest);
	}

	@Test // DATAREDIS-1005
	void scriptFlushShouldRemoveScripts() {

		byte[] keyAndArgs = JedisConverters.toBytes("FOO");
		String luaScript = "return redis.call(\"GET\", KEYS[1])";
		byte[] luaScriptBin = JedisConverters.toBytes(luaScript);

		clusterConnection.scriptingCommands().scriptLoad(luaScriptBin);
		clusterConnection.scriptingCommands().scriptFlush();

		try {
			clusterConnection.scriptingCommands().evalSha(luaScriptBin, ReturnType.VALUE, 1, keyAndArgs);
			fail("expected InvalidDataAccessApiUsageException");
		} catch (InvalidDataAccessApiUsageException ex) {
			assertThat(ex.getMessage()).contains("NOSCRIPT");
		}
	}

	@Test // DATAREDIS-1005
	void evelShaShouldRunScript() {

		byte[] keyAndArgs = JedisConverters.toBytes("FOO");
		String luaScript = "return redis.call(\"INCR\", KEYS[1])";
		byte[] digest = JedisConverters.toBytes(DigestUtils.sha1DigestAsHex(luaScript));

		clusterConnection.scriptingCommands().scriptLoad(JedisConverters.toBytes(luaScript));

		Long result = clusterConnection.scriptingCommands().evalSha(digest, ReturnType.VALUE, 1, keyAndArgs);
		assertThat(result).isEqualTo(1L);
	}

	@Test // GH-1957
	@EnabledOnCommand("LPOS")
	void lPos() {

		nativeConnection.rpush(KEY_1, "a", "b", "c", "1", "2", "3", "c", "c");
		List<Long> result = clusterConnection.listCommands().lPos(KEY_1_BYTES, "c".getBytes(StandardCharsets.UTF_8), null,
				null);

		assertThat(result).containsOnly(2L);
	}

	@Test // GH-1957
	@EnabledOnCommand("LPOS")
	void lPosRank() {

		nativeConnection.rpush(KEY_1, "a", "b", "c", "1", "2", "3", "c", "c");
		List<Long> result = clusterConnection.listCommands().lPos(KEY_1_BYTES, "c".getBytes(StandardCharsets.UTF_8), 2,
				null);

		assertThat(result).containsExactly(6L);
	}

	@Test // GH-1957
	@EnabledOnCommand("LPOS")
	void lPosNegativeRank() {

		nativeConnection.rpush(KEY_1, "a", "b", "c", "1", "2", "3", "c", "c");
		List<Long> result = clusterConnection.listCommands().lPos(KEY_1_BYTES, "c".getBytes(StandardCharsets.UTF_8), -1,
				null);

		assertThat(result).containsExactly(7L);
	}

	@Test // GH-1957
	@EnabledOnCommand("LPOS")
	void lPosCount() {

		nativeConnection.rpush(KEY_1, "a", "b", "c", "1", "2", "3", "c", "c");
		List<Long> result = clusterConnection.listCommands().lPos(KEY_1_BYTES, "c".getBytes(StandardCharsets.UTF_8), null,
				2);

		assertThat(result).containsExactly(2L, 6L);
	}

	@Test // GH-1957
	@EnabledOnCommand("LPOS")
	void lPosRankCount() {

		nativeConnection.rpush(KEY_1, "a", "b", "c", "1", "2", "3", "c", "c");
		List<Long> result = clusterConnection.listCommands().lPos(KEY_1_BYTES, "c".getBytes(StandardCharsets.UTF_8), -1, 2);

		assertThat(result).containsExactly(7L, 6L);
	}

	@Test // GH-1957
	@EnabledOnCommand("LPOS")
	void lPosCountZero() {

		nativeConnection.rpush(KEY_1, "a", "b", "c", "1", "2", "3", "c", "c");
		List<Long> result = clusterConnection.listCommands().lPos(KEY_1_BYTES, "c".getBytes(StandardCharsets.UTF_8), null,
				0);

		assertThat(result).containsExactly(2L, 6L, 7L);
	}

	@Test // GH-1957
	@EnabledOnCommand("LPOS")
	void lPosNonExisting() {

		nativeConnection.rpush(KEY_1, "a", "b", "c", "1", "2", "3", "c", "c");
		List<Long> result = clusterConnection.listCommands().lPos(KEY_1_BYTES, "x".getBytes(StandardCharsets.UTF_8), null,
				null);

		assertThat(result).isEmpty();
	}

	@Test // GH-2986
	void shouldUseCachedTopology() {

		JedisClusterConnection.JedisClusterTopologyProvider provider = (JedisClusterConnection.JedisClusterTopologyProvider) clusterConnection
				.getTopologyProvider();
		ReflectionTestUtils.setField(provider, "cached", null);

		ClusterTopology topology = provider.getTopology();
		assertThat(topology).isInstanceOf(JedisClusterConnection.JedisClusterTopology.class);

		assertThat(provider.shouldUseCachedValue(null)).isFalse();
		assertThat(provider.shouldUseCachedValue(
				new JedisClusterConnection.JedisClusterTopology(Set.of(), System.currentTimeMillis() - 101, 100))).isFalse();
		assertThat(provider.shouldUseCachedValue(
				new JedisClusterConnection.JedisClusterTopology(Set.of(), System.currentTimeMillis() + 100, 100))).isTrue();
	}
}
