/*
 * Copyright 2015-2016 the original author or authors.
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
package org.springframework.data.redis.connection.lettuce;

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.data.Offset.offset;
import static org.springframework.data.redis.connection.ClusterTestVariables.*;
import static org.springframework.data.redis.connection.lettuce.LettuceConverters.*;
import static org.springframework.data.redis.core.ScanOptions.*;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.ClusterConnectionTests;
import org.springframework.data.redis.connection.ClusterSlotHashUtil;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.DefaultSortParameters;
import org.springframework.data.redis.connection.DefaultTuple;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisListCommands.Position;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.RedisStringCommands.BitOperation;
import org.springframework.data.redis.connection.RedisStringCommands.SetOption;
import org.springframework.data.redis.connection.RedisZSetCommands.Range;
import org.springframework.data.redis.connection.RedisZSetCommands.Tuple;
import org.springframework.data.redis.connection.jedis.JedisConverters;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.test.util.RedisClusterRule;

import com.lambdaworks.redis.RedisURI.Builder;
import com.lambdaworks.redis.cluster.RedisAdvancedClusterConnection;
import com.lambdaworks.redis.cluster.RedisClusterClient;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class LettuceClusterConnectionTests implements ClusterConnectionTests {

	static final byte[] KEY_1_BYTES = toBytes(KEY_1);
	static final byte[] KEY_2_BYTES = toBytes(KEY_2);
	static final byte[] KEY_3_BYTES = toBytes(KEY_3);

	static final byte[] SAME_SLOT_KEY_1_BYTES = toBytes(SAME_SLOT_KEY_1);
	static final byte[] SAME_SLOT_KEY_2_BYTES = toBytes(SAME_SLOT_KEY_2);
	static final byte[] SAME_SLOT_KEY_3_BYTES = toBytes(SAME_SLOT_KEY_3);

	static final byte[] VALUE_1_BYTES = toBytes(VALUE_1);
	static final byte[] VALUE_2_BYTES = toBytes(VALUE_2);
	static final byte[] VALUE_3_BYTES = toBytes(VALUE_3);

	RedisClusterClient client;
	RedisAdvancedClusterConnection<String, String> nativeConnection;
	LettuceClusterConnection clusterConnection;

	public static @ClassRule RedisClusterRule clusterAvailable = new RedisClusterRule();

	@Before
	public void setUp() {

		client = RedisClusterClient.create(LettuceTestClientResources.getSharedClientResources(),
				Builder.redis(CLUSTER_HOST, MASTER_NODE_1_PORT).withTimeout(100, TimeUnit.MILLISECONDS).build());
		nativeConnection = client.connectCluster();
		clusterConnection = new LettuceClusterConnection(client);
	}

	@After
	public void tearDown() throws InterruptedException {

		clusterConnection.flushDb();
		nativeConnection.close();
		clusterConnection.close();
		client.shutdown(0, 0, TimeUnit.MILLISECONDS);
	}

	@Test // DATAREDIS-315
	public void shouldAllowSettingAndGettingValues() {

		clusterConnection.set(KEY_1_BYTES, VALUE_1_BYTES);
		assertThat(clusterConnection.get(KEY_1_BYTES)).isEqualTo(VALUE_1_BYTES);
	}

	@Test // DATAREDIS-315
	public void delShouldRemoveSingleKeyCorrectly() {

		nativeConnection.set(KEY_1, VALUE_1);

		clusterConnection.del(KEY_1_BYTES);

		assertThat(nativeConnection.get(KEY_1)).isNull();
	}

	@Test // DATAREDIS-315
	public void delShouldRemoveMultipleKeysCorrectly() {

		nativeConnection.set(KEY_1, VALUE_1);
		nativeConnection.set(KEY_2, VALUE_2);

		clusterConnection.del(KEY_1_BYTES);
		clusterConnection.del(KEY_2_BYTES);

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
	public void typeShouldReadKeyTypeCorrectly() {

		nativeConnection.sadd(KEY_1, VALUE_1);
		nativeConnection.set(KEY_2, VALUE_2);
		nativeConnection.hmset(KEY_3, Collections.singletonMap(KEY_1, VALUE_1));

		assertThat(clusterConnection.type(KEY_1_BYTES)).isEqualTo(DataType.SET);
		assertThat(clusterConnection.type(KEY_2_BYTES)).isEqualTo(DataType.STRING);
		assertThat(clusterConnection.type(KEY_3_BYTES)).isEqualTo(DataType.HASH);
	}

	@Test // DATAREDIS-315
	public void keysShouldReturnAllKeys() {

		nativeConnection.set(KEY_1, VALUE_1);
		nativeConnection.set(KEY_2, VALUE_2);

		assertThat(clusterConnection.keys(toBytes("*"))).contains(KEY_1_BYTES, KEY_2_BYTES);
	}

	@Test // DATAREDIS-315
	public void keysShouldReturnAllKeysForSpecificNode() {

		nativeConnection.set(KEY_1, VALUE_1);
		nativeConnection.set(KEY_2, VALUE_2);

		Set<byte[]> keysOnNode = clusterConnection.keys(new RedisClusterNode("127.0.0.1", 7379, null),
				JedisConverters.toBytes("*"));

		assertThat(keysOnNode).contains(KEY_2_BYTES);
		assertThat(keysOnNode).doesNotContain(KEY_1_BYTES);
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

		nativeConnection.set(KEY_1, VALUE_1);

		clusterConnection.rename(KEY_1_BYTES, KEY_2_BYTES);

		assertThat(nativeConnection.exists(KEY_1)).isFalse();
		assertThat(nativeConnection.get(KEY_2)).isEqualTo(VALUE_1);
	}

	@Test // DATAREDIS-315
	public void renameSameKeysOnSameSlot() {

		nativeConnection.set(SAME_SLOT_KEY_1, VALUE_1);

		clusterConnection.rename(SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES);

		assertThat(nativeConnection.exists(SAME_SLOT_KEY_1)).isFalse();
		assertThat(nativeConnection.get(SAME_SLOT_KEY_2)).isEqualTo(VALUE_1);
	}

	@Test // DATAREDIS-315
	public void renameNXWhenTargetKeyDoesNotExist() {

		nativeConnection.set(KEY_1, VALUE_1);

		assertThat(clusterConnection.renameNX(KEY_1_BYTES, KEY_2_BYTES)).isEqualTo(Boolean.TRUE);

		assertThat(nativeConnection.exists(KEY_1)).isFalse();
		assertThat(nativeConnection.get(KEY_2)).isEqualTo(VALUE_1);
	}

	@Test // DATAREDIS-315
	public void renameNXWhenTargetKeyDoesExist() {

		nativeConnection.set(KEY_1, VALUE_1);
		nativeConnection.set(KEY_2, VALUE_2);

		assertThat(clusterConnection.renameNX(KEY_1_BYTES, KEY_2_BYTES)).isEqualTo(Boolean.FALSE);

		assertThat(nativeConnection.get(KEY_1)).isEqualTo(VALUE_1);
		assertThat(nativeConnection.get(KEY_2)).isEqualTo(VALUE_2);
	}

	@Test // DATAREDIS-315
	public void renameNXWhenOnSameSlot() {

		nativeConnection.set(SAME_SLOT_KEY_1, VALUE_1);

		assertThat(clusterConnection.renameNX(SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES)).isEqualTo(Boolean.TRUE);

		assertThat(nativeConnection.exists(SAME_SLOT_KEY_1)).isFalse();
		assertThat(nativeConnection.get(SAME_SLOT_KEY_2)).isEqualTo(VALUE_1);
	}

	@Test // DATAREDIS-315
	public void expireShouldBeSetCorreclty() {

		nativeConnection.set(KEY_1, VALUE_1);

		clusterConnection.expire(KEY_1_BYTES, 5);

		assertThat(nativeConnection.ttl(LettuceConverters.toString(KEY_1_BYTES))).isGreaterThan(1);
	}

	@Test // DATAREDIS-315
	public void pExpireShouldBeSetCorreclty() {

		nativeConnection.set(KEY_1, VALUE_1);

		clusterConnection.pExpire(KEY_1_BYTES, 5000);

		assertThat(nativeConnection.ttl(LettuceConverters.toString(KEY_1_BYTES))).isGreaterThan(1);
	}

	@Test // DATAREDIS-315
	public void expireAtShouldBeSetCorrectly() {

		nativeConnection.set(KEY_1, VALUE_1);

		clusterConnection.expireAt(KEY_1_BYTES, System.currentTimeMillis() / 1000 + 5000);

		assertThat(nativeConnection.ttl(LettuceConverters.toString(KEY_1_BYTES))).isGreaterThan(1);
	}

	@Test // DATAREDIS-315
	public void pExpireAtShouldBeSetCorrectly() {

		nativeConnection.set(KEY_1, VALUE_1);

		clusterConnection.pExpireAt(KEY_1_BYTES, System.currentTimeMillis() + 5000);

		assertThat(nativeConnection.ttl(LettuceConverters.toString(KEY_1_BYTES))).isGreaterThan(1);
	}

	@Test // DATAREDIS-315
	public void persistShoudRemoveTTL() {

		nativeConnection.setex(KEY_1, 10, VALUE_1);

		assertThat(clusterConnection.persist(KEY_1_BYTES)).isEqualTo(Boolean.TRUE);
		assertThat(nativeConnection.ttl(KEY_1)).isEqualTo(-1L);
	}

	@Test(expected = UnsupportedOperationException.class) // DATAREDIS-315
	public void moveShouldNotBeSupported() {
		clusterConnection.move(KEY_1_BYTES, 3);
	}

	@Test // DATAREDIS-315
	public void dbSizeShouldReturnCummulatedDbSize() {

		nativeConnection.set(KEY_1, VALUE_1);
		nativeConnection.set(KEY_2, VALUE_2);

		assertThat(clusterConnection.dbSize()).isEqualTo(2L);
	}

	@Test // DATAREDIS-315
	public void dbSizeForSpecificNodeShouldGetNodeDbSize() {

		nativeConnection.set(KEY_1, VALUE_1);
		nativeConnection.set(KEY_2, VALUE_2);

		assertThat(clusterConnection.dbSize(new RedisClusterNode("127.0.0.1", 7379, null))).isEqualTo(1L);
		assertThat(clusterConnection.dbSize(new RedisClusterNode("127.0.0.1", 7380, null))).isEqualTo(1L);
		assertThat(clusterConnection.dbSize(new RedisClusterNode("127.0.0.1", 7381, null))).isEqualTo(0L);
	}

	@Test // DATAREDIS-315
	public void ttlShouldReturnMinusTwoWhenKeyDoesNotExist() {
		assertThat(clusterConnection.ttl(KEY_1_BYTES)).isEqualTo(-2L);
	}

	@Test // DATAREDIS-315
	public void ttlShouldReturnMinusOneWhenKeyDoesNotHaveExpirationSet() {

		nativeConnection.set(KEY_1, VALUE_1);

		assertThat(clusterConnection.ttl(KEY_1_BYTES)).isEqualTo(-1L);
	}

	@Test // DATAREDIS-315
	public void ttlShouldReturnValueCorrectly() {

		nativeConnection.set(KEY_1, VALUE_1);
		nativeConnection.expire(KEY_1, 5);

		assertThat(clusterConnection.ttl(KEY_1_BYTES)).isGreaterThan(1);
	}

	@Test // DATAREDIS-315
	public void pTtlShouldReturnMinusTwoWhenKeyDoesNotExist() {
		assertThat(clusterConnection.pTtl(KEY_1_BYTES)).isEqualTo(-2L);
	}

	@Test // DATAREDIS-315
	public void pTtlShouldReturnMinusOneWhenKeyDoesNotHaveExpirationSet() {

		nativeConnection.set(KEY_1, VALUE_1);

		assertThat(clusterConnection.pTtl(KEY_1_BYTES)).isEqualTo(-1L);
	}

	@Test // DATAREDIS-315
	public void pTtlShouldReturValueCorrectly() {

		nativeConnection.set(KEY_1, VALUE_1);
		nativeConnection.expire(KEY_1, 5);

		assertThat(clusterConnection.pTtl(KEY_1_BYTES)).isGreaterThan(1);
	}

	@Test // DATAREDIS-315
	public void sortShouldReturnValuesCorrectly() {

		nativeConnection.lpush(KEY_1, VALUE_2, VALUE_1);

		assertThat(clusterConnection.sort(KEY_1_BYTES, new DefaultSortParameters().alpha())).contains(VALUE_1_BYTES, VALUE_2_BYTES);
	}

	@Test // DATAREDIS-315
	public void sortAndStoreShouldAddSortedValuesValuesCorrectly() {

		nativeConnection.lpush(KEY_1, VALUE_2, VALUE_1);

		assertThat(clusterConnection.sort(KEY_1_BYTES, new DefaultSortParameters().alpha(), KEY_2_BYTES)).isEqualTo(1L);
		assertThat(nativeConnection.exists(KEY_2)).isTrue();
	}

	@Test // DATAREDIS-315
	public void sortAndStoreShouldReturnZeroWhenListDoesNotExist() {
		assertThat(clusterConnection.sort(KEY_1_BYTES, new DefaultSortParameters().alpha(), KEY_2_BYTES)).isEqualTo(0L);
	}

	@Test // DATAREDIS-315
	public void dumpAndRestoreShouldWorkCorrectly() {

		nativeConnection.set(KEY_1, VALUE_1);

		byte[] dumpedValue = clusterConnection.dump(KEY_1_BYTES);
		clusterConnection.restore(KEY_2_BYTES, 0, dumpedValue);

		assertThat(nativeConnection.get(KEY_2)).isEqualTo(VALUE_1);
	}

	@Test // DATAREDIS-315
	public void getShouldReturnValueCorrectly() {

		nativeConnection.set(KEY_1, VALUE_1);

		assertThat(clusterConnection.get(KEY_1_BYTES)).isEqualTo(VALUE_1_BYTES);
	}

	@Test // DATAREDIS-315
	public void getSetShouldWorkCorrectly() {

		nativeConnection.set(KEY_1, VALUE_1);

		byte[] valueBeforeSet = clusterConnection.getSet(KEY_1_BYTES, VALUE_2_BYTES);

		assertThat(valueBeforeSet).isEqualTo(VALUE_1_BYTES);
		assertThat(nativeConnection.get(KEY_1)).isEqualTo(VALUE_2);
	}

	@Test // DATAREDIS-315
	public void mGetShouldReturnCorrectlyWhenKeysMapToSameSlot() {

		nativeConnection.set(SAME_SLOT_KEY_1, VALUE_1);
		nativeConnection.set(SAME_SLOT_KEY_2, VALUE_2);

		assertThat(clusterConnection.mGet(SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES)).contains(VALUE_1_BYTES, VALUE_2_BYTES);
	}

	@Test // DATAREDIS-315
	public void mGetShouldReturnCorrectlyWhenKeysDoNotMapToSameSlot() {

		nativeConnection.set(KEY_1, VALUE_1);
		nativeConnection.set(KEY_2, VALUE_2);

		assertThat(clusterConnection.mGet(KEY_1_BYTES, KEY_2_BYTES)).contains(VALUE_1_BYTES, VALUE_2_BYTES);
	}

	@Test // DATAREDIS-315
	public void setShouldSetValueCorrectly() {

		clusterConnection.set(KEY_1_BYTES, VALUE_1_BYTES);

		assertThat(nativeConnection.get(KEY_1)).isEqualTo(VALUE_1);
	}

	@Test // DATAREDIS-315
	public void setNxShouldSetValueCorrectly() {

		clusterConnection.setNX(KEY_1_BYTES, VALUE_1_BYTES);

		assertThat(nativeConnection.get(KEY_1)).isEqualTo(VALUE_1);
	}

	@Test // DATAREDIS-315
	public void setNxShouldNotSetValueWhenAlreadyExistsInDBCorrectly() {

		nativeConnection.set(KEY_1, VALUE_1);

		clusterConnection.setNX(KEY_1_BYTES, VALUE_2_BYTES);

		assertThat(nativeConnection.get(KEY_1)).isEqualTo(VALUE_1);
	}

	@Test // DATAREDIS-315
	public void setExShouldSetValueCorrectly() {

		clusterConnection.setEx(KEY_1_BYTES, 5, VALUE_1_BYTES);

		assertThat(nativeConnection.get(KEY_1)).isEqualTo(VALUE_1);
		assertThat(nativeConnection.ttl(KEY_1)).isGreaterThan(1);
	}

	@Test // DATAREDIS-315
	public void pSetExShouldSetValueCorrectly() {

		clusterConnection.pSetEx(KEY_1_BYTES, 5000, VALUE_1_BYTES);

		assertThat(nativeConnection.get(KEY_1)).isEqualTo(VALUE_1);
		assertThat(nativeConnection.ttl(KEY_1)).isGreaterThan(1);
	}

	@Test // DATAREDIS-315
	public void mSetShouldWorkWhenKeysMapToSameSlot() {

		Map<byte[], byte[]> map = new LinkedHashMap<byte[], byte[]>();
		map.put(SAME_SLOT_KEY_1_BYTES, VALUE_1_BYTES);
		map.put(SAME_SLOT_KEY_2_BYTES, VALUE_2_BYTES);

		clusterConnection.mSet(map);

		assertThat(nativeConnection.get(SAME_SLOT_KEY_1)).isEqualTo(VALUE_1);
		assertThat(nativeConnection.get(SAME_SLOT_KEY_2)).isEqualTo(VALUE_2);
	}

	@Test // DATAREDIS-315
	public void mSetShouldWorkWhenKeysDoNotMapToSameSlot() {

		Map<byte[], byte[]> map = new LinkedHashMap<byte[], byte[]>();
		map.put(KEY_1_BYTES, VALUE_1_BYTES);
		map.put(KEY_2_BYTES, VALUE_2_BYTES);

		clusterConnection.mSet(map);

		assertThat(nativeConnection.get(KEY_1)).isEqualTo(VALUE_1);
		assertThat(nativeConnection.get(KEY_2)).isEqualTo(VALUE_2);
	}

	@Test // DATAREDIS-315
	public void mSetNXShouldReturnTrueIfAllKeysSet() {

		Map<byte[], byte[]> map = new LinkedHashMap<byte[], byte[]>();
		map.put(KEY_1_BYTES, VALUE_1_BYTES);
		map.put(KEY_2_BYTES, VALUE_2_BYTES);

		assertThat(clusterConnection.mSetNX(map)).isTrue();

		assertThat(nativeConnection.get(KEY_1)).isEqualTo(VALUE_1);
		assertThat(nativeConnection.get(KEY_2)).isEqualTo(VALUE_2);
	}

	@Test // DATAREDIS-315
	public void mSetNXShouldReturnFalseIfNotAllKeysSet() {

		nativeConnection.set(KEY_2, VALUE_3);
		Map<byte[], byte[]> map = new LinkedHashMap<byte[], byte[]>();
		map.put(KEY_1_BYTES, VALUE_1_BYTES);
		map.put(KEY_2_BYTES, VALUE_2_BYTES);

		assertThat(clusterConnection.mSetNX(map)).isFalse();

		assertThat(nativeConnection.get(KEY_1)).isEqualTo(VALUE_1);
		assertThat(nativeConnection.get(KEY_2)).isEqualTo(VALUE_3);
	}

	@Test // DATAREDIS-315
	public void mSetNXShouldWorkForOnSameSlotKeys() {

		Map<byte[], byte[]> map = new LinkedHashMap<byte[], byte[]>();
		map.put(SAME_SLOT_KEY_1_BYTES, VALUE_1_BYTES);
		map.put(SAME_SLOT_KEY_2_BYTES, VALUE_2_BYTES);

		assertThat(clusterConnection.mSetNX(map)).isTrue();

		assertThat(nativeConnection.get(SAME_SLOT_KEY_1)).isEqualTo(VALUE_1);
		assertThat(nativeConnection.get(SAME_SLOT_KEY_2)).isEqualTo(VALUE_2);
	}

	@Test // DATAREDIS-315
	public void incrShouldIncreaseValueCorrectly() {

		nativeConnection.set(KEY_1, "1");

		assertThat(clusterConnection.incr(KEY_1_BYTES)).isEqualTo(2L);
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
	public void decrShouldDecreaseValueCorrectly() {

		nativeConnection.set(KEY_1, "5");

		assertThat(clusterConnection.decr(KEY_1_BYTES)).isEqualTo(4L);
	}

	@Test // DATAREDIS-315
	public void decrByShouldDecreaseValueCorrectly() {

		nativeConnection.set(KEY_1, "5");

		assertThat(clusterConnection.decrBy(KEY_1_BYTES, 4)).isEqualTo(1L);
	}

	@Test // DATAREDIS-315
	public void appendShouldAddValueCorrectly() {

		clusterConnection.append(KEY_1_BYTES, VALUE_1_BYTES);
		clusterConnection.append(KEY_1_BYTES, VALUE_2_BYTES);

		assertThat(nativeConnection.get(KEY_1)).isEqualTo(VALUE_1.concat(VALUE_2));
	}

	@Test // DATAREDIS-315
	public void getRangeShouldReturnValueCorrectly() {

		nativeConnection.set(KEY_1, VALUE_1);

		assertThat(clusterConnection.getRange(KEY_1_BYTES, 0, 2)).isEqualTo(toBytes("val"));
	}

	@Test // DATAREDIS-315
	public void setRangeShouldWorkCorrectly() {

		nativeConnection.set(KEY_1, VALUE_1);

		clusterConnection.setRange(KEY_1_BYTES, toBytes("UE"), 3);

		assertThat(nativeConnection.get(KEY_1)).isEqualTo("valUE1");
	}

	@Test // DATAREDIS-315
	public void getBitShouldWorkCorrectly() {

		nativeConnection.setbit(KEY_1, 0, 1);
		nativeConnection.setbit(KEY_1, 1, 0);

		assertThat(clusterConnection.getBit(KEY_1_BYTES, 0)).isTrue();
		assertThat(clusterConnection.getBit(KEY_1_BYTES, 1)).isFalse();
	}

	@Test // DATAREDIS-315
	public void setBitShouldWorkCorrectly() {

		clusterConnection.setBit(KEY_1_BYTES, 0, true);
		clusterConnection.setBit(KEY_1_BYTES, 1, false);

		assertThat(nativeConnection.getbit(KEY_1, 0)).isEqualTo(1L);
		assertThat(nativeConnection.getbit(KEY_1, 1)).isEqualTo(0L);
	}

	@Test // DATAREDIS-315
	public void bitCountShouldWorkCorrectly() {

		nativeConnection.setbit(KEY_1, 0, 1);
		nativeConnection.setbit(KEY_1, 1, 0);

		assertThat(clusterConnection.bitCount(KEY_1_BYTES)).isEqualTo(1L);
	}

	@Test // DATAREDIS-315
	public void bitCountWithRangeShouldWorkCorrectly() {

		nativeConnection.setbit(KEY_1, 0, 1);
		nativeConnection.setbit(KEY_1, 1, 0);
		nativeConnection.setbit(KEY_1, 2, 1);
		nativeConnection.setbit(KEY_1, 3, 0);
		nativeConnection.setbit(KEY_1, 4, 1);

		assertThat(clusterConnection.bitCount(KEY_1_BYTES, 0, 3)).isEqualTo(3L);
	}

	@Test // DATAREDIS-315
	public void bitOpShouldWorkCorrectly() {

		nativeConnection.set(SAME_SLOT_KEY_1, "foo");
		nativeConnection.set(SAME_SLOT_KEY_2, "bar");

		clusterConnection.bitOp(BitOperation.AND, SAME_SLOT_KEY_3_BYTES, SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES);

		assertThat(nativeConnection.get(SAME_SLOT_KEY_3)).isEqualTo("bab");
	}

	@Test(expected = DataAccessException.class) // DATAREDIS-315
	public void bitOpShouldThrowExceptionWhenKeysDoNotMapToSameSlot() {
		clusterConnection.bitOp(BitOperation.AND, KEY_1_BYTES, KEY_2_BYTES, KEY_3_BYTES);
	}

	@Test // DATAREDIS-315
	public void strLenShouldWorkCorrectly() {

		nativeConnection.set(KEY_1, VALUE_1);

		assertThat(clusterConnection.strLen(KEY_1_BYTES)).isEqualTo(6L);
	}

	@Test // DATAREDIS-315
	public void rPushShoultAddValuesCorrectly() {

		clusterConnection.rPush(KEY_1_BYTES, VALUE_1_BYTES, VALUE_2_BYTES);

		assertThat(nativeConnection.lrange(KEY_1, 0, -1)).contains(VALUE_1, VALUE_2);
	}

	@Test // DATAREDIS-315
	public void lPushShoultAddValuesCorrectly() {

		clusterConnection.lPush(KEY_1_BYTES, VALUE_1_BYTES, VALUE_2_BYTES);

		assertThat(nativeConnection.lrange(KEY_1, 0, -1)).contains(VALUE_1, VALUE_2);
	}

	@Test // DATAREDIS-315
	public void rPushNXShoultNotAddValuesWhenKeyDoesNotExist() {

		clusterConnection.rPushX(KEY_1_BYTES, VALUE_1_BYTES);

		assertThat(nativeConnection.exists(KEY_1)).isFalse();
	}

	@Test // DATAREDIS-315
	public void lPushNXShoultNotAddValuesWhenKeyDoesNotExist() {

		clusterConnection.lPushX(KEY_1_BYTES, VALUE_1_BYTES);

		assertThat(nativeConnection.exists(KEY_1)).isFalse();
	}

	@Test // DATAREDIS-315
	public void lLenShouldCountValuesCorrectly() {

		nativeConnection.lpush(KEY_1, VALUE_1, VALUE_2);

		assertThat(clusterConnection.lLen(KEY_1_BYTES)).isEqualTo(2L);
	}

	@Test // DATAREDIS-315
	public void lRangeShouldGetValuesCorrectly() {

		nativeConnection.lpush(KEY_1, VALUE_1, VALUE_2);

		assertThat(clusterConnection.lRange(KEY_1_BYTES, 0L, -1L)).contains(VALUE_1_BYTES, VALUE_2_BYTES);
	}

	@Test // DATAREDIS-315
	public void lTrimShouldTrimListCorrectly() {

		nativeConnection.lpush(KEY_1, VALUE_1, VALUE_2, "foo", "bar");

		clusterConnection.lTrim(KEY_1_BYTES, 2, 3);

		assertThat(nativeConnection.lrange(KEY_1, 0, -1)).contains(VALUE_1, VALUE_2);
	}

	@Test // DATAREDIS-315
	public void lIndexShouldGetElementAtIndexCorrectly() {

		nativeConnection.rpush(KEY_1, VALUE_1, VALUE_2, "foo", "bar");

		assertThat(clusterConnection.lIndex(KEY_1_BYTES, 1)).isEqualTo(VALUE_2_BYTES);
	}

	@Test // DATAREDIS-315
	public void lInsertShouldAddElementAtPositionCorrectly() {

		nativeConnection.rpush(KEY_1, VALUE_1, VALUE_2, "foo", "bar");

		clusterConnection.lInsert(KEY_1_BYTES, Position.AFTER, VALUE_2_BYTES, toBytes("booh!"));

		assertThat(nativeConnection.lrange(KEY_1, 0, -1).get(2)).isEqualTo("booh!");
	}

	@Test // DATAREDIS-315
	public void lSetShouldSetElementAtPositionCorrectly() {

		nativeConnection.rpush(KEY_1, VALUE_1, VALUE_2, "foo", "bar");

		clusterConnection.lSet(KEY_1_BYTES, 1L, VALUE_1_BYTES);

		assertThat(nativeConnection.lrange(KEY_1, 0, -1).get(1)).isEqualTo(VALUE_1);
	}

	@Test // DATAREDIS-315
	public void lRemShouldRemoveElementAtPositionCorrectly() {

		nativeConnection.rpush(KEY_1, VALUE_1, VALUE_2, "foo", "bar");

		clusterConnection.lRem(KEY_1_BYTES, 1L, VALUE_1_BYTES);

		assertThat(nativeConnection.llen(KEY_1)).isEqualTo(3L);
	}

	@Test // DATAREDIS-315
	public void lPopShouldReturnElementCorrectly() {

		nativeConnection.rpush(KEY_1, VALUE_1, VALUE_2);

		assertThat(clusterConnection.lPop(KEY_1_BYTES)).isEqualTo(VALUE_1_BYTES);
	}

	@Test // DATAREDIS-315
	public void rPopShouldReturnElementCorrectly() {

		nativeConnection.rpush(KEY_1, VALUE_1, VALUE_2);

		assertThat(clusterConnection.rPop(KEY_1_BYTES)).isEqualTo(VALUE_2_BYTES);
	}

	@Test // DATAREDIS-315
	public void blPopShouldPopElementCorectly() {

		nativeConnection.lpush(KEY_1, VALUE_1, VALUE_2);
		nativeConnection.lpush(KEY_2, VALUE_3);

		assertThat(clusterConnection.bLPop(100, KEY_1_BYTES, KEY_2_BYTES).size()).isEqualTo(2);
	}

	@Test // DATAREDIS-315
	public void blPopShouldPopElementCorectlyWhenKeyOnSameSlot() {

		nativeConnection.lpush(SAME_SLOT_KEY_1, VALUE_1, VALUE_2);
		nativeConnection.lpush(SAME_SLOT_KEY_2, VALUE_3);

		assertThat(clusterConnection.bLPop(100, SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES).size()).isEqualTo(2);
	}

	@Test // DATAREDIS-315
	public void brPopShouldPopElementCorectly() {

		nativeConnection.lpush(KEY_1, VALUE_1, VALUE_2);
		nativeConnection.lpush(KEY_2, VALUE_3);

		assertThat(clusterConnection.bRPop(100, KEY_1_BYTES, KEY_2_BYTES).size()).isEqualTo(2);
	}

	@Test // DATAREDIS-315
	public void brPopShouldPopElementCorectlyWhenKeyOnSameSlot() {

		nativeConnection.lpush(SAME_SLOT_KEY_1, VALUE_1, VALUE_2);
		nativeConnection.lpush(SAME_SLOT_KEY_2, VALUE_3);

		assertThat(clusterConnection.bRPop(100, SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES).size()).isEqualTo(2);
	}

	@Test // DATAREDIS-315
	public void rPopLPushShouldWorkWhenDoNotMapToSameSlot() {

		nativeConnection.lpush(KEY_1, VALUE_1, VALUE_2);
		nativeConnection.lpush(KEY_2, VALUE_3);

		assertThat(clusterConnection.bLPop(100, KEY_1_BYTES, KEY_2_BYTES).size()).isEqualTo(2);
	}

	@Test // DATAREDIS-315
	public void bRPopLPushShouldWork() {

		nativeConnection.lpush(KEY_1, VALUE_1, VALUE_2);

		assertThat(clusterConnection.bRPopLPush(0, KEY_1_BYTES, KEY_2_BYTES)).isEqualTo(VALUE_1_BYTES);
		assertThat(nativeConnection.exists(KEY_2)).isTrue();
	}

	@Test // DATAREDIS-315
	public void bRPopLPushShouldWorkOnSameSlotKeys() {

		nativeConnection.lpush(SAME_SLOT_KEY_1, VALUE_1, VALUE_2);

		assertThat(clusterConnection.bRPopLPush(0, SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES)).isEqualTo(VALUE_1_BYTES);
		assertThat(nativeConnection.exists(SAME_SLOT_KEY_2)).isTrue();
	}

	@Test // DATAREDIS-315
	public void rPopLPushShouldWorkWhenKeysOnSameSlot() {

		nativeConnection.lpush(SAME_SLOT_KEY_1, VALUE_1, VALUE_2);

		assertThat(clusterConnection.rPopLPush(SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES)).isEqualTo(VALUE_1_BYTES);
		assertThat(nativeConnection.exists(SAME_SLOT_KEY_2)).isTrue();
	}

	@Test // DATAREDIS-315
	public void sAddShouldAddValueToSetCorrectly() {

		clusterConnection.sAdd(KEY_1_BYTES, VALUE_1_BYTES, VALUE_2_BYTES);

		assertThat(nativeConnection.smembers(KEY_1)).contains(VALUE_1, VALUE_2);
	}

	@Test // DATAREDIS-315
	public void sRemShouldRemoveValueFromSetCorrectly() {

		nativeConnection.sadd(KEY_1, VALUE_1, VALUE_2);

		clusterConnection.sRem(KEY_1_BYTES, VALUE_2_BYTES);

		assertThat(nativeConnection.smembers(KEY_1)).contains(VALUE_1);
	}

	@Test // DATAREDIS-315
	public void sPopShouldPopValueFromSetCorrectly() {

		nativeConnection.sadd(KEY_1, VALUE_1, VALUE_2);

		assertThat(clusterConnection.sPop(KEY_1_BYTES)).isNotNull();
	}

	@Test // DATAREDIS-315
	public void sMoveShouldWorkWhenKeysMapToSameSlot() {

		nativeConnection.sadd(SAME_SLOT_KEY_1, VALUE_1, VALUE_2);
		nativeConnection.sadd(SAME_SLOT_KEY_2, VALUE_3);

		clusterConnection.sMove(SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES, VALUE_2_BYTES);

		assertThat(nativeConnection.sismember(SAME_SLOT_KEY_1, VALUE_2)).isFalse();
		assertThat(nativeConnection.sismember(SAME_SLOT_KEY_2, VALUE_2)).isTrue();
	}

	@Test // DATAREDIS-315
	public void sMoveShouldWorkWhenKeysDoNotMapToSameSlot() {

		nativeConnection.sadd(KEY_1, VALUE_1, VALUE_2);
		nativeConnection.sadd(KEY_2, VALUE_3);

		clusterConnection.sMove(KEY_1_BYTES, KEY_2_BYTES, VALUE_2_BYTES);

		assertThat(nativeConnection.sismember(KEY_1, VALUE_2)).isFalse();
		assertThat(nativeConnection.sismember(KEY_2, VALUE_2)).isTrue();
	}

	@Test // DATAREDIS-315
	public void sCardShouldCountValuesInSetCorrectly() {

		nativeConnection.sadd(KEY_1, VALUE_1, VALUE_2);

		assertThat(clusterConnection.sCard(KEY_1_BYTES)).isEqualTo(2L);
	}

	@Test // DATAREDIS-315
	public void sIsMemberShouldReturnTrueIfValueIsMemberOfSet() {

		nativeConnection.sadd(KEY_1, VALUE_1, VALUE_2);

		assertThat(clusterConnection.sIsMember(KEY_1_BYTES, VALUE_1_BYTES)).isTrue();
	}

	@Test // DATAREDIS-315
	public void sIsMemberShouldReturnFalseIfValueIsMemberOfSet() {

		nativeConnection.sadd(KEY_1, VALUE_1, VALUE_2);

		assertThat(clusterConnection.sIsMember(KEY_1_BYTES, toBytes("foo"))).isFalse();
	}

	@Test // DATAREDIS-315
	public void sInterShouldWorkForKeysMappingToSameSlot() {

		nativeConnection.sadd(SAME_SLOT_KEY_1, VALUE_1, VALUE_2);
		nativeConnection.sadd(SAME_SLOT_KEY_2, VALUE_2, VALUE_3);

		assertThat(clusterConnection.sInter(SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES)).contains(VALUE_2_BYTES);
	}

	@Test // DATAREDIS-315
	public void sInterShouldWorkForKeysNotMappingToSameSlot() {

		nativeConnection.sadd(KEY_1, VALUE_1, VALUE_2);
		nativeConnection.sadd(KEY_2, VALUE_2, VALUE_3);

		assertThat(clusterConnection.sInter(KEY_1_BYTES, KEY_2_BYTES)).contains(VALUE_2_BYTES);
	}

	@Test // DATAREDIS-315
	public void sInterStoreShouldWorkForKeysMappingToSameSlot() {

		nativeConnection.sadd(SAME_SLOT_KEY_1, VALUE_1, VALUE_2);
		nativeConnection.sadd(SAME_SLOT_KEY_2, VALUE_2, VALUE_3);

		clusterConnection.sInterStore(SAME_SLOT_KEY_3_BYTES, SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES);

		assertThat(nativeConnection.smembers(SAME_SLOT_KEY_3)).contains(VALUE_2);
	}

	@Test // DATAREDIS-315
	public void sInterStoreShouldWorkForKeysNotMappingToSameSlot() {

		nativeConnection.sadd(KEY_1, VALUE_1, VALUE_2);
		nativeConnection.sadd(KEY_2, VALUE_2, VALUE_3);

		clusterConnection.sInterStore(KEY_3_BYTES, KEY_1_BYTES, KEY_2_BYTES);

		assertThat(nativeConnection.smembers(KEY_3)).contains(VALUE_2);
	}

	@Test // DATAREDIS-315
	public void sUnionShouldWorkForKeysMappingToSameSlot() {

		nativeConnection.sadd(SAME_SLOT_KEY_1, VALUE_1, VALUE_2);
		nativeConnection.sadd(SAME_SLOT_KEY_2, VALUE_2, VALUE_3);

		assertThat(clusterConnection.sUnion(SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES)).contains(VALUE_1_BYTES, VALUE_2_BYTES, VALUE_3_BYTES);
	}

	@Test // DATAREDIS-315
	public void sUnionShouldWorkForKeysNotMappingToSameSlot() {

		nativeConnection.sadd(KEY_1, VALUE_1, VALUE_2);
		nativeConnection.sadd(KEY_2, VALUE_2, VALUE_3);

		assertThat(clusterConnection.sUnion(KEY_1_BYTES, KEY_2_BYTES)).contains(VALUE_1_BYTES, VALUE_2_BYTES, VALUE_3_BYTES);
	}

	@Test // DATAREDIS-315
	public void sUnionStoreShouldWorkForKeysMappingToSameSlot() {

		nativeConnection.sadd(SAME_SLOT_KEY_1, VALUE_1, VALUE_2);
		nativeConnection.sadd(SAME_SLOT_KEY_2, VALUE_2, VALUE_3);

		clusterConnection.sUnionStore(SAME_SLOT_KEY_3_BYTES, SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES);

		assertThat(nativeConnection.smembers(SAME_SLOT_KEY_3)).contains(VALUE_1, VALUE_2, VALUE_3);
	}

	@Test // DATAREDIS-315
	public void sUnionStoreShouldWorkForKeysNotMappingToSameSlot() {

		nativeConnection.sadd(KEY_1, VALUE_1, VALUE_2);
		nativeConnection.sadd(KEY_2, VALUE_2, VALUE_3);

		clusterConnection.sUnionStore(KEY_3_BYTES, KEY_1_BYTES, KEY_2_BYTES);

		assertThat(nativeConnection.smembers(KEY_3)).contains(VALUE_1, VALUE_2, VALUE_3);
	}

	@Test // DATAREDIS-315
	public void sDiffShouldWorkWhenKeysMapToSameSlot() {

		nativeConnection.sadd(SAME_SLOT_KEY_1, VALUE_1, VALUE_2);
		nativeConnection.sadd(SAME_SLOT_KEY_2, VALUE_2, VALUE_3);

		assertThat(clusterConnection.sDiff(SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES)).contains(VALUE_1_BYTES);
	}

	@Test // DATAREDIS-315
	public void sDiffShouldWorkWhenKeysNotMapToSameSlot() {

		nativeConnection.sadd(KEY_1, VALUE_1, VALUE_2);
		nativeConnection.sadd(KEY_2, VALUE_2, VALUE_3);

		assertThat(clusterConnection.sDiff(KEY_1_BYTES, KEY_2_BYTES)).contains(VALUE_1_BYTES);
	}

	@Test // DATAREDIS-315
	public void sDiffStoreShouldWorkWhenKeysMapToSameSlot() {

		nativeConnection.sadd(SAME_SLOT_KEY_1, VALUE_1, VALUE_2);
		nativeConnection.sadd(SAME_SLOT_KEY_2, VALUE_2, VALUE_3);

		clusterConnection.sDiffStore(SAME_SLOT_KEY_3_BYTES, SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES);

		assertThat(nativeConnection.smembers(SAME_SLOT_KEY_3)).contains(VALUE_1);
	}

	@Test // DATAREDIS-315
	public void sDiffStoreShouldWorkWhenKeysNotMapToSameSlot() {

		nativeConnection.sadd(KEY_1, VALUE_1, VALUE_2);
		nativeConnection.sadd(KEY_2, VALUE_2, VALUE_3);

		clusterConnection.sDiffStore(KEY_3_BYTES, KEY_1_BYTES, KEY_2_BYTES);

		assertThat(nativeConnection.smembers(KEY_3)).contains(VALUE_1);
	}

	@Test // DATAREDIS-315
	public void sMembersShouldReturnValuesContainedInSetCorrectly() {

		nativeConnection.sadd(KEY_1, VALUE_1, VALUE_2);

		assertThat(clusterConnection.sMembers(KEY_1_BYTES)).contains(VALUE_1_BYTES, VALUE_2_BYTES);
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
	public void sscanShouldRetrieveAllValuesInSetCorrectly() {

		for (int i = 0; i < 30; i++) {
			nativeConnection.sadd(KEY_1, Integer.valueOf(i).toString());
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
	public void zAddShouldAddValueWithScoreCorrectly() {

		clusterConnection.zAdd(KEY_1_BYTES, 10D, VALUE_1_BYTES);
		clusterConnection.zAdd(KEY_1_BYTES, 20D, VALUE_2_BYTES);

		assertThat(nativeConnection.zcard(KEY_1)).isEqualTo(2L);
	}

	@Test // DATAREDIS-315
	public void zRemShouldRemoveValueWithScoreCorrectly() {

		nativeConnection.zadd(KEY_1, 10D, VALUE_1);
		nativeConnection.zadd(KEY_1, 20D, VALUE_2);

		clusterConnection.zRem(KEY_1_BYTES, VALUE_1_BYTES);

		assertThat(nativeConnection.zcard(KEY_1)).isEqualTo(1L);
	}

	@Test // DATAREDIS-315
	public void zIncrByShouldIncScoreForValueCorrectly() {

		nativeConnection.zadd(KEY_1, 10D, VALUE_1);
		nativeConnection.zadd(KEY_1, 20D, VALUE_2);

		clusterConnection.zIncrBy(KEY_1_BYTES, 100D, VALUE_1_BYTES);

		assertThat(nativeConnection.zrank(KEY_1, VALUE_1)).isEqualTo(1L);
	}

	@Test // DATAREDIS-315
	public void zRankShouldReturnPositionForValueCorrectly() {

		nativeConnection.zadd(KEY_1, 10D, VALUE_1);
		nativeConnection.zadd(KEY_1, 20D, VALUE_2);

		assertThat(clusterConnection.zRank(KEY_1_BYTES, VALUE_2_BYTES)).isEqualTo(1L);
	}

	@Test // DATAREDIS-315
	public void zRankShouldReturnReversePositionForValueCorrectly() {

		nativeConnection.zadd(KEY_1, 10D, VALUE_1);
		nativeConnection.zadd(KEY_1, 20D, VALUE_2);

		assertThat(clusterConnection.zRevRank(KEY_1_BYTES, VALUE_2_BYTES)).isEqualTo(0L);
	}

	@Test // DATAREDIS-315
	public void zRangeShouldReturnValuesCorrectly() {

		nativeConnection.zadd(KEY_1, 10D, VALUE_1);
		nativeConnection.zadd(KEY_1, 20D, VALUE_2);
		nativeConnection.zadd(KEY_1, 5D, VALUE_3);

		assertThat(clusterConnection.zRange(KEY_1_BYTES, 1, 2)).contains(VALUE_1_BYTES, VALUE_2_BYTES);
	}

	@Test // DATAREDIS-315
	public void zRangeWithScoresShouldReturnValuesAndScoreCorrectly() {

		nativeConnection.zadd(KEY_1, 10D, VALUE_1);
		nativeConnection.zadd(KEY_1, 20D, VALUE_2);
		nativeConnection.zadd(KEY_1, 5D, VALUE_3);

		assertThat(clusterConnection.zRangeWithScores(KEY_1_BYTES, 1, 2)).contains(new DefaultTuple(VALUE_1_BYTES, 10D), new DefaultTuple(VALUE_2_BYTES, 20D));
	}

	@Test // DATAREDIS-315
	public void zRangeByScoreShouldReturnValuesCorrectly() {

		nativeConnection.zadd(KEY_1, 10D, VALUE_1);
		nativeConnection.zadd(KEY_1, 20D, VALUE_2);
		nativeConnection.zadd(KEY_1, 5D, VALUE_3);

		assertThat(clusterConnection.zRangeByScore(KEY_1_BYTES, 10, 20)).contains(VALUE_1_BYTES, VALUE_2_BYTES);
	}

	@Test // DATAREDIS-315
	public void zRangeByScoreWithScoresShouldReturnValuesAndScoreCorrectly() {

		nativeConnection.zadd(KEY_1, 10D, VALUE_1);
		nativeConnection.zadd(KEY_1, 20D, VALUE_2);
		nativeConnection.zadd(KEY_1, 5D, VALUE_3);

		assertThat(clusterConnection.zRangeByScoreWithScores(KEY_1_BYTES, 10, 20)).contains(new DefaultTuple(VALUE_1_BYTES, 10D), new DefaultTuple(VALUE_2_BYTES, 20D));
	}

	@Test // DATAREDIS-315
	public void zRangeByScoreShouldReturnValuesCorrectlyWhenGivenOffsetAndScore() {

		nativeConnection.zadd(KEY_1, 10D, VALUE_1);
		nativeConnection.zadd(KEY_1, 20D, VALUE_2);
		nativeConnection.zadd(KEY_1, 5D, VALUE_3);

		assertThat(clusterConnection.zRangeByScore(KEY_1_BYTES, 10D, 20D, 0L, 1L)).contains(VALUE_1_BYTES);
	}

	@Test // DATAREDIS-315
	public void zRangeByScoreWithScoresShouldReturnValuesCorrectlyWhenGivenOffsetAndScore() {

		nativeConnection.zadd(KEY_1, 10D, VALUE_1);
		nativeConnection.zadd(KEY_1, 20D, VALUE_2);
		nativeConnection.zadd(KEY_1, 5D, VALUE_3);

		assertThat(clusterConnection.zRangeByScoreWithScores(KEY_1_BYTES, 10D, 20D, 0L, 1L)).contains( new DefaultTuple(VALUE_1_BYTES, 10D));
	}

	@Test // DATAREDIS-315
	public void zRevRangeShouldReturnValuesCorrectly() {

		nativeConnection.zadd(KEY_1, 10D, VALUE_1);
		nativeConnection.zadd(KEY_1, 20D, VALUE_2);
		nativeConnection.zadd(KEY_1, 5D, VALUE_3);

		assertThat(clusterConnection.zRevRange(KEY_1_BYTES, 1, 2)).contains(VALUE_3_BYTES, VALUE_1_BYTES);
	}

	@Test // DATAREDIS-315
	public void zRevRangeWithScoresShouldReturnValuesAndScoreCorrectly() {

		nativeConnection.zadd(KEY_1, 10D, VALUE_1);
		nativeConnection.zadd(KEY_1, 20D, VALUE_2);
		nativeConnection.zadd(KEY_1, 5D, VALUE_3);

		assertThat(clusterConnection.zRevRangeWithScores(KEY_1_BYTES, 1, 2)).contains(new DefaultTuple(VALUE_3_BYTES, 5D), new DefaultTuple(VALUE_1_BYTES, 10D));
	}

	@Test // DATAREDIS-315
	public void zRevRangeByScoreShouldReturnValuesCorrectly() {

		nativeConnection.zadd(KEY_1, 10D, VALUE_1);
		nativeConnection.zadd(KEY_1, 20D, VALUE_2);
		nativeConnection.zadd(KEY_1, 5D, VALUE_3);

		assertThat(clusterConnection.zRevRangeByScore(KEY_1_BYTES, 10D, 20D)).contains(VALUE_2_BYTES, VALUE_1_BYTES);
	}

	@Test // DATAREDIS-315
	public void zRevRangeByScoreWithScoresShouldReturnValuesAndScoreCorrectly() {

		nativeConnection.zadd(KEY_1, 10D, VALUE_1);
		nativeConnection.zadd(KEY_1, 20D, VALUE_2);
		nativeConnection.zadd(KEY_1, 5D, VALUE_3);

		assertThat(clusterConnection.zRevRangeByScoreWithScores(KEY_1_BYTES, 10D, 20D)).contains(new DefaultTuple(VALUE_2_BYTES, 20D), new DefaultTuple(VALUE_1_BYTES, 10D));
	}

	@Test // DATAREDIS-315
	public void zRevRangeByScoreShouldReturnValuesCorrectlyWhenGivenOffsetAndScore() {

		nativeConnection.zadd(KEY_1, 10D, VALUE_1);
		nativeConnection.zadd(KEY_1, 20D, VALUE_2);
		nativeConnection.zadd(KEY_1, 5D, VALUE_3);

		assertThat(clusterConnection.zRevRangeByScore(KEY_1_BYTES, 10D, 20D, 0L, 1L)).contains(VALUE_2_BYTES);
	}

	@Test // DATAREDIS-315
	public void zRevRangeByScoreWithScoresShouldReturnValuesCorrectlyWhenGivenOffsetAndScore() {

		nativeConnection.zadd(KEY_1, 10D, VALUE_1);
		nativeConnection.zadd(KEY_1, 20D, VALUE_2);
		nativeConnection.zadd(KEY_1, 5D, VALUE_3);

		assertThat(clusterConnection.zRevRangeByScoreWithScores(KEY_1_BYTES, 10D, 20D, 0L, 1L)).contains(new DefaultTuple(VALUE_2_BYTES, 20D));
	}

	@Test // DATAREDIS-315
	public void zCountShouldCountValuesInRange() {

		nativeConnection.zadd(KEY_1, 10D, VALUE_1);
		nativeConnection.zadd(KEY_1, 20D, VALUE_2);
		nativeConnection.zadd(KEY_1, 5D, VALUE_3);

		assertThat(clusterConnection.zCount(KEY_1_BYTES, 10, 20)).isEqualTo(2L);
	}

	@Test // DATAREDIS-315
	public void zCardShouldReturnTotalNumberOfValues() {

		nativeConnection.zadd(KEY_1, 10D, VALUE_1);
		nativeConnection.zadd(KEY_1, 20D, VALUE_2);
		nativeConnection.zadd(KEY_1, 5D, VALUE_3);

		assertThat(clusterConnection.zCard(KEY_1_BYTES)).isEqualTo(3L);
	}

	@Test // DATAREDIS-315
	public void zScoreShouldRetrieveScoreForValue() {

		nativeConnection.zadd(KEY_1, 10D, VALUE_1);
		nativeConnection.zadd(KEY_1, 20D, VALUE_2);

		assertThat(clusterConnection.zScore(KEY_1_BYTES, VALUE_2_BYTES)).isEqualTo(20D);
	}

	@Test // DATAREDIS-315
	public void zRemRangeShouldRemoveValues() {

		nativeConnection.zadd(KEY_1, 10D, VALUE_1);
		nativeConnection.zadd(KEY_1, 20D, VALUE_2);
		nativeConnection.zadd(KEY_1, 30D, VALUE_3);

		clusterConnection.zRemRange(KEY_1_BYTES, 1, 2);

		assertThat(nativeConnection.zcard(KEY_1)).isEqualTo(1L);
		assertThat(nativeConnection.zrange(KEY_1, 0, -1)).contains(VALUE_1);
	}

	@Test // DATAREDIS-315
	public void zRemRangeByScoreShouldRemoveValues() {

		nativeConnection.zadd(KEY_1, 10D, VALUE_1);
		nativeConnection.zadd(KEY_1, 20D, VALUE_2);
		nativeConnection.zadd(KEY_1, 30D, VALUE_3);

		clusterConnection.zRemRangeByScore(KEY_1_BYTES, 15D, 25D);

		assertThat(nativeConnection.zcard(KEY_1)).isEqualTo(2L);
		assertThat(nativeConnection.zrange(KEY_1, 0, -1)).contains(VALUE_1, VALUE_3);
	}

	@Test // DATAREDIS-315
	public void zUnionStoreShouldWorkForSameSlotKeys() {

		nativeConnection.zadd(SAME_SLOT_KEY_1, 10D, VALUE_1);
		nativeConnection.zadd(SAME_SLOT_KEY_1, 30D, VALUE_3);
		nativeConnection.zadd(SAME_SLOT_KEY_2, 20D, VALUE_2);

		clusterConnection.zUnionStore(SAME_SLOT_KEY_3_BYTES, SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES);

		assertThat(nativeConnection.zrange(SAME_SLOT_KEY_3, 0, -1)).contains(VALUE_1, VALUE_2, VALUE_3);
	}

	@Test(expected = DataAccessException.class) // DATAREDIS-315
	public void zUnionStoreShouldThrowExceptionWhenKeysDoNotMapToSameSlots() {
		clusterConnection.zUnionStore(KEY_3_BYTES, KEY_1_BYTES, KEY_2_BYTES);
	}

	@Test // DATAREDIS-315
	public void zInterStoreShouldWorkForSameSlotKeys() {

		nativeConnection.zadd(SAME_SLOT_KEY_1, 10D, VALUE_1);
		nativeConnection.zadd(SAME_SLOT_KEY_1, 20D, VALUE_2);

		nativeConnection.zadd(SAME_SLOT_KEY_2, 20D, VALUE_2);
		nativeConnection.zadd(SAME_SLOT_KEY_2, 30D, VALUE_3);

		clusterConnection.zInterStore(SAME_SLOT_KEY_3_BYTES, SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES);

		assertThat(nativeConnection.zrange(SAME_SLOT_KEY_3, 0, -1)).contains(VALUE_2);
	}

	@Test(expected = DataAccessException.class) // DATAREDIS-315
	public void zInterStoreShouldThrowExceptionWhenKeysDoNotMapToSameSlots() {
		clusterConnection.zInterStore(KEY_3_BYTES, KEY_1_BYTES, KEY_2_BYTES);
	}

	@Test
	public void zScanShouldReadEntireValueRange() {

		int nrOfValues = 321;
		for (int i = 0; i < nrOfValues; i++) {
			nativeConnection.zadd(KEY_1, i, "value-" + i);
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
	public void hSetShouldSetValueCorrectly() {

		clusterConnection.hSet(KEY_1_BYTES, KEY_2_BYTES, VALUE_1_BYTES);

		assertThat(nativeConnection.hget(KEY_1, KEY_2)).isEqualTo(VALUE_1);
	}

	@Test // DATAREDIS-315
	public void hSetNXShouldSetValueCorrectly() {

		clusterConnection.hSetNX(KEY_1_BYTES, KEY_2_BYTES, VALUE_1_BYTES);

		assertThat(nativeConnection.hget(KEY_1, KEY_2)).isEqualTo(VALUE_1);
	}

	@Test // DATAREDIS-315
	public void hSetNXShouldNotSetValueWhenAlreadyExists() {

		nativeConnection.hset(KEY_1, KEY_2, VALUE_1);

		clusterConnection.hSetNX(KEY_1_BYTES, KEY_2_BYTES, VALUE_2_BYTES);

		assertThat(nativeConnection.hget(KEY_1, KEY_2)).isEqualTo(VALUE_1);
	}

	@Test // DATAREDIS-315
	public void hGetShouldRetrieveValueCorrectly() {

		nativeConnection.hset(KEY_1, KEY_2, VALUE_1);

		assertThat(clusterConnection.hGet(KEY_1_BYTES, KEY_2_BYTES)).isEqualTo(VALUE_1_BYTES);
	}

	@Test // DATAREDIS-315
	public void hMGetShouldRetrieveValueCorrectly() {

		nativeConnection.hset(KEY_1, KEY_2, VALUE_1);
		nativeConnection.hset(KEY_1, KEY_3, VALUE_2);

		assertThat(clusterConnection.hMGet(KEY_1_BYTES, KEY_2_BYTES, KEY_3_BYTES)).contains(VALUE_1_BYTES, VALUE_2_BYTES);
	}

	@Test // DATAREDIS-315
	public void hMSetShouldAddValuesCorrectly() {

		Map<byte[], byte[]> hashes = new HashMap<byte[], byte[]>();
		hashes.put(KEY_2_BYTES, VALUE_1_BYTES);
		hashes.put(KEY_3_BYTES, VALUE_2_BYTES);

		clusterConnection.hMSet(KEY_1_BYTES, hashes);

		assertThat(nativeConnection.hmget(KEY_1, KEY_2, KEY_3)).contains(VALUE_1, VALUE_2);
	}

	@Test // DATAREDIS-315
	public void hIncrByShouldIncreaseFieldCorretly() {

		nativeConnection.hset(KEY_1, KEY_2, "1");
		nativeConnection.hset(KEY_1, KEY_3, "2");

		clusterConnection.hIncrBy(KEY_1_BYTES, KEY_3_BYTES, 3);

		assertThat(nativeConnection.hget(KEY_1, KEY_3)).isEqualTo("5");
	}

	@Test // DATAREDIS-315
	public void hIncrByFloatShouldIncreaseFieldCorretly() {

		nativeConnection.hset(KEY_1, KEY_2, "1");
		nativeConnection.hset(KEY_1, KEY_3, "2");

		clusterConnection.hIncrBy(KEY_1_BYTES, KEY_3_BYTES, 3.5D);

		assertThat(nativeConnection.hget(KEY_1, KEY_3)).isEqualTo("5.5");
	}

	@Test // DATAREDIS-315
	public void hExistsShouldReturnPresenceOfFieldCorrectly() {

		nativeConnection.hset(KEY_1, KEY_2, VALUE_1);

		assertThat(clusterConnection.hExists(KEY_1_BYTES, KEY_2_BYTES)).isTrue();
		assertThat(clusterConnection.hExists(KEY_1_BYTES, KEY_3_BYTES)).isFalse();
		assertThat(clusterConnection.hExists(toBytes("foo"), KEY_2_BYTES)).isFalse();
	}

	@Test // DATAREDIS-315
	public void hDelShouldRemoveFieldsCorrectly() {

		nativeConnection.hset(KEY_1, KEY_2, VALUE_1);
		nativeConnection.hset(KEY_1, KEY_3, VALUE_2);

		clusterConnection.hDel(KEY_1_BYTES, KEY_2_BYTES);

		assertThat(nativeConnection.hexists(KEY_1, KEY_2)).isFalse();
		assertThat(nativeConnection.hexists(KEY_1, KEY_3)).isTrue();
	}

	@Test // DATAREDIS-315
	public void hLenShouldRetrieveSizeCorrectly() {

		nativeConnection.hset(KEY_1, KEY_2, VALUE_1);
		nativeConnection.hset(KEY_1, KEY_3, VALUE_2);

		assertThat(clusterConnection.hLen(KEY_1_BYTES)).isEqualTo(2L);
	}

	@Test // DATAREDIS-315
	public void hKeysShouldRetrieveKeysCorrectly() {

		nativeConnection.hset(KEY_1, KEY_2, VALUE_1);
		nativeConnection.hset(KEY_1, KEY_3, VALUE_2);

		assertThat(clusterConnection.hKeys(KEY_1_BYTES)).contains(KEY_2_BYTES, KEY_3_BYTES);
	}

	@Test // DATAREDIS-315
	public void hValsShouldRetrieveValuesCorrectly() {

		nativeConnection.hset(KEY_1, KEY_2, VALUE_1);
		nativeConnection.hset(KEY_1, KEY_3, VALUE_2);

		assertThat(clusterConnection.hVals(KEY_1_BYTES)).contains(VALUE_1_BYTES, VALUE_2_BYTES);
	}

	@Test // DATAREDIS-315
	public void hGetAllShouldRetrieveEntriesCorrectly() {

		Map<String, String> hashes = new HashMap<String, String>();
		hashes.put(KEY_2, VALUE_1);
		hashes.put(KEY_3, VALUE_2);

		nativeConnection.hmset(KEY_1, hashes);

		Map<byte[], byte[]> hGetAll = clusterConnection.hGetAll(KEY_1_BYTES);

		assertThat(hGetAll.keySet()).contains(KEY_2_BYTES, KEY_3_BYTES);
	}

	@Test
	public void hScanShouldReadEntireValueRange() {

		int nrOfValues = 321;
		for (int i = 0; i < nrOfValues; i++) {
			nativeConnection.hset(KEY_1, "key" + i, "value-" + i);
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

	@Test(expected = DataAccessException.class) // DATAREDIS-315
	public void multiShouldThrowException() {
		clusterConnection.multi();
	}

	@Test(expected = DataAccessException.class) // DATAREDIS-315
	public void execShouldThrowException() {
		clusterConnection.exec();
	}

	@Test(expected = DataAccessException.class) // DATAREDIS-315
	public void discardShouldThrowException() {
		clusterConnection.discard();
	}

	@Test(expected = DataAccessException.class) // DATAREDIS-315
	public void watchShouldThrowException() {
		clusterConnection.watch();
	}

	@Test(expected = DataAccessException.class) // DATAREDIS-315
	public void unwatchShouldThrowException() {
		clusterConnection.unwatch();
	}

	@Test // DATAREDIS-315
	public void selectShouldAllowSelectionOfDBIndexZero() {
		clusterConnection.select(0);
	}

	@Test(expected = DataAccessException.class) // DATAREDIS-315
	public void selectShouldThrowExceptionWhenSelectingNonZeroDbIndex() {
		clusterConnection.select(1);
	}

	@Test // DATAREDIS-315
	public void echoShouldReturnInputCorrectly() {
		assertThat(clusterConnection.echo(VALUE_1_BYTES)).isEqualTo(VALUE_1_BYTES);
	}

	@Test // DATAREDIS-315
	public void pingShouldRetrunPongForExistingNode() {
		assertThat(clusterConnection.ping(new RedisClusterNode("127.0.0.1", 7379, null))).isEqualTo("PONG");
	}

	@Test // DATAREDIS-315
	public void pingShouldRetrunPong() {
		assertThat(clusterConnection.ping()).isEqualTo("PONG");
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-315
	public void pingShouldThrowExceptionWhenNodeNotKnownToCluster() {
		clusterConnection.ping(new RedisClusterNode("127.0.0.1", 1234, null));
	}

	@Test // DATAREDIS-315
	public void flushDbShouldFlushAllClusterNodes() {

		nativeConnection.set(KEY_1, VALUE_1);
		nativeConnection.set(KEY_2, VALUE_2);

		clusterConnection.flushDb();

		assertThat(nativeConnection.get(KEY_1)).isNull();
		assertThat(nativeConnection.get(KEY_2)).isNull();
	}

	@Test // DATAREDIS-315
	public void flushDbOnSingleNodeShouldFlushOnlyGivenNodesDb() {

		nativeConnection.set(KEY_1, VALUE_1);
		nativeConnection.set(KEY_2, VALUE_2);

		clusterConnection.flushDb(new RedisClusterNode("127.0.0.1", 7379, null));

		assertThat(nativeConnection.get(KEY_1)).isNotNull();
		assertThat(nativeConnection.get(KEY_2)).isNull();
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

		Set<byte[]> values = clusterConnection.zRangeByLex(KEY_1_BYTES, Range.range().lte("c"));

		assertThat(values).contains(toBytes("a"), toBytes("b"), toBytes("c"));
		assertThat(values).doesNotContain(toBytes("d"), toBytes("e"),
				toBytes("f"), toBytes("g"));

		values = clusterConnection.zRangeByLex(KEY_1_BYTES, Range.range().lt("c"));
		assertThat(values).contains(toBytes("a"), toBytes("b"));
		assertThat(values).doesNotContain(toBytes("c"));

		values = clusterConnection.zRangeByLex(KEY_1_BYTES, Range.range().gte("aaa").lt("g"));
		assertThat(values).contains(toBytes("b"), toBytes("c"),
				toBytes("d"), toBytes("e"), toBytes("f"));
		assertThat(values).doesNotContain(toBytes("a"), toBytes("g"));

		values = clusterConnection.zRangeByLex(KEY_1_BYTES, Range.range().gte("e"));
		assertThat(values).contains(toBytes("e"), toBytes("f"), toBytes("g"));
		assertThat(values).doesNotContain(toBytes("a"), toBytes("b"),
				toBytes("c"), toBytes("d"));
	}

	@Test // DATAREDIS-315
	public void infoShouldCollectionInfoFromAllClusterNodes() {
		assertThat(clusterConnection.info().size()).isCloseTo(245, offset(35));
	}

	@Test // DATAREDIS-315
	public void clientListShouldGetInfosForAllClients() {
		assertThat(clusterConnection.getClientList().isEmpty()).isFalse();
	}

	@Test // DATAREDIS-315
	public void getClusterNodeForKeyShouldReturnNodeCorrectly() {
		assertThat((RedisNode) clusterConnection.clusterGetNodeForKey(KEY_1_BYTES)).isEqualTo(new RedisNode("127.0.0.1", 7380));
	}

	@Test // DATAREDIS-315
	public void countKeysShouldReturnNumberOfKeysInSlot() {

		nativeConnection.set(SAME_SLOT_KEY_1, VALUE_1);
		nativeConnection.set(SAME_SLOT_KEY_2, VALUE_2);

		assertThat(clusterConnection.clusterCountKeysInSlot(ClusterSlotHashUtil.calculateSlot(SAME_SLOT_KEY_1))).isEqualTo(2L);
	}

	@Test // DATAREDIS-315
	public void pfAddShouldAddValuesCorrectly() {

		clusterConnection.pfAdd(KEY_1_BYTES, VALUE_1_BYTES, VALUE_2_BYTES, VALUE_3_BYTES);

		assertThat(nativeConnection.pfcount(KEY_1)).isEqualTo(3L);
	}

	@Test // DATAREDIS-315
	public void pfCountShouldAllowCountingOnSingleKey() {

		nativeConnection.pfadd(KEY_1, VALUE_1, VALUE_2, VALUE_3);

		assertThat(clusterConnection.pfCount(KEY_1_BYTES)).isEqualTo(3L);
	}

	@Test // DATAREDIS-315
	public void pfCountShouldAllowCountingOnSameSlotKeys() {

		nativeConnection.pfadd(SAME_SLOT_KEY_1, VALUE_1, VALUE_2);
		nativeConnection.pfadd(SAME_SLOT_KEY_2, VALUE_2, VALUE_3);

		assertThat(clusterConnection.pfCount(SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES)).isEqualTo(3L);
	}

	@Test(expected = DataAccessException.class) // DATAREDIS-315
	public void pfCountShouldThrowErrorCountingOnDifferentSlotKeys() {

		nativeConnection.pfadd(KEY_1, VALUE_1, VALUE_2);
		nativeConnection.pfadd(KEY_2, VALUE_2, VALUE_3);

		clusterConnection.pfCount(KEY_1_BYTES, KEY_2_BYTES);
	}

	@Test // DATAREDIS-315
	public void pfMergeShouldWorkWhenAllKeysMapToSameSlot() {

		nativeConnection.pfadd(SAME_SLOT_KEY_1, VALUE_1, VALUE_2);
		nativeConnection.pfadd(SAME_SLOT_KEY_2, VALUE_2, VALUE_3);

		nativeConnection.pfmerge(SAME_SLOT_KEY_3, SAME_SLOT_KEY_1, SAME_SLOT_KEY_2);

		assertThat(nativeConnection.pfcount(SAME_SLOT_KEY_3)).isEqualTo(3L);
	}

	@Test(expected = DataAccessException.class) // DATAREDIS-315
	public void pfMergeShouldThrowErrorOnDifferentSlotKeys() {
		clusterConnection.pfMerge(KEY_3_BYTES, KEY_1_BYTES, KEY_2_BYTES);
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

	@Test // DATAREDIS-315
	public void getConfigShouldLoadCumulatedConfiguration() {

		List<String> result = clusterConnection.getConfig("*max-*-entries*");

		// config get *max-*-entries on redis 3.0.7 returns 8 entries per node while on 3.2.0-rc3 returns 6.
		// @link https://github.com/spring-projects/spring-data-redis/pull/187
		assertThat(result.size() % 6).isEqualTo(0);
		for (int i = 0; i < result.size(); i++) {

			if (i % 2 == 0) {
				assertThat(result.get(i)).startsWith(CLUSTER_HOST);
			} else {
				assertThat(result.get(i)).doesNotStartWith(CLUSTER_HOST);
			}
		}
	}

	@Test // DATAREDIS-315
	public void getConfigShouldLoadConfigurationOfSpecificNode() {

		List<String> result = clusterConnection.getConfig(new RedisClusterNode(CLUSTER_HOST, SLAVEOF_NODE_1_PORT), "*");

		ListIterator<String> it = result.listIterator();
		Integer valueIndex = null;
		while (it.hasNext()) {

			String cur = it.next();
			if (cur.equals("slaveof")) {
				valueIndex = it.nextIndex();
				break;
			}
		}

		assertThat(valueIndex).isNotNull();
		assertThat(result.get(valueIndex)).endsWith("7379");
	}

	@Test // DATAREDIS-315
	public void clusterGetSlavesShouldReturnSlaveCorrectly() {

		Set<RedisClusterNode> slaves = clusterConnection
				.clusterGetSlaves(new RedisClusterNode(CLUSTER_HOST, MASTER_NODE_1_PORT));

		assertThat(slaves.size()).isEqualTo(1);
		assertThat(slaves).contains(new RedisClusterNode(CLUSTER_HOST, SLAVEOF_NODE_1_PORT));
	}

	@Test // DATAREDIS-315
	public void clusterGetMasterSlaveMapShouldListMastersAndSlavesCorrectly() {

		Map<RedisClusterNode, Collection<RedisClusterNode>> masterSlaveMap = clusterConnection.clusterGetMasterSlaveMap();

		assertThat(masterSlaveMap).isNotNull();
		assertThat(masterSlaveMap.size()).isEqualTo(3);
		assertThat(masterSlaveMap.get(new RedisClusterNode(CLUSTER_HOST, MASTER_NODE_1_PORT))).contains(new RedisClusterNode(CLUSTER_HOST, SLAVEOF_NODE_1_PORT));
		assertThat(masterSlaveMap.get(new RedisClusterNode(CLUSTER_HOST, MASTER_NODE_2_PORT)).isEmpty()).isTrue();
		assertThat(masterSlaveMap.get(new RedisClusterNode(CLUSTER_HOST, MASTER_NODE_3_PORT)).isEmpty()).isTrue();
	}

	@Test // DATAREDIS-316
	public void setWithExpirationInSecondsShouldWorkCorrectly() {

		clusterConnection.set(KEY_1_BYTES, VALUE_1_BYTES, Expiration.seconds(1), SetOption.upsert());

		assertThat(nativeConnection.exists(KEY_1)).isTrue();
		assertThat(nativeConnection.ttl(KEY_1)).isEqualTo(1L);
	}

	@Test // DATAREDIS-316
	public void setWithExpirationInMillisecondsShouldWorkCorrectly() {

		clusterConnection.set(KEY_1_BYTES, VALUE_1_BYTES, Expiration.milliseconds(500), SetOption.upsert());

		assertThat(nativeConnection.exists(KEY_1)).isTrue();
		assertThat(nativeConnection.pttl(KEY_1)).isCloseTo(500, offset(500L));
	}

	@Test // DATAREDIS-316
	public void setWithOptionIfPresentShouldWorkCorrectly() {

		nativeConnection.set(KEY_1, VALUE_1);
		clusterConnection.set(KEY_1_BYTES, VALUE_2_BYTES, Expiration.persistent(), SetOption.ifPresent());
	}

	@Test // DATAREDIS-316
	public void setWithOptionIfAbsentShouldWorkCorrectly() {

		clusterConnection.set(KEY_1_BYTES, VALUE_1_BYTES, Expiration.persistent(), SetOption.ifAbsent());

		assertThat(nativeConnection.exists(KEY_1)).isTrue();
		assertThat(nativeConnection.ttl(KEY_1)).isEqualTo(-1L);
	}

	@Test // DATAREDIS-316
	public void setWithExpirationAndIfAbsentShouldWorkCorrectly() {

		clusterConnection.set(KEY_1_BYTES, VALUE_1_BYTES, Expiration.seconds(1), SetOption.ifAbsent());

		assertThat(nativeConnection.exists(KEY_1)).isTrue();
		assertThat(nativeConnection.ttl(KEY_1)).isEqualTo(1L);
	}

	@Test // DATAREDIS-316
	public void setWithExpirationAndIfAbsentShouldNotBeAppliedWhenKeyExists() {

		nativeConnection.set(KEY_1, VALUE_1);

		clusterConnection.set(KEY_1_BYTES, VALUE_2_BYTES, Expiration.seconds(1), SetOption.ifAbsent());

		assertThat(nativeConnection.exists(KEY_1)).isTrue();
		assertThat(nativeConnection.ttl(KEY_1)).isEqualTo(-1L);
		assertThat(nativeConnection.get(KEY_1)).isEqualTo(VALUE_1);
	}

	@Test // DATAREDIS-316
	public void setWithExpirationAndIfPresentShouldWorkCorrectly() {

		nativeConnection.set(KEY_1, VALUE_1);

		clusterConnection.set(KEY_1_BYTES, VALUE_2_BYTES, Expiration.seconds(1), SetOption.ifPresent());

		assertThat(nativeConnection.exists(KEY_1)).isTrue();
		assertThat(nativeConnection.ttl(KEY_1)).isEqualTo(1L);
		assertThat(nativeConnection.get(KEY_1)).isEqualTo(VALUE_2);
	}

	@Test // DATAREDIS-316
	public void setWithExpirationAndIfPresentShouldNotBeAppliedWhenKeyDoesNotExists() {

		clusterConnection.set(KEY_1_BYTES, VALUE_1_BYTES, Expiration.seconds(1), SetOption.ifPresent());

		assertThat(nativeConnection.exists(KEY_1)).isFalse();
	}
}
