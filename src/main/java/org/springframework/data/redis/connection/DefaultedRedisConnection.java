/*
 * Copyright 2017-2021 the original author or authors.
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
package org.springframework.data.redis.connection;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Metric;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.stream.ByteRecord;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.PendingMessagesSummary;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamInfo.XInfoConsumers;
import org.springframework.data.redis.connection.stream.StreamInfo.XInfoGroups;
import org.springframework.data.redis.connection.stream.StreamInfo.XInfoStream;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.core.types.RedisClientInfo;
import org.springframework.lang.Nullable;

/**
 * {@link DefaultedRedisConnection} provides method delegates to {@code Redis*Command} interfaces accessible via
 * {@link RedisConnection}. This allows us to maintain backwards compatibility while moving the actual implementation
 * and stay in sync with {@link ReactiveRedisConnection}. Going forward the {@link RedisCommands} extension is likely to
 * be removed from {@link RedisConnection}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Tugdual Grall
 * @author Andrey Shlykov
 * @since 2.0
 */
public interface DefaultedRedisConnection extends RedisConnection {

	// KEY COMMANDS

	/** @deprecated in favor of {@link RedisConnection#keyCommands()}. */
	@Override
	@Deprecated
	default Boolean exists(byte[] key) {
		return keyCommands().exists(key);
	}

	/** @deprecated in favor of {@link RedisConnection#keyCommands()}. */
	@Override
	@Deprecated
	default Long exists(byte[]... keys) {
		return keyCommands().exists(keys);
	}

	/** @deprecated in favor of {@link RedisConnection#keyCommands()}. */
	@Override
	@Deprecated
	default Long del(byte[]... keys) {
		return keyCommands().del(keys);
	}

	/** @deprecated in favor of {@link RedisConnection#keyCommands()}. */
	@Override
	@Deprecated
	default Long unlink(byte[]... keys) {
		return keyCommands().unlink(keys);
	}

	/** @deprecated in favor of {@link RedisConnection#keyCommands()}. */
	@Override
	@Deprecated
	default DataType type(byte[] pattern) {
		return keyCommands().type(pattern);
	}

	/** @deprecated in favor of {@link RedisConnection#keyCommands()}. */
	@Override
	@Deprecated
	default Long touch(byte[]... keys) {
		return keyCommands().touch(keys);
	}

	/** @deprecated in favor of {@link RedisConnection#keyCommands()}. */
	@Override
	@Deprecated
	default Set<byte[]> keys(byte[] pattern) {
		return keyCommands().keys(pattern);
	}

	/** @deprecated in favor of {@link RedisConnection#keyCommands()}. */
	@Override
	@Deprecated
	default Cursor<byte[]> scan(ScanOptions options) {
		return keyCommands().scan(options);
	}

	/** @deprecated in favor of {@link RedisConnection#keyCommands()}. */
	@Override
	@Deprecated
	default byte[] randomKey() {
		return keyCommands().randomKey();
	}

	/** @deprecated in favor of {@link RedisConnection#keyCommands()}. */
	@Override
	@Deprecated
	default void rename(byte[] sourceKey, byte[] targetKey) {
		keyCommands().rename(sourceKey, targetKey);
	}

	/** @deprecated in favor of {@link RedisConnection#keyCommands()}. */
	@Override
	@Deprecated
	default Boolean renameNX(byte[] sourceKey, byte[] targetKey) {
		return keyCommands().renameNX(sourceKey, targetKey);
	}

	/** @deprecated in favor of {@link RedisConnection#keyCommands()}. */
	@Override
	@Deprecated
	default Boolean expire(byte[] key, long seconds) {
		return keyCommands().expire(key, seconds);
	}

	/** @deprecated in favor of {@link RedisConnection#keyCommands()}. */
	@Override
	@Deprecated
	default Boolean persist(byte[] key) {
		return keyCommands().persist(key);
	}

	/** @deprecated in favor of {@link RedisConnection#keyCommands()}. */
	@Override
	@Deprecated
	default Boolean move(byte[] key, int dbIndex) {
		return keyCommands().move(key, dbIndex);
	}

	/** @deprecated in favor of {@link RedisConnection#keyCommands()}. */
	@Override
	@Deprecated
	default void restore(byte[] key, long ttlInMillis, byte[] serializedValue, boolean replace) {
		keyCommands().restore(key, ttlInMillis, serializedValue, replace);
	}

	/** @deprecated in favor of {@link RedisConnection#keyCommands()}. */
	@Override
	@Deprecated
	default Long pTtl(byte[] key) {
		return keyCommands().pTtl(key);
	}

	/** @deprecated in favor of {@link RedisConnection#keyCommands()}. */
	@Override
	@Deprecated
	default Long pTtl(byte[] key, TimeUnit timeUnit) {
		return keyCommands().pTtl(key, timeUnit);
	}

	/** @deprecated in favor of {@link RedisConnection#keyCommands()}. */
	@Override
	@Deprecated
	default Boolean pExpire(byte[] key, long millis) {
		return keyCommands().pExpire(key, millis);
	}

	/** @deprecated in favor of {@link RedisConnection#keyCommands()}. */
	@Override
	@Deprecated
	default Boolean pExpireAt(byte[] key, long unixTimeInMillis) {
		return keyCommands().pExpireAt(key, unixTimeInMillis);
	}

	/** @deprecated in favor of {@link RedisConnection#keyCommands()}. */
	@Override
	@Deprecated
	default Boolean expireAt(byte[] key, long unixTime) {
		return keyCommands().expireAt(key, unixTime);
	}

	/** @deprecated in favor of {@link RedisConnection#keyCommands()}. */
	@Override
	@Deprecated
	default Long ttl(byte[] key) {
		return keyCommands().ttl(key);
	}

	/** @deprecated in favor of {@link RedisConnection#keyCommands()}. */
	@Override
	@Deprecated
	default Long ttl(byte[] key, TimeUnit timeUnit) {
		return keyCommands().ttl(key, timeUnit);
	}

	/** @deprecated in favor of {@link RedisConnection#keyCommands()}. */
	@Override
	@Deprecated
	default byte[] dump(byte[] key) {
		return keyCommands().dump(key);
	}

	/** @deprecated in favor of {@link RedisConnection#keyCommands()}. */
	@Override
	@Deprecated
	default List<byte[]> sort(byte[] key, SortParameters params) {
		return keyCommands().sort(key, params);
	}

	/** @deprecated in favor of {@link RedisConnection#keyCommands()}. */
	@Override
	@Deprecated
	default Long sort(byte[] key, SortParameters params, byte[] sortKey) {
		return keyCommands().sort(key, params, sortKey);
	}

	/** @deprecated in favor of {@link RedisConnection#keyCommands()}. */
	@Override
	@Deprecated
	default ValueEncoding encodingOf(byte[] key) {
		return keyCommands().encodingOf(key);
	}

	/** @deprecated in favor of {@link RedisConnection#keyCommands()}. */
	@Override
	@Deprecated
	default Duration idletime(byte[] key) {
		return keyCommands().idletime(key);
	}

	/** @deprecated in favor of {@link RedisConnection#keyCommands()}. */
	@Override
	@Deprecated
	default Long refcount(byte[] key) {
		return keyCommands().refcount(key);
	}

	// STRING COMMANDS

	/** @deprecated in favor of {@link RedisConnection#stringCommands()}}. */
	@Override
	@Deprecated
	default byte[] get(byte[] key) {
		return stringCommands().get(key);
	}

	/** @deprecated in favor of {@link RedisConnection#stringCommands()}}. */
	@Override
	@Deprecated
	default byte[] getSet(byte[] key, byte[] value) {
		return stringCommands().getSet(key, value);
	}

	/** @deprecated in favor of {@link RedisConnection#stringCommands()}}. */
	@Override
	@Deprecated
	default List<byte[]> mGet(byte[]... keys) {
		return stringCommands().mGet(keys);
	}

	/** @deprecated in favor of {@link RedisConnection#stringCommands()}}. */
	@Override
	@Deprecated
	default Boolean set(byte[] key, byte[] value) {
		return stringCommands().set(key, value);
	}

	/** @deprecated in favor of {@link RedisConnection#stringCommands()}}. */
	@Override
	@Deprecated
	default Boolean set(byte[] key, byte[] value, Expiration expiration, SetOption option) {
		return stringCommands().set(key, value, expiration, option);
	}

	/** @deprecated in favor of {@link RedisConnection#stringCommands()}}. */
	@Override
	@Deprecated
	default Boolean setNX(byte[] key, byte[] value) {
		return stringCommands().setNX(key, value);
	}

	/** @deprecated in favor of {@link RedisConnection#stringCommands()}}. */
	@Override
	@Deprecated
	default Boolean setEx(byte[] key, long seconds, byte[] value) {
		return stringCommands().setEx(key, seconds, value);
	}

	/** @deprecated in favor of {@link RedisConnection#stringCommands()}}. */
	@Override
	@Deprecated
	default Boolean pSetEx(byte[] key, long milliseconds, byte[] value) {
		return stringCommands().pSetEx(key, milliseconds, value);
	}

	/** @deprecated in favor of {@link RedisConnection#stringCommands()}}. */
	@Override
	@Deprecated
	default Boolean mSet(Map<byte[], byte[]> tuple) {
		return stringCommands().mSet(tuple);
	}

	/** @deprecated in favor of {@link RedisConnection#stringCommands()}}. */
	@Override
	@Deprecated
	default Boolean mSetNX(Map<byte[], byte[]> tuple) {
		return stringCommands().mSetNX(tuple);
	}

	/** @deprecated in favor of {@link RedisConnection#stringCommands()}}. */
	@Override
	@Deprecated
	default Long incr(byte[] key) {
		return stringCommands().incr(key);
	}

	/** @deprecated in favor of {@link RedisConnection#stringCommands()}}. */
	@Override
	@Deprecated
	default Double incrBy(byte[] key, double value) {
		return stringCommands().incrBy(key, value);
	}

	/** @deprecated in favor of {@link RedisConnection#stringCommands()}}. */
	@Override
	@Deprecated
	default Long incrBy(byte[] key, long value) {
		return stringCommands().incrBy(key, value);
	}

	/** @deprecated in favor of {@link RedisConnection#stringCommands()}}. */
	@Override
	@Deprecated
	default Long decr(byte[] key) {
		return stringCommands().decr(key);
	}

	/** @deprecated in favor of {@link RedisConnection#stringCommands()}}. */
	@Override
	@Deprecated
	default Long decrBy(byte[] key, long value) {
		return stringCommands().decrBy(key, value);
	}

	/** @deprecated in favor of {@link RedisConnection#stringCommands()}}. */
	@Override
	@Deprecated
	default Long append(byte[] key, byte[] value) {
		return stringCommands().append(key, value);
	}

	/** @deprecated in favor of {@link RedisConnection#stringCommands()}}. */
	@Override
	@Deprecated
	default byte[] getRange(byte[] key, long start, long end) {
		return stringCommands().getRange(key, start, end);
	}

	/** @deprecated in favor of {@link RedisConnection#stringCommands()}}. */
	@Override
	@Deprecated
	default void setRange(byte[] key, byte[] value, long offset) {
		stringCommands().setRange(key, value, offset);
	}

	/** @deprecated in favor of {@link RedisConnection#stringCommands()}}. */
	@Override
	@Deprecated
	default Boolean getBit(byte[] key, long offset) {
		return stringCommands().getBit(key, offset);
	}

	/** @deprecated in favor of {@link RedisConnection#stringCommands()}}. */
	@Override
	@Deprecated
	default Boolean setBit(byte[] key, long offset, boolean value) {
		return stringCommands().setBit(key, offset, value);
	}

	/** @deprecated in favor of {@link RedisConnection#stringCommands()}}. */
	@Override
	@Deprecated
	default Long bitCount(byte[] key) {
		return stringCommands().bitCount(key);
	}

	/** @deprecated in favor of {@link RedisConnection#stringCommands()}}. */
	@Override
	@Deprecated
	default Long bitCount(byte[] key, long start, long end) {
		return stringCommands().bitCount(key, start, end);
	}

	/** @deprecated in favor of {@link RedisConnection#stringCommands()}}. */
	@Override
	@Deprecated
	default List<Long> bitField(byte[] key, BitFieldSubCommands subCommands) {
		return stringCommands().bitField(key, subCommands);
	}

	/** @deprecated in favor of {@link RedisConnection#stringCommands()}}. */
	@Override
	@Deprecated
	default Long bitOp(BitOperation op, byte[] destination, byte[]... keys) {
		return stringCommands().bitOp(op, destination, keys);
	}

	/** @deprecated in favor of {@link RedisConnection#stringCommands()}}. */
	@Override
	@Deprecated
	default Long bitPos(byte[] key, boolean bit, org.springframework.data.domain.Range<Long> range) {
		return stringCommands().bitPos(key, bit, range);
	}

	/** @deprecated in favor of {@link RedisConnection#stringCommands()}}. */
	@Override
	@Deprecated
	default Long strLen(byte[] key) {
		return stringCommands().strLen(key);
	}

	// STREAM COMMANDS

	/** @deprecated in favor of {@link RedisConnection#streamCommands()}}. */
	@Override
	@Deprecated
	default Long xAck(byte[] key, String group, RecordId... messageIds) {
		return streamCommands().xAck(key, group, messageIds);
	}

	/** @deprecated in favor of {@link RedisConnection#streamCommands()}}. */
	@Override
	@Deprecated
	default RecordId xAdd(MapRecord<byte[], byte[], byte[]> record, XAddOptions options) {
		return streamCommands().xAdd(record, options);
	}

	/** @deprecated in favor of {@link RedisConnection#streamCommands()}}. */
	@Override
	@Deprecated
	default List<RecordId> xClaimJustId(byte[] key, String group, String newOwner, XClaimOptions options) {
		return streamCommands().xClaimJustId(key, group, newOwner, options);
	}

	/** @deprecated in favor of {@link RedisConnection#streamCommands()}}. */
	@Override
	@Deprecated
	default List<ByteRecord> xClaim(byte[] key, String group, String newOwner, XClaimOptions options) {
		return streamCommands().xClaim(key, group, newOwner, options);
	}

	/** @deprecated in favor of {@link RedisConnection#streamCommands()}}. */
	@Override
	@Deprecated
	default Long xDel(byte[] key, RecordId... recordIds) {
		return streamCommands().xDel(key, recordIds);
	}

	/** @deprecated in favor of {@link RedisConnection#streamCommands()}}. */
	@Override
	@Deprecated
	default String xGroupCreate(byte[] key, String groupName, ReadOffset readOffset) {
		return streamCommands().xGroupCreate(key, groupName, readOffset);
	}

	/** @deprecated in favor of {@link RedisConnection#streamCommands()}}. */
	@Override
	@Deprecated
	default String xGroupCreate(byte[] key, String groupName, ReadOffset readOffset, boolean mkStream) {
		return streamCommands().xGroupCreate(key, groupName, readOffset, mkStream);
	}

	/** @deprecated in favor of {@link RedisConnection#streamCommands()}}. */
	@Override
	@Deprecated
	default Boolean xGroupDelConsumer(byte[] key, Consumer consumer) {
		return streamCommands().xGroupDelConsumer(key, consumer);
	}

	/** @deprecated in favor of {@link RedisConnection#streamCommands()}}. */
	@Override
	@Deprecated
	default Boolean xGroupDestroy(byte[] key, String groupName) {
		return streamCommands().xGroupDestroy(key, groupName);
	}

	/** @deprecated in favor of {@link RedisConnection#streamCommands()}}. */
	@Override
	@Deprecated
	default XInfoStream xInfo(byte[] key) {
		return streamCommands().xInfo(key);
	}

	/** @deprecated in favor of {@link RedisConnection#streamCommands()}}. */
	@Override
	@Deprecated
	default XInfoGroups xInfoGroups(byte[] key) {
		return streamCommands().xInfoGroups(key);
	}

	/** @deprecated in favor of {@link RedisConnection#streamCommands()}}. */
	@Override
	@Deprecated
	default XInfoConsumers xInfoConsumers(byte[] key, String groupName) {
		return streamCommands().xInfoConsumers(key, groupName);
	}

	/** @deprecated in favor of {@link RedisConnection#streamCommands()}}. */
	@Override
	@Deprecated
	default Long xLen(byte[] key) {
		return streamCommands().xLen(key);
	}

	/** @deprecated in favor of {@link RedisConnection#streamCommands()}}. */
	@Override
	@Deprecated
	default PendingMessagesSummary xPending(byte[] key, String groupName) {
		return streamCommands().xPending(key, groupName);
	}

	/** @deprecated in favor of {@link RedisConnection#streamCommands()}}. */
	@Override
	@Deprecated
	default PendingMessages xPending(byte[] key, String groupName, XPendingOptions options) {
		return streamCommands().xPending(key, groupName, options);
	}

	/** @deprecated in favor of {@link RedisConnection#streamCommands()}}. */
	@Override
	@Deprecated
	default List<ByteRecord> xRange(byte[] key, org.springframework.data.domain.Range<String> range) {
		return streamCommands().xRange(key, range);
	}

	/** @deprecated in favor of {@link RedisConnection#streamCommands()}}. */
	@Override
	@Deprecated
	default List<ByteRecord> xRange(byte[] key, org.springframework.data.domain.Range<String> range, Limit limit) {
		return streamCommands().xRange(key, range, limit);
	}

	/** @deprecated in favor of {@link RedisConnection#streamCommands()}}. */
	@Override
	@Deprecated
	default List<ByteRecord> xRead(StreamOffset<byte[]>... streams) {
		return streamCommands().xRead(streams);
	}

	/** @deprecated in favor of {@link RedisConnection#streamCommands()}}. */
	@Override
	@Deprecated
	default List<ByteRecord> xRead(StreamReadOptions readOptions, StreamOffset<byte[]>... streams) {
		return streamCommands().xRead(readOptions, streams);
	}

	/** @deprecated in favor of {@link RedisConnection#streamCommands()}}. */
	@Override
	@Deprecated
	default List<ByteRecord> xReadGroup(Consumer consumer, StreamOffset<byte[]>... streams) {
		return streamCommands().xReadGroup(consumer, streams);
	}

	/** @deprecated in favor of {@link RedisConnection#streamCommands()}}. */
	@Override
	@Deprecated
	default List<ByteRecord> xReadGroup(Consumer consumer, StreamReadOptions readOptions,
			StreamOffset<byte[]>... streams) {
		return streamCommands().xReadGroup(consumer, readOptions, streams);
	}

	/** @deprecated in favor of {@link RedisConnection#streamCommands()}}. */
	@Override
	@Deprecated
	default List<ByteRecord> xRevRange(byte[] key, org.springframework.data.domain.Range<String> range) {
		return streamCommands().xRevRange(key, range);
	}

	/** @deprecated in favor of {@link RedisConnection#streamCommands()}}. */
	@Override
	@Deprecated
	default List<ByteRecord> xRevRange(byte[] key, org.springframework.data.domain.Range<String> range, Limit limit) {
		return streamCommands().xRevRange(key, range, limit);
	}

	/** @deprecated in favor of {@link RedisConnection#streamCommands()}}. */
	@Override
	@Deprecated
	default Long xTrim(byte[] key, long count) {
		return xTrim(key, count, false);
	}

	@Override
	@Deprecated
	default Long xTrim(byte[] key, long count, boolean approximateTrimming) {
		return streamCommands().xTrim(key, count, approximateTrimming);
	}

	// LIST COMMANDS

	/** @deprecated in favor of {@link RedisConnection#listCommands()}}. */
	@Override
	@Deprecated
	default Long rPush(byte[] key, byte[]... values) {
		return listCommands().rPush(key, values);
	}

	/** @deprecated in favor of {@link RedisConnection#listCommands()}}. */
	@Override
	@Deprecated
	default List<Long> lPos(byte[] key, byte[] element, @Nullable Integer rank, @Nullable Integer count) {
		return listCommands().lPos(key, element, rank, count);
	}

	/** @deprecated in favor of {@link RedisConnection#listCommands()}}. */
	@Override
	@Deprecated
	default Long lPush(byte[] key, byte[]... values) {
		return listCommands().lPush(key, values);
	}

	/** @deprecated in favor of {@link RedisConnection#listCommands()}}. */
	@Override
	@Deprecated
	default Long rPushX(byte[] key, byte[] value) {
		return listCommands().rPushX(key, value);
	}

	/** @deprecated in favor of {@link RedisConnection#listCommands()}}. */
	@Override
	@Deprecated
	default Long lPushX(byte[] key, byte[] value) {
		return listCommands().lPushX(key, value);
	}

	/** @deprecated in favor of {@link RedisConnection#listCommands()}}. */
	@Override
	@Deprecated
	default Long lLen(byte[] key) {
		return listCommands().lLen(key);
	}

	/** @deprecated in favor of {@link RedisConnection#listCommands()}}. */
	@Override
	@Deprecated
	default List<byte[]> lRange(byte[] key, long start, long end) {
		return listCommands().lRange(key, start, end);
	}

	/** @deprecated in favor of {@link RedisConnection#listCommands()}}. */
	@Override
	@Deprecated
	default void lTrim(byte[] key, long start, long end) {
		listCommands().lTrim(key, start, end);
	}

	/** @deprecated in favor of {@link RedisConnection#listCommands()}}. */
	@Override
	@Deprecated
	default byte[] lIndex(byte[] key, long index) {
		return listCommands().lIndex(key, index);
	}

	/** @deprecated in favor of {@link RedisConnection#listCommands()}}. */
	@Override
	@Deprecated
	default Long lInsert(byte[] key, Position where, byte[] pivot, byte[] value) {
		return listCommands().lInsert(key, where, pivot, value);
	}

	/** @deprecated in favor of {@link RedisConnection#listCommands()}}. */
	@Override
	@Deprecated
	default void lSet(byte[] key, long index, byte[] value) {
		listCommands().lSet(key, index, value);
	}

	/** @deprecated in favor of {@link RedisConnection#listCommands()}}. */
	@Override
	@Deprecated
	default Long lRem(byte[] key, long count, byte[] value) {
		return listCommands().lRem(key, count, value);
	}

	/** @deprecated in favor of {@link RedisConnection#listCommands()}}. */
	@Override
	@Deprecated
	default byte[] lPop(byte[] key) {
		return listCommands().lPop(key);
	}

	/** @deprecated in favor of {@link RedisConnection#listCommands()}}. */
	@Override
	@Deprecated
	default byte[] rPop(byte[] key) {
		return listCommands().rPop(key);
	}

	/** @deprecated in favor of {@link RedisConnection#listCommands()}}. */
	@Override
	@Deprecated
	default List<byte[]> bLPop(int timeout, byte[]... keys) {
		return listCommands().bLPop(timeout, keys);
	}

	/** @deprecated in favor of {@link RedisConnection#listCommands()}}. */
	@Override
	@Deprecated
	default List<byte[]> bRPop(int timeout, byte[]... keys) {
		return listCommands().bRPop(timeout, keys);
	}

	/** @deprecated in favor of {@link RedisConnection#listCommands()}}. */
	@Override
	@Deprecated
	default byte[] rPopLPush(byte[] srcKey, byte[] dstKey) {
		return listCommands().rPopLPush(srcKey, dstKey);
	}

	/** @deprecated in favor of {@link RedisConnection#listCommands()}}. */
	@Override
	@Deprecated
	default byte[] bRPopLPush(int timeout, byte[] srcKey, byte[] dstKey) {
		return listCommands().bRPopLPush(timeout, srcKey, dstKey);
	}

	// SET COMMANDS

	/** @deprecated in favor of {@link RedisConnection#setCommands()}}. */
	@Override
	@Deprecated
	default Long sAdd(byte[] key, byte[]... values) {
		return setCommands().sAdd(key, values);
	}

	/** @deprecated in favor of {@link RedisConnection#setCommands()}}. */
	@Override
	@Deprecated
	default Long sCard(byte[] key) {
		return setCommands().sCard(key);
	}

	/** @deprecated in favor of {@link RedisConnection#setCommands()}}. */
	@Override
	@Deprecated
	default Set<byte[]> sDiff(byte[]... keys) {
		return setCommands().sDiff(keys);
	}

	/** @deprecated in favor of {@link RedisConnection#setCommands()}}. */
	@Override
	@Deprecated
	default Long sDiffStore(byte[] destKey, byte[]... keys) {
		return setCommands().sDiffStore(destKey, keys);
	}

	/** @deprecated in favor of {@link RedisConnection#setCommands()}}. */
	@Override
	@Deprecated
	default Set<byte[]> sInter(byte[]... keys) {
		return setCommands().sInter(keys);
	}

	/** @deprecated in favor of {@link RedisConnection#setCommands()}}. */
	@Override
	@Deprecated
	default Long sInterStore(byte[] destKey, byte[]... keys) {
		return setCommands().sInterStore(destKey, keys);
	}

	/** @deprecated in favor of {@link RedisConnection#setCommands()}}. */
	@Override
	@Deprecated
	default Boolean sIsMember(byte[] key, byte[] value) {
		return setCommands().sIsMember(key, value);
	}

	/** @deprecated in favor of {@link RedisConnection#setCommands()}}. */
	@Override
	@Deprecated
	default Set<byte[]> sMembers(byte[] key) {
		return setCommands().sMembers(key);
	}

	/** @deprecated in favor of {@link RedisConnection#setCommands()}}. */
	@Override
	@Deprecated
	default Boolean sMove(byte[] srcKey, byte[] destKey, byte[] value) {
		return setCommands().sMove(srcKey, destKey, value);
	}

	/** @deprecated in favor of {@link RedisConnection#setCommands()}}. */
	@Override
	@Deprecated
	default byte[] sPop(byte[] key) {
		return setCommands().sPop(key);
	}

	/** @deprecated in favor of {@link RedisConnection#setCommands()}}. */
	@Override
	@Deprecated
	default List<byte[]> sPop(byte[] key, long count) {
		return setCommands().sPop(key, count);
	}

	/** @deprecated in favor of {@link RedisConnection#setCommands()}}. */
	@Override
	@Deprecated
	default byte[] sRandMember(byte[] key) {
		return setCommands().sRandMember(key);
	}

	/** @deprecated in favor of {@link RedisConnection#setCommands()}}. */
	@Override
	@Deprecated
	default List<byte[]> sRandMember(byte[] key, long count) {
		return setCommands().sRandMember(key, count);
	}

	/** @deprecated in favor of {@link RedisConnection#setCommands()}}. */
	@Override
	@Deprecated
	default Long sRem(byte[] key, byte[]... values) {
		return setCommands().sRem(key, values);
	}

	/** @deprecated in favor of {@link RedisConnection#setCommands()}}. */
	@Override
	@Deprecated
	default Set<byte[]> sUnion(byte[]... keys) {
		return setCommands().sUnion(keys);
	}

	/** @deprecated in favor of {@link RedisConnection#setCommands()}}. */
	@Override
	@Deprecated
	default Long sUnionStore(byte[] destKey, byte[]... keys) {
		return setCommands().sUnionStore(destKey, keys);
	}

	/** @deprecated in favor of {@link RedisConnection#setCommands()}}. */
	@Override
	@Deprecated
	default Cursor<byte[]> sScan(byte[] key, ScanOptions options) {
		return setCommands().sScan(key, options);
	}

	// ZSET COMMANDS

	/** @deprecated in favor of {@link RedisConnection#zSetCommands()}}. */
	@Override
	@Deprecated
	default Boolean zAdd(byte[] key, double score, byte[] value) {
		return zSetCommands().zAdd(key, score, value);
	}

	/** @deprecated in favor of {@link RedisConnection#zSetCommands()}}. */
	@Override
	@Deprecated
	default Long zAdd(byte[] key, Set<Tuple> tuples) {
		return zSetCommands().zAdd(key, tuples);
	}

	/** @deprecated in favor of {@link RedisConnection#zSetCommands()}}. */
	@Override
	@Deprecated
	default Long zCard(byte[] key) {
		return zSetCommands().zCard(key);
	}

	/** @deprecated in favor of {@link RedisConnection#zSetCommands()}}. */
	@Override
	@Deprecated
	default Long zCount(byte[] key, double min, double max) {
		return zSetCommands().zCount(key, min, max);
	}

	/** @deprecated in favor of {@link RedisConnection#zSetCommands()}}. */
	@Override
	@Deprecated
	default Long zLexCount(byte[] key, Range range) {
		return zSetCommands().zLexCount(key, range);
	}

	/** @deprecated in favor of {@link RedisConnection#zSetCommands()}}. */
	@Override
	@Deprecated
	default Long zCount(byte[] key, Range range) {
		return zSetCommands().zCount(key, range);
	}

	/** @deprecated in favor of {@link RedisConnection#zSetCommands()}}. */
	@Override
	@Deprecated
	default Double zIncrBy(byte[] key, double increment, byte[] value) {
		return zSetCommands().zIncrBy(key, increment, value);
	}

	/** @deprecated in favor of {@link RedisConnection#zSetCommands()}}. */
	@Override
	@Deprecated
	default Long zInterStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
		return zSetCommands().zInterStore(destKey, aggregate, weights, sets);
	}

	/** @deprecated in favor of {@link RedisConnection#zSetCommands()}}. */
	@Override
	@Deprecated
	default Long zInterStore(byte[] destKey, Aggregate aggregate, Weights weights, byte[]... sets) {
		return zSetCommands().zInterStore(destKey, aggregate, weights, sets);
	}

	/** @deprecated in favor of {@link RedisConnection#zSetCommands()}}. */
	@Override
	@Deprecated
	default Long zInterStore(byte[] destKey, byte[]... sets) {
		return zSetCommands().zInterStore(destKey, sets);
	}

	/** @deprecated in favor of {@link RedisConnection#zSetCommands()}}. */
	@Override
	@Deprecated
	default Set<byte[]> zRange(byte[] key, long start, long end) {
		return zSetCommands().zRange(key, start, end);
	}

	/** @deprecated in favor of {@link RedisConnection#zSetCommands()}}. */
	@Override
	@Deprecated
	default Set<Tuple> zRangeWithScores(byte[] key, long start, long end) {
		return zSetCommands().zRangeWithScores(key, start, end);
	}

	/** @deprecated in favor of {@link RedisConnection#zSetCommands()}}. */
	@Override
	@Deprecated
	default Set<byte[]> zRangeByLex(byte[] key, Range range, Limit limit) {
		return zSetCommands().zRangeByLex(key, range, limit);
	}

	/** @deprecated in favor of {@link RedisConnection#zSetCommands()}}. */
	@Override
	@Deprecated
	default Set<byte[]> zRevRangeByLex(byte[] key, Range range, Limit limit) {
		return zSetCommands().zRevRangeByLex(key, range, limit);
	}

	/** @deprecated in favor of {@link RedisConnection#zSetCommands()}}. */
	@Override
	@Deprecated
	default Set<byte[]> zRangeByScore(byte[] key, Range range, Limit limit) {
		return zSetCommands().zRangeByScore(key, range, limit);
	}

	/** @deprecated in favor of {@link RedisConnection#zSetCommands()}}. */
	@Override
	@Deprecated
	default Set<Tuple> zRangeByScoreWithScores(byte[] key, Range range, Limit limit) {
		return zSetCommands().zRangeByScoreWithScores(key, range, limit);
	}

	/** @deprecated in favor of {@link RedisConnection#zSetCommands()}}. */
	@Override
	@Deprecated
	default Set<Tuple> zRevRangeWithScores(byte[] key, long start, long end) {
		return zSetCommands().zRevRangeWithScores(key, start, end);
	}

	/** @deprecated in favor of {@link RedisConnection#zSetCommands()}}. */
	@Override
	@Deprecated
	default Set<byte[]> zRevRangeByScore(byte[] key, Range range, Limit limit) {
		return zSetCommands().zRevRangeByScore(key, range, limit);
	}

	/** @deprecated in favor of {@link RedisConnection#zSetCommands()}}. */
	@Override
	@Deprecated
	default Set<Tuple> zRevRangeByScoreWithScores(byte[] key, Range range, Limit limit) {
		return zSetCommands().zRevRangeByScoreWithScores(key, range, limit);
	}

	/** @deprecated in favor of {@link RedisConnection#zSetCommands()}}. */
	@Override
	@Deprecated
	default Long zRank(byte[] key, byte[] value) {
		return zSetCommands().zRank(key, value);
	}

	/** @deprecated in favor of {@link RedisConnection#zSetCommands()}}. */
	@Override
	@Deprecated
	default Long zRem(byte[] key, byte[]... values) {
		return zSetCommands().zRem(key, values);
	}

	/** @deprecated in favor of {@link RedisConnection#zSetCommands()}}. */
	@Override
	@Deprecated
	default Long zRemRange(byte[] key, long start, long end) {
		return zSetCommands().zRemRange(key, start, end);
	}

	/** @deprecated in favor of {@link RedisConnection#zSetCommands()}}. */
	@Override
	@Deprecated
	default Long zRemRangeByLex(byte[] key, Range range) {
		return zSetCommands().zRemRangeByLex(key, range);
	}

	/** @deprecated in favor of {@link RedisConnection#zSetCommands()}}. */
	@Override
	@Deprecated
	default Long zRemRangeByScore(byte[] key, Range range) {
		return zSetCommands().zRemRangeByScore(key, range);
	}

	/** @deprecated in favor of {@link RedisConnection#zSetCommands()}}. */
	@Override
	@Deprecated
	default Long zRemRangeByScore(byte[] key, double min, double max) {
		return zSetCommands().zRemRangeByScore(key, min, max);
	}

	/** @deprecated in favor of {@link RedisConnection#zSetCommands()}}. */
	@Override
	@Deprecated
	default Set<byte[]> zRevRange(byte[] key, long start, long end) {
		return zSetCommands().zRevRange(key, start, end);
	}

	/** @deprecated in favor of {@link RedisConnection#zSetCommands()}}. */
	@Override
	@Deprecated
	default Long zRevRank(byte[] key, byte[] value) {
		return zSetCommands().zRevRank(key, value);
	}

	/** @deprecated in favor of {@link RedisConnection#zSetCommands()}}. */
	@Override
	@Deprecated
	default Double zScore(byte[] key, byte[] value) {
		return zSetCommands().zScore(key, value);
	}

	/** @deprecated in favor of {@link RedisConnection#zSetCommands()}}. */
	@Override
	@Deprecated
	default Long zUnionStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
		return zSetCommands().zUnionStore(destKey, aggregate, weights, sets);
	}

	/** @deprecated in favor of {@link RedisConnection#zSetCommands()}}. */
	@Override
	@Deprecated
	default Long zUnionStore(byte[] destKey, Aggregate aggregate, Weights weights, byte[]... sets) {
		return zSetCommands().zUnionStore(destKey, aggregate, weights, sets);
	}

	/** @deprecated in favor of {@link RedisConnection#zSetCommands()}}. */
	@Override
	@Deprecated
	default Long zUnionStore(byte[] destKey, byte[]... sets) {
		return zSetCommands().zUnionStore(destKey, sets);
	}

	/** @deprecated in favor of {@link RedisConnection#zSetCommands()}}. */
	@Override
	@Deprecated
	default Cursor<Tuple> zScan(byte[] key, ScanOptions options) {
		return zSetCommands().zScan(key, options);
	}

	/** @deprecated in favor of {@link RedisConnection#zSetCommands()}}. */
	@Override
	@Deprecated
	default Set<byte[]> zRangeByScore(byte[] key, String min, String max) {
		return zSetCommands().zRangeByScore(key, min, max);
	}

	/** @deprecated in favor of {@link RedisConnection#zSetCommands()}}. */
	@Override
	@Deprecated
	default Set<byte[]> zRangeByScore(byte[] key, String min, String max, long offset, long count) {
		return zSetCommands().zRangeByScore(key, min, max, offset, count);
	}

	// HASH COMMANDS

	/** @deprecated in favor of {@link RedisConnection#hashCommands()}}. */
	@Override
	@Deprecated
	default Boolean hSet(byte[] key, byte[] field, byte[] value) {
		return hashCommands().hSet(key, field, value);
	}

	/** @deprecated in favor of {@link RedisConnection#hashCommands()}}. */
	@Override
	@Deprecated
	default Boolean hSetNX(byte[] key, byte[] field, byte[] value) {
		return hashCommands().hSetNX(key, field, value);
	}

	/** @deprecated in favor of {@link RedisConnection#hashCommands()}}. */
	@Override
	@Deprecated
	default Long hDel(byte[] key, byte[]... fields) {
		return hashCommands().hDel(key, fields);
	}

	/** @deprecated in favor of {@link RedisConnection#hashCommands()}}. */
	@Override
	@Deprecated
	default Boolean hExists(byte[] key, byte[] field) {
		return hashCommands().hExists(key, field);
	}

	/** @deprecated in favor of {@link RedisConnection#hashCommands()}}. */
	@Override
	@Deprecated
	default byte[] hGet(byte[] key, byte[] field) {
		return hashCommands().hGet(key, field);
	}

	/** @deprecated in favor of {@link RedisConnection#hashCommands()}}. */
	@Override
	@Deprecated
	default Map<byte[], byte[]> hGetAll(byte[] key) {
		return hashCommands().hGetAll(key);
	}

	/** @deprecated in favor of {@link RedisConnection#hashCommands()}}. */
	@Override
	@Deprecated
	default Double hIncrBy(byte[] key, byte[] field, double delta) {
		return hashCommands().hIncrBy(key, field, delta);
	}

	/** @deprecated in favor of {@link RedisConnection#hashCommands()}}. */
	@Override
	@Deprecated
	default Long hIncrBy(byte[] key, byte[] field, long delta) {
		return hashCommands().hIncrBy(key, field, delta);
	}

	/** @deprecated in favor of {@link RedisConnection#hashCommands()}}. */
	@Override
	@Deprecated
	default Set<byte[]> hKeys(byte[] key) {
		return hashCommands().hKeys(key);
	}

	@Override
	default Long hLen(byte[] key) {
		return hashCommands().hLen(key);
	}

	/** @deprecated in favor of {@link RedisConnection#hashCommands()}}. */
	@Override
	@Deprecated
	default List<byte[]> hMGet(byte[] key, byte[]... fields) {
		return hashCommands().hMGet(key, fields);
	}

	/** @deprecated in favor of {@link RedisConnection#hashCommands()}}. */
	@Override
	@Deprecated
	default void hMSet(byte[] key, Map<byte[], byte[]> hashes) {
		hashCommands().hMSet(key, hashes);
	}

	/** @deprecated in favor of {@link RedisConnection#hashCommands()}}. */
	@Override
	@Deprecated
	default List<byte[]> hVals(byte[] key) {
		return hashCommands().hVals(key);
	}

	/** @deprecated in favor of {@link RedisConnection#hashCommands()}}. */
	@Override
	@Deprecated
	default Cursor<Entry<byte[], byte[]>> hScan(byte[] key, ScanOptions options) {
		return hashCommands().hScan(key, options);
	}

	/** @deprecated in favor of {@link RedisConnection#hashCommands()}. */
	@Override
	@Deprecated
	default Long hStrLen(byte[] key, byte[] field) {
		return hashCommands().hStrLen(key, field);
	}

	// GEO COMMANDS

	/** @deprecated in favor of {@link RedisConnection#geoCommands()}}. */
	@Override
	@Deprecated
	default Long geoAdd(byte[] key, Point point, byte[] member) {
		return geoCommands().geoAdd(key, point, member);
	}

	/** @deprecated in favor of {@link RedisConnection#geoCommands()}}. */
	@Override
	@Deprecated
	default Long geoAdd(byte[] key, Map<byte[], Point> memberCoordinateMap) {
		return geoCommands().geoAdd(key, memberCoordinateMap);
	}

	/** @deprecated in favor of {@link RedisConnection#geoCommands()}}. */
	@Override
	@Deprecated
	default Long geoAdd(byte[] key, Iterable<GeoLocation<byte[]>> locations) {
		return geoCommands().geoAdd(key, locations);
	}

	/** @deprecated in favor of {@link RedisConnection#geoCommands()}}. */
	@Override
	@Deprecated
	default Distance geoDist(byte[] key, byte[] member1, byte[] member2) {
		return geoCommands().geoDist(key, member1, member2);
	}

	/** @deprecated in favor of {@link RedisConnection#geoCommands()}}. */
	@Override
	@Deprecated
	default Distance geoDist(byte[] key, byte[] member1, byte[] member2, Metric metric) {
		return geoCommands().geoDist(key, member1, member2, metric);
	}

	/** @deprecated in favor of {@link RedisConnection#geoCommands()}}. */
	@Override
	@Deprecated
	default List<String> geoHash(byte[] key, byte[]... members) {
		return geoCommands().geoHash(key, members);
	}

	/** @deprecated in favor of {@link RedisConnection#geoCommands()}}. */
	@Override
	@Deprecated
	default List<Point> geoPos(byte[] key, byte[]... members) {
		return geoCommands().geoPos(key, members);
	}

	/** @deprecated in favor of {@link RedisConnection#geoCommands()}}. */
	@Override
	@Deprecated
	default GeoResults<GeoLocation<byte[]>> geoRadius(byte[] key, Circle within) {
		return geoCommands().geoRadius(key, within);
	}

	/** @deprecated in favor of {@link RedisConnection#geoCommands()}}. */
	@Override
	@Deprecated
	default GeoResults<GeoLocation<byte[]>> geoRadius(byte[] key, Circle within, GeoRadiusCommandArgs args) {
		return geoCommands().geoRadius(key, within, args);
	}

	/** @deprecated in favor of {@link RedisConnection#geoCommands()}}. */
	@Override
	@Deprecated
	default GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte[] key, byte[] member, Distance radius) {
		return geoCommands().geoRadiusByMember(key, member, radius);
	}

	/** @deprecated in favor of {@link RedisConnection#geoCommands()}}. */
	@Override
	@Deprecated
	default GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte[] key, byte[] member, Distance radius,
			GeoRadiusCommandArgs args) {
		return geoCommands().geoRadiusByMember(key, member, radius, args);
	}

	/** @deprecated in favor of {@link RedisConnection#geoCommands()}}. */
	@Override
	@Deprecated
	default Long geoRemove(byte[] key, byte[]... members) {
		return geoCommands().geoRemove(key, members);
	}

	// HLL COMMANDS

	/** @deprecated in favor of {@link RedisConnection#hyperLogLogCommands()}. */
	@Override
	@Deprecated
	default Long pfAdd(byte[] key, byte[]... values) {
		return hyperLogLogCommands().pfAdd(key, values);
	}

	/** @deprecated in favor of {@link RedisConnection#hyperLogLogCommands()}. */
	@Override
	@Deprecated
	default Long pfCount(byte[]... keys) {
		return hyperLogLogCommands().pfCount(keys);
	}

	/** @deprecated in favor of {@link RedisConnection#hyperLogLogCommands()}. */
	@Override
	@Deprecated
	default void pfMerge(byte[] destinationKey, byte[]... sourceKeys) {
		hyperLogLogCommands().pfMerge(destinationKey, sourceKeys);
	}

	// SERVER COMMANDS

	/** @deprecated in favor of {@link RedisConnection#serverCommands()}. */
	@Override
	@Deprecated
	default void bgWriteAof() {
		serverCommands().bgWriteAof();
	}

	/** @deprecated in favor of {@link RedisConnection#serverCommands()}. */
	@Override
	@Deprecated
	default void bgReWriteAof() {
		serverCommands().bgReWriteAof();
	}

	/** @deprecated in favor of {@link RedisConnection#serverCommands()}. */
	@Override
	@Deprecated
	default void bgSave() {
		serverCommands().bgSave();
	}

	/** @deprecated in favor of {@link RedisConnection#serverCommands()}. */
	@Override
	@Deprecated
	default Long lastSave() {
		return serverCommands().lastSave();
	}

	/** @deprecated in favor of {@link RedisConnection#serverCommands()}. */
	@Override
	@Deprecated
	default void save() {
		serverCommands().save();
	}

	/** @deprecated in favor of {@link RedisConnection#serverCommands()}. */
	@Override
	@Deprecated
	default Long dbSize() {
		return serverCommands().dbSize();
	}

	/** @deprecated in favor of {@link RedisConnection#serverCommands()}. */
	@Override
	@Deprecated
	default void flushDb() {
		serverCommands().flushDb();
	}

	/** @deprecated in favor of {@link RedisConnection#serverCommands()}. */
	@Override
	@Deprecated
	default void flushAll() {
		serverCommands().flushAll();
	}

	/** @deprecated in favor of {@link RedisConnection#serverCommands()}. */
	@Override
	@Deprecated
	default Properties info() {
		return serverCommands().info();
	}

	/** @deprecated in favor of {@link RedisConnection#serverCommands()}. */
	@Override
	@Deprecated
	default Properties info(String section) {
		return serverCommands().info(section);
	}

	/** @deprecated in favor of {@link RedisConnection#serverCommands()}. */
	@Override
	@Deprecated
	default void shutdown() {
		serverCommands().shutdown();
	}

	/** @deprecated in favor of {@link RedisConnection#serverCommands()}. */
	@Override
	@Deprecated
	default void shutdown(ShutdownOption option) {
		serverCommands().shutdown(option);
	}

	/** @deprecated in favor of {@link RedisConnection#serverCommands()}. */
	@Override
	@Deprecated
	default Properties getConfig(String pattern) {
		return serverCommands().getConfig(pattern);
	}

	/** @deprecated in favor of {@link RedisConnection#serverCommands()}. */
	@Override
	@Deprecated
	default void setConfig(String param, String value) {
		serverCommands().setConfig(param, value);
	}

	/** @deprecated in favor of {@link RedisConnection#serverCommands()}. */
	@Override
	@Deprecated
	default void resetConfigStats() {
		serverCommands().resetConfigStats();
	}

	/** @deprecated in favor of {@link RedisConnection#serverCommands()}. */
	@Override
	@Deprecated
	default Long time() {
		return serverCommands().time();
	}

	/** @deprecated in favor of {@link RedisConnection#serverCommands()}. */
	@Override
	@Deprecated
	default Long time(TimeUnit timeUnit) {
		return serverCommands().time(timeUnit);
	}

	/** @deprecated in favor of {@link RedisConnection#serverCommands()}. */
	@Override
	@Deprecated
	default void killClient(String host, int port) {
		serverCommands().killClient(host, port);
	}

	/** @deprecated in favor of {@link RedisConnection#serverCommands()}. */
	@Override
	@Deprecated
	default void setClientName(byte[] name) {
		serverCommands().setClientName(name);
	}

	/** @deprecated in favor of {@link RedisConnection#serverCommands()}. */
	@Override
	@Deprecated
	default String getClientName() {
		return serverCommands().getClientName();
	}

	/** @deprecated in favor of {@link RedisConnection#serverCommands()}. */
	@Override
	@Deprecated
	default List<RedisClientInfo> getClientList() {
		return serverCommands().getClientList();
	}

	/** @deprecated in favor of {@link RedisConnection#serverCommands()}. */
	@Override
	@Deprecated
	default void slaveOf(String host, int port) {
		serverCommands().slaveOf(host, port);
	}

	/** @deprecated in favor of {@link RedisConnection#serverCommands()}. */
	@Override
	@Deprecated
	default void slaveOfNoOne() {
		serverCommands().slaveOfNoOne();
	}

	/** @deprecated in favor of {@link RedisConnection#serverCommands()}. */
	@Override
	@Deprecated
	default void migrate(byte[] key, RedisNode target, int dbIndex, @Nullable MigrateOption option) {
		serverCommands().migrate(key, target, dbIndex, option);
	}

	/** @deprecated in favor of {@link RedisConnection#serverCommands()}. */
	@Override
	@Deprecated
	default void migrate(byte[] key, RedisNode target, int dbIndex, @Nullable MigrateOption option, long timeout) {
		serverCommands().migrate(key, target, dbIndex, option, timeout);
	}

	// SCRIPTING COMMANDS

	/** @deprecated in favor of {@link RedisConnection#scriptingCommands()}. */
	@Override
	@Deprecated
	default void scriptFlush() {
		scriptingCommands().scriptFlush();
	}

	/** @deprecated in favor of {@link RedisConnection#scriptingCommands()}. */
	@Override
	@Deprecated
	default void scriptKill() {
		scriptingCommands().scriptKill();
	}

	/** @deprecated in favor of {@link RedisConnection#scriptingCommands()}. */
	@Override
	@Deprecated
	default String scriptLoad(byte[] script) {
		return scriptingCommands().scriptLoad(script);
	}

	/** @deprecated in favor of {@link RedisConnection#scriptingCommands()}. */
	@Override
	@Deprecated
	default List<Boolean> scriptExists(String... scriptShas) {
		return scriptingCommands().scriptExists(scriptShas);
	}

	/** @deprecated in favor of {@link RedisConnection#scriptingCommands()}. */
	@Override
	@Deprecated
	default <T> T eval(byte[] script, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
		return scriptingCommands().eval(script, returnType, numKeys, keysAndArgs);
	}

	/** @deprecated in favor of {@link RedisConnection#scriptingCommands()}. */
	@Override
	@Deprecated
	default <T> T evalSha(String scriptSha, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
		return scriptingCommands().evalSha(scriptSha, returnType, numKeys, keysAndArgs);
	}

	/** @deprecated in favor of {@link RedisConnection#scriptingCommands()}. */
	@Override
	@Deprecated
	default <T> T evalSha(byte[] scriptSha, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
		return scriptingCommands().evalSha(scriptSha, returnType, numKeys, keysAndArgs);
	}
}
