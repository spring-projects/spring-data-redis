/*
 * Copyright 2011-2025 the original author or authors.
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
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Metric;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.convert.Converters;
import org.springframework.data.redis.connection.convert.ListConverter;
import org.springframework.data.redis.connection.convert.MapConverter;
import org.springframework.data.redis.connection.convert.SetConverter;
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
import org.springframework.data.redis.connection.stream.StringRecord;
import org.springframework.data.redis.connection.zset.Aggregate;
import org.springframework.data.redis.connection.zset.DefaultTuple;
import org.springframework.data.redis.connection.zset.Tuple;
import org.springframework.data.redis.connection.zset.Weights;
import org.springframework.data.redis.core.ConvertingCursor;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.core.types.RedisClientInfo;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.data.redis.domain.geo.GeoReference.GeoMemberReference;
import org.springframework.data.redis.domain.geo.GeoShape;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Default implementation of {@link StringRedisConnection}.
 *
 * @author Costin Leau
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Mark Paluch
 * @author Ninad Divadkar
 * @author Tugdual Grall
 * @author Andrey Shlykov
 * @author dengliming
 * @author ihaohong
 * @author Dennis Neufeld
 * @author Shyngys Sapraliyev
 * @author Jeonggyu Choi
 * @author Mingi Lee
 */
@NullUnmarked
@SuppressWarnings({ "ConstantConditions", "deprecation" })
public class DefaultStringRedisConnection implements StringRedisConnection, DecoratedRedisConnection {

	private static final byte[][] EMPTY_2D_BYTE_ARRAY = new byte[0][];

	private final Log log = LogFactory.getLog(DefaultStringRedisConnection.class);
	private final RedisConnection delegate;
	private final RedisSerializer<String> serializer;
	private Converter<byte[], String> bytesToString = new DeserializingConverter();
	private Converter<String, byte[]> stringToBytes = new SerializingConverter();
	private final TupleConverter tupleConverter = new TupleConverter();
	private SetConverter<Tuple, StringTuple> tupleToStringTuple = new SetConverter<>(tupleConverter);
	private SetConverter<StringTuple, Tuple> stringTupleToTuple = new SetConverter<>(new StringTupleConverter());
	private ListConverter<Tuple, StringTuple> tupleListToStringTuple = new ListConverter<>(new TupleConverter());
	private ListConverter<byte[], String> byteListToStringList = new ListConverter<>(bytesToString);
	private MapConverter<byte[], String> byteMapToStringMap = new MapConverter<>(bytesToString);
	private SetConverter<byte[], String> byteSetToStringSet = new SetConverter<>(bytesToString);
	private Converter<GeoResults<GeoLocation<byte[]>>, GeoResults<GeoLocation<String>>> byteGeoResultsToStringGeoResults;
	private Converter<ByteRecord, StringRecord> byteMapRecordToStringMapRecordConverter = new Converter<ByteRecord, StringRecord>() {

		@Override
		public @Nullable StringRecord convert(ByteRecord source) {
			return StringRecord.of(source.deserialize(serializer));
		}
	};

	private ListConverter<ByteRecord, StringRecord> listByteMapRecordToStringMapRecordConverter = new ListConverter<>(
			byteMapRecordToStringMapRecordConverter);

	@SuppressWarnings("rawtypes") private Queue<Converter> pipelineConverters = new LinkedList<>();
	@SuppressWarnings("rawtypes") private Queue<Converter> txConverters = new LinkedList<>();
	private boolean deserializePipelineAndTxResults = false;

	private Entry<String, String> convertEntry(Entry<byte[], byte[]> source) {
		return Converters.entryOf(bytesToString.convert(source.getKey()), bytesToString.convert(source.getValue()));
	}

	private class DeserializingConverter implements Converter<byte[], String> {
		public String convert(byte[] source) {
			return serializer.deserialize(source);
		}
	}

	private class SerializingConverter implements Converter<String, byte[]> {

		@Override
		public byte @Nullable[] convert(String source) {
			return serializer.serialize(source);
		}
	}

	private class TupleConverter implements Converter<Tuple, StringTuple> {
		public StringTuple convert(Tuple source) {
			return new DefaultStringTuple(source, serializer.deserialize(source.getValue()));
		}
	}

	private class StringTupleConverter implements Converter<StringTuple, Tuple> {
		public Tuple convert(StringTuple source) {
			return new DefaultTuple(source.getValue(), source.getScore());
		}
	}

	@SuppressWarnings("rawtypes")
	private class TransactionResultConverter implements Converter<List<Object>, List<Object>> {
		private Queue<Converter> txConverters;

		public TransactionResultConverter(Queue<Converter> txConverters) {
			this.txConverters = txConverters;
		}

		public List<Object> convert(List<Object> execResults) {
			return convertResults(execResults, txConverters);
		}
	}

	/**
	 * Constructs a new <code>DefaultStringRedisConnection</code> instance. Uses {@link StringRedisSerializer} as
	 * underlying serializer.
	 *
	 * @param connection Redis connection
	 */
	public DefaultStringRedisConnection(RedisConnection connection) {
		this(connection, RedisSerializer.string());
	}

	/**
	 * Constructs a new <code>DefaultStringRedisConnection</code> instance.
	 *
	 * @param connection Redis connection
	 * @param serializer String serializer
	 */
	public DefaultStringRedisConnection(RedisConnection connection, RedisSerializer<String> serializer) {

		Assert.notNull(connection, "connection is required");
		Assert.notNull(serializer, "serializer is required");

		this.delegate = connection;
		this.serializer = serializer;
		this.byteGeoResultsToStringGeoResults = Converters.deserializingGeoResultsConverter(serializer);
	}

	@Override
	public RedisCommands commands() {
		return this;
	}

	@Override
	public RedisGeoCommands geoCommands() {
		return this;
	}

	@Override
	public RedisHashCommands hashCommands() {
		return this;
	}

	@Override
	public RedisHyperLogLogCommands hyperLogLogCommands() {
		return this;
	}

	@Override
	public RedisKeyCommands keyCommands() {
		return this;
	}

	@Override
	public RedisListCommands listCommands() {
		return this;
	}

	@Override
	public RedisSetCommands setCommands() {
		return this;
	}

	@Override
	public RedisScriptingCommands scriptingCommands() {
		return this;
	}

	@Override
	public RedisServerCommands serverCommands() {
		return this;
	}

	@Override
	public RedisStreamCommands streamCommands() {
		return this;
	}

	@Override
	public RedisStringCommands stringCommands() {
		return this;
	}

	@Override
	public RedisZSetCommands zSetCommands() {
		return this;
	}

	@Override
	public Long append(byte[] key, byte[] value) {
		return convertAndReturn(delegate.append(key, value), Converters.identityConverter());
	}

	@Override
	public void bgSave() {
		delegate.bgSave();
	}

	@Override
	public void bgReWriteAof() {
		delegate.bgReWriteAof();
	}

	@Override
	public List<byte[]> bLPop(int timeout, byte[]... keys) {
		return convertAndReturn(delegate.bLPop(timeout, keys), Converters.identityConverter());
	}

	@Override
	public List<byte[]> bRPop(int timeout, byte[]... keys) {
		return convertAndReturn(delegate.bRPop(timeout, keys), Converters.identityConverter());
	}

	@Override
	public byte[] bRPopLPush(int timeout, byte[] srcKey, byte[] dstKey) {
		return convertAndReturn(delegate.bRPopLPush(timeout, srcKey, dstKey), Converters.identityConverter());
	}

	@Override
	public void close() throws RedisSystemException {
		delegate.close();
	}

	@Override
	public Boolean copy(byte[] sourceKey, byte[] targetKey, boolean replace) {
		return convertAndReturn(delegate.copy(sourceKey, targetKey, replace), Converters.identityConverter());
	}

	@Override
	public Long dbSize() {
		return convertAndReturn(delegate.dbSize(), Converters.identityConverter());
	}

	@Override
	public Long decr(byte[] key) {
		return convertAndReturn(delegate.decr(key), Converters.identityConverter());
	}

	@Override
	public Long decrBy(byte[] key, long value) {
		return convertAndReturn(delegate.decrBy(key, value), Converters.identityConverter());
	}

	@Override
	public Long del(byte[]... keys) {
		return convertAndReturn(delegate.del(keys), Converters.identityConverter());
	}

	@Override
	public Long unlink(byte[]... keys) {
		return convertAndReturn(delegate.unlink(keys), Converters.identityConverter());
	}

	@Override
	public void discard() {
		try {
			delegate.discard();
		} finally {
			txConverters.clear();
		}
	}

	@Override
	public byte[] echo(byte[] message) {
		return convertAndReturn(delegate.echo(message), Converters.identityConverter());
	}

	@Override
	@SuppressWarnings("rawtypes")
	public List<Object> exec() {

		try {
			List<Object> results = delegate.exec();
			if (isPipelined()) {
				pipelineConverters.add(new TransactionResultConverter(new LinkedList<>(txConverters)));
				return results;
			}
			return convertResults(results, txConverters);
		} finally {
			txConverters.clear();
		}
	}

	@Override
	public Boolean exists(byte[] key) {
		return convertAndReturn(delegate.exists(key), Converters.identityConverter());
	}

	@Override
	public Long exists(String... keys) {
		return convertAndReturn(delegate.exists(serializeMulti(keys)), Converters.identityConverter());
	}

	@Override
	public Long exists(byte[]... keys) {
		return convertAndReturn(delegate.exists(keys), Converters.identityConverter());
	}

	@Override
	public Boolean expire(byte[] key, long seconds, ExpirationOptions.Condition condition) {
		return convertAndReturn(delegate.expire(key, seconds, condition), Converters.identityConverter());
	}

	@Override
	public Boolean expireAt(byte[] key, long unixTime, ExpirationOptions.Condition condition) {
		return convertAndReturn(delegate.expireAt(key, unixTime, condition), Converters.identityConverter());
	}

	@Override
	public void flushAll() {
		delegate.flushAll();
	}

	@Override
	public void flushAll(FlushOption option) {
		delegate.flushAll(option);
	}

	@Override
	public void flushDb() {
		delegate.flushDb();
	}

	@Override
	public void flushDb(FlushOption option) {
		delegate.flushDb(option);
	}

	@Override
	public byte[] get(byte[] key) {
		return convertAndReturn(delegate.get(key), Converters.identityConverter());
	}


	@Override
	public byte @Nullable[] getDel(byte[] key) {
		return convertAndReturn(delegate.getDel(key), Converters.identityConverter());
	}

	@Override
	public @Nullable String getDel(String key) {
		return convertAndReturn(delegate.getDel(serialize(key)), bytesToString);
	}


	@Override
	public byte @Nullable[] getEx(byte[] key, Expiration expiration) {
		return convertAndReturn(delegate.getEx(key, expiration), Converters.identityConverter());
	}

	@Override
	public @Nullable String getEx(String key, Expiration expiration) {
		return convertAndReturn(delegate.getEx(serialize(key), expiration), bytesToString);
	}

	@Override
	public Boolean getBit(byte[] key, long offset) {
		return convertAndReturn(delegate.getBit(key, offset), Converters.identityConverter());
	}

	@Override
	public Properties getConfig(String pattern) {
		return convertAndReturn(delegate.getConfig(pattern), Converters.identityConverter());
	}

	@Override
	public Object getNativeConnection() {
		return convertAndReturn(delegate.getNativeConnection(), Converters.identityConverter());
	}

	@Override
	public byte[] getRange(byte[] key, long start, long end) {
		return convertAndReturn(delegate.getRange(key, start, end), Converters.identityConverter());
	}

	@Override
	public byte[] getSet(byte[] key, byte[] value) {
		return convertAndReturn(delegate.getSet(key, value), Converters.identityConverter());
	}

	@Override
	public Subscription getSubscription() {
		return delegate.getSubscription();
	}

	@Override
	public Long hDel(byte[] key, byte[]... fields) {
		return convertAndReturn(delegate.hDel(key, fields), Converters.identityConverter());
	}

	@Override
	public Boolean hExists(byte[] key, byte[] field) {
		return convertAndReturn(delegate.hExists(key, field), Converters.identityConverter());
	}

	@Override
	public byte[] hGet(byte[] key, byte[] field) {
		return convertAndReturn(delegate.hGet(key, field), Converters.identityConverter());
	}

	@Override
	public Map<byte[], byte[]> hGetAll(byte[] key) {
		return convertAndReturn(delegate.hGetAll(key), Converters.identityConverter());
	}

	@Override
	public Long hIncrBy(byte[] key, byte[] field, long delta) {
		return convertAndReturn(delegate.hIncrBy(key, field, delta), Converters.identityConverter());
	}

	@Override
	public Double hIncrBy(byte[] key, byte[] field, double delta) {
		return convertAndReturn(delegate.hIncrBy(key, field, delta), Converters.identityConverter());
	}

	@Override
	public Set<byte[]> hKeys(byte[] key) {
		return convertAndReturn(delegate.hKeys(key), Converters.identityConverter());
	}

	@Override
	public Long hLen(byte[] key) {
		return convertAndReturn(delegate.hLen(key), Converters.identityConverter());
	}

	@Override
	public List<byte[]> hMGet(byte[] key, byte[]... fields) {
		return convertAndReturn(delegate.hMGet(key, fields), Converters.identityConverter());
	}

	@Override
	public void hMSet(byte[] key, Map<byte[], byte[]> hashes) {
		delegate.hMSet(key, hashes);
	}

	@Override
	public Boolean hSet(byte[] key, byte[] field, byte[] value) {
		return convertAndReturn(delegate.hSet(key, field, value), Converters.identityConverter());
	}

	@Override
	public Boolean hSetNX(byte[] key, byte[] field, byte[] value) {
		return convertAndReturn(delegate.hSetNX(key, field, value), Converters.identityConverter());
	}

	@Override
	public List<byte[]> hVals(byte[] key) {
		return convertAndReturn(delegate.hVals(key), Converters.identityConverter());
	}

	@Override
	public Long incr(byte[] key) {
		return convertAndReturn(delegate.incr(key), Converters.identityConverter());
	}

	@Override
	public Long incrBy(byte[] key, long value) {

		return convertAndReturn(delegate.incrBy(key, value), Converters.identityConverter());
	}

	@Override
	public Double incrBy(byte[] key, double value) {
		return convertAndReturn(delegate.incrBy(key, value), Converters.identityConverter());
	}

	@Override
	public Properties info() {
		return convertAndReturn(delegate.info(), Converters.identityConverter());
	}

	@Override
	public Properties info(String section) {
		return convertAndReturn(delegate.info(section), Converters.identityConverter());
	}

	@Override
	public boolean isClosed() {
		return delegate.isClosed();
	}

	@Override
	public boolean isQueueing() {
		return delegate.isQueueing();
	}

	@Override
	public boolean isSubscribed() {
		return delegate.isSubscribed();
	}

	@Override
	public Set<byte[]> keys(byte[] pattern) {
		return convertAndReturn(delegate.keys(pattern), Converters.identityConverter());
	}

	@Override
	public Long lastSave() {
		return convertAndReturn(delegate.lastSave(), Converters.identityConverter());
	}

	@Override
	public byte[] lIndex(byte[] key, long index) {
		return convertAndReturn(delegate.lIndex(key, index), Converters.identityConverter());
	}

	@Override
	public Long lInsert(byte[] key, Position where, byte[] pivot, byte[] value) {
		return convertAndReturn(delegate.lInsert(key, where, pivot, value), Converters.identityConverter());
	}

	@Override
	public byte[] lMove(byte[] sourceKey, byte[] destinationKey, Direction from, Direction to) {
		return convertAndReturn(delegate.lMove(sourceKey, destinationKey, from, to), Converters.identityConverter());
	}

	@Override
	public byte[] bLMove(byte[] sourceKey, byte[] destinationKey, Direction from, Direction to, double timeout) {
		return convertAndReturn(delegate.bLMove(sourceKey, destinationKey, from, to, timeout),
				Converters.identityConverter());
	}

	@Override
	public String lMove(String sourceKey, String destinationKey, Direction from, Direction to) {
		return convertAndReturn(delegate.lMove(serialize(sourceKey), serialize(destinationKey), from, to), bytesToString);
	}

	@Override
	public String bLMove(String sourceKey, String destinationKey, Direction from, Direction to, double timeout) {
		return convertAndReturn(delegate.bLMove(serialize(sourceKey), serialize(destinationKey), from, to, timeout),
				bytesToString);
	}

	@Override
	public Long lLen(byte[] key) {
		return convertAndReturn(delegate.lLen(key), Converters.identityConverter());
	}

	@Override
	public byte[] lPop(byte[] key) {
		return convertAndReturn(delegate.lPop(key), Converters.identityConverter());
	}

	@Override
	public List<byte[]> lPop(byte[] key, long count) {
		return convertAndReturn(delegate.lPop(key, count), Converters.identityConverter());
	}

	@Override
	public List<Long> lPos(byte[] key, byte[] element, @Nullable Integer rank, @Nullable Integer count) {
		return convertAndReturn(delegate.lPos(key, element, rank, count), Converters.identityConverter());
	}

	@Override
	public Long lPush(byte[] key, byte[]... values) {
		return convertAndReturn(delegate.lPush(key, values), Converters.identityConverter());
	}

	@Override
	public Long lPushX(byte[] key, byte[] value) {
		return convertAndReturn(delegate.lPushX(key, value), Converters.identityConverter());
	}

	@Override
	public List<byte[]> lRange(byte[] key, long start, long end) {
		return convertAndReturn(delegate.lRange(key, start, end), Converters.identityConverter());
	}

	@Override
	public Long lRem(byte[] key, long count, byte[] value) {

		return convertAndReturn(delegate.lRem(key, count, value), Converters.identityConverter());
	}

	@Override
	public void lSet(byte[] key, long index, byte[] value) {
		delegate.lSet(key, index, value);
	}

	@Override
	public void lTrim(byte[] key, long start, long end) {
		delegate.lTrim(key, start, end);
	}

	@Override
	public List<byte[]> mGet(byte[]... keys) {
		return convertAndReturn(delegate.mGet(keys), Converters.identityConverter());
	}

	@Override
	public Boolean mSet(Map<byte[], byte[]> tuple) {
		return convertAndReturn(delegate.mSet(tuple), Converters.identityConverter());
	}

	@Override
	public Boolean mSetNX(Map<byte[], byte[]> tuple) {
		return convertAndReturn(delegate.mSetNX(tuple), Converters.identityConverter());
	}

	@Override
	public void multi() {
		delegate.multi();
	}

	@Override
	public Boolean persist(byte[] key) {
		return convertAndReturn(delegate.persist(key), Converters.identityConverter());
	}

	@Override
	public Boolean move(byte[] key, int dbIndex) {
		return convertAndReturn(delegate.move(key, dbIndex), Converters.identityConverter());
	}

	@Override
	public String ping() {
		return convertAndReturn(delegate.ping(), Converters.identityConverter());
	}

	@Override
	public void pSubscribe(MessageListener listener, byte[]... patterns) {
		delegate.pSubscribe(listener, patterns);
	}

	@Override
	public Long publish(byte[] channel, byte[] message) {
		return convertAndReturn(delegate.publish(channel, message), Converters.identityConverter());
	}

	@Override
	public byte[] randomKey() {
		return convertAndReturn(delegate.randomKey(), Converters.identityConverter());
	}

	@Override
	public void rename(byte[] oldKey, byte[] newKey) {
		delegate.rename(oldKey, newKey);
	}

	@Override
	public Boolean renameNX(byte[] oldKey, byte[] newKey) {
		return convertAndReturn(delegate.renameNX(oldKey, newKey), Converters.identityConverter());
	}

	@Override
	public void resetConfigStats() {
		delegate.resetConfigStats();
	}

	@Override
	public void rewriteConfig() {
		delegate.rewriteConfig();
	}

	@Override
	public byte[] rPop(byte[] key) {
		return convertAndReturn(delegate.rPop(key), Converters.identityConverter());
	}

	@Override
	public List<byte[]> rPop(byte[] key, long count) {
		return convertAndReturn(delegate.rPop(key, count), Converters.identityConverter());
	}

	@Override
	public byte[] rPopLPush(byte[] srcKey, byte[] dstKey) {
		return convertAndReturn(delegate.rPopLPush(srcKey, dstKey), Converters.identityConverter());
	}

	@Override
	public Long rPush(byte[] key, byte[]... values) {
		return convertAndReturn(delegate.rPush(key, values), Converters.identityConverter());
	}

	@Override
	public Long rPushX(byte[] key, byte[] value) {
		return convertAndReturn(delegate.rPushX(key, value), Converters.identityConverter());
	}

	@Override
	public Long sAdd(byte[] key, byte[]... values) {
		return convertAndReturn(delegate.sAdd(key, values), Converters.identityConverter());
	}

	@Override
	public void save() {
		delegate.save();
	}

	@Override
	public Long sCard(byte[] key) {
		return convertAndReturn(delegate.sCard(key), Converters.identityConverter());
	}

	@Override
	public Set<byte[]> sDiff(byte[]... keys) {
		return convertAndReturn(delegate.sDiff(keys), Converters.identityConverter());
	}

	@Override
	public Long sDiffStore(byte[] destKey, byte[]... keys) {
		return convertAndReturn(delegate.sDiffStore(destKey, keys), Converters.identityConverter());
	}

	@Override
	public void select(int dbIndex) {
		delegate.select(dbIndex);
	}

	@Override
	public Boolean set(byte[] key, byte[] value) {
		return convertAndReturn(delegate.set(key, value), Converters.identityConverter());
	}

	@Override
	public Boolean set(byte[] key, byte[] value, Expiration expiration, SetOption option) {
		return convertAndReturn(delegate.set(key, value, expiration, option), Converters.identityConverter());
	}

	@Override
	public byte[] setGet(byte[] key, byte[] value, Expiration expiration, SetOption option) {
		return convertAndReturn(delegate.setGet(key, value, expiration, option), Converters.identityConverter());
	}

	@Override
	public Boolean setBit(byte[] key, long offset, boolean value) {
		return convertAndReturn(delegate.setBit(key, offset, value), Converters.identityConverter());
	}

	@Override
	public void setConfig(String param, String value) {
		delegate.setConfig(param, value);
	}

	@Override
	public Boolean setEx(byte[] key, long seconds, byte[] value) {
		return convertAndReturn(delegate.setEx(key, seconds, value), Converters.identityConverter());
	}

	@Override
	public Boolean pSetEx(byte[] key, long milliseconds, byte[] value) {
		return convertAndReturn(delegate.pSetEx(key, milliseconds, value), Converters.identityConverter());
	}

	@Override
	public Boolean setNX(byte[] key, byte[] value) {
		return convertAndReturn(delegate.setNX(key, value), Converters.identityConverter());
	}

	@Override
	public void setRange(byte[] key, byte[] value, long start) {
		delegate.setRange(key, value, start);
	}

	@Override
	public void shutdown() {
		delegate.shutdown();
	}

	@Override
	public void shutdown(ShutdownOption option) {
		delegate.shutdown(option);
	}

	@Override
	public Set<byte[]> sInter(byte[]... keys) {
		return convertAndReturn(delegate.sInter(keys), Converters.identityConverter());
	}

	@Override
	public Long sInterStore(byte[] destKey, byte[]... keys) {
		return convertAndReturn(delegate.sInterStore(destKey, keys), Converters.identityConverter());
	}

	@Override
	public Long sInterCard(byte[]... keys) {
		return convertAndReturn(delegate.sInterCard(keys), Converters.identityConverter());
	}

	@Override
	public Boolean sIsMember(byte[] key, byte[] value) {
		return convertAndReturn(delegate.sIsMember(key, value), Converters.identityConverter());
	}

	@Override
	public List<Boolean> sMIsMember(byte[] key, byte[]... values) {
		return convertAndReturn(delegate.sMIsMember(key, values), Converters.identityConverter());
	}

	@Override
	public Set<byte[]> sMembers(byte[] key) {
		return convertAndReturn(delegate.sMembers(key), Converters.identityConverter());
	}

	@Override
	public Boolean sMove(byte[] srcKey, byte[] destKey, byte[] value) {
		return convertAndReturn(delegate.sMove(srcKey, destKey, value), Converters.identityConverter());
	}

	@Override
	public Long sort(byte[] key, SortParameters params, byte[] storeKey) {
		return convertAndReturn(delegate.sort(key, params, storeKey), Converters.identityConverter());
	}

	@Override
	public List<byte[]> sort(byte[] key, SortParameters params) {
		return convertAndReturn(delegate.sort(key, params), Converters.identityConverter());
	}

	@Override
	public ValueEncoding encodingOf(byte[] key) {
		return convertAndReturn(delegate.encodingOf(key), Converters.identityConverter());
	}

	@Override
	public Duration idletime(byte[] key) {
		return convertAndReturn(delegate.idletime(key), Converters.identityConverter());
	}

	@Override
	public Long refcount(byte[] key) {
		return convertAndReturn(delegate.refcount(key), Converters.identityConverter());
	}

	@Override
	public byte[] sPop(byte[] key) {
		return convertAndReturn(delegate.sPop(key), Converters.identityConverter());
	}

	@Override
	public List<byte[]> sPop(byte[] key, long count) {
		return convertAndReturn(delegate.sPop(key, count), Converters.identityConverter());
	}

	@Override
	public byte[] sRandMember(byte[] key) {
		return convertAndReturn(delegate.sRandMember(key), Converters.identityConverter());
	}

	@Override
	public List<byte[]> sRandMember(byte[] key, long count) {
		return convertAndReturn(delegate.sRandMember(key, count), Converters.identityConverter());
	}

	@Override
	public Long sRem(byte[] key, byte[]... values) {
		return convertAndReturn(delegate.sRem(key, values), Converters.identityConverter());
	}

	@Override
	public Long strLen(byte[] key) {
		return convertAndReturn(delegate.strLen(key), Converters.identityConverter());
	}

	@Override
	public Long bitCount(byte[] key) {
		return convertAndReturn(delegate.bitCount(key), Converters.identityConverter());
	}

	@Override
	public Long bitCount(byte[] key, long start, long end) {
		return convertAndReturn(delegate.bitCount(key, start, end), Converters.identityConverter());
	}

	@Override
	public Long bitOp(BitOperation op, byte[] destination, byte[]... keys) {
		return convertAndReturn(delegate.bitOp(op, destination, keys), Converters.identityConverter());
	}

	@Override
	public @Nullable Long bitPos(byte[] key, boolean bit, org.springframework.data.domain.Range<Long> range) {
		return convertAndReturn(delegate.bitPos(key, bit, range), Converters.identityConverter());
	}

	@Override
	public void subscribe(MessageListener listener, byte[]... channels) {
		delegate.subscribe(listener, channels);
	}

	@Override
	public Set<byte[]> sUnion(byte[]... keys) {
		return convertAndReturn(delegate.sUnion(keys), Converters.identityConverter());
	}

	@Override
	public Long sUnionStore(byte[] destKey, byte[]... keys) {
		return convertAndReturn(delegate.sUnionStore(destKey, keys), Converters.identityConverter());
	}

	@Override
	public Long ttl(byte[] key) {
		return convertAndReturn(delegate.ttl(key), Converters.identityConverter());
	}

	@Override
	public Long ttl(byte[] key, TimeUnit timeUnit) {
		return convertAndReturn(delegate.ttl(key, timeUnit), Converters.identityConverter());
	}

	@Override
	public DataType type(byte[] key) {
		return convertAndReturn(delegate.type(key), Converters.identityConverter());
	}

	@Override
	public Long touch(byte[]... keys) {
		return convertAndReturn(delegate.touch(keys), Converters.identityConverter());
	}

	@Override
	public void unwatch() {
		delegate.unwatch();
	}

	@Override
	public void watch(byte[]... keys) {
		delegate.watch(keys);
	}

	@Override
	public Boolean zAdd(byte[] key, double score, byte[] value, ZAddArgs args) {
		return convertAndReturn(delegate.zAdd(key, score, value, args), Converters.identityConverter());
	}

	@Override
	public Long zAdd(byte[] key, Set<Tuple> tuples, ZAddArgs args) {
		return convertAndReturn(delegate.zAdd(key, tuples, args), Converters.identityConverter());
	}

	@Override
	public Long zCard(byte[] key) {
		return convertAndReturn(delegate.zCard(key), Converters.identityConverter());
	}

	@Override
	public Long zCount(byte[] key, double min, double max) {
		return convertAndReturn(delegate.zCount(key, min, max), Converters.identityConverter());
	}

	@Override
	public Long zCount(byte[] key, org.springframework.data.domain.Range<? extends Number> range) {
		return convertAndReturn(delegate.zCount(key, range), Converters.identityConverter());
	}

	@Override
	public Double zIncrBy(byte[] key, double increment, byte[] value) {
		return convertAndReturn(delegate.zIncrBy(key, increment, value), Converters.identityConverter());
	}

	@Override
	public @Nullable Set<byte[]> zDiff(byte[]... sets) {
		return convertAndReturn(delegate.zDiff(sets), Converters.identityConverter());
	}

	@Override
	public @Nullable Set<Tuple> zDiffWithScores(byte[]... sets) {
		return convertAndReturn(delegate.zDiffWithScores(sets), Converters.identityConverter());
	}

	@Override
	public @Nullable Long zDiffStore(byte[] destKey, byte[]... sets) {
		return convertAndReturn(delegate.zDiffStore(destKey, sets), Converters.identityConverter());
	}

	@Override
	public @Nullable Set<String> zDiff(String... sets) {
		return convertAndReturn(delegate.zDiff(serializeMulti(sets)), byteSetToStringSet);
	}

	@Override
	public @Nullable Set<StringTuple> zDiffWithScores(String... sets) {
		return convertAndReturn(delegate.zDiffWithScores(serializeMulti(sets)), tupleToStringTuple);
	}

	@Override
	public @Nullable Long zDiffStore(String destKey, String... sets) {
		return convertAndReturn(delegate.zDiffStore(serialize(destKey), serializeMulti(sets)),
				Converters.identityConverter());
	}

	@Override
	public @Nullable Set<byte[]> zInter(byte[]... sets) {
		return convertAndReturn(delegate.zInter(sets), Converters.identityConverter());
	}

	@Override
	public @Nullable Set<Tuple> zInterWithScores(byte[]... sets) {
		return convertAndReturn(delegate.zInterWithScores(sets), Converters.identityConverter());
	}

	@Override
	public @Nullable Set<Tuple> zInterWithScores(Aggregate aggregate, Weights weights, byte[]... sets) {
		return convertAndReturn(delegate.zInterWithScores(aggregate, weights, sets), Converters.identityConverter());
	}

	@Override
	public @Nullable Set<String> zInter(String... sets) {
		return convertAndReturn(delegate.zInter(serializeMulti(sets)), byteSetToStringSet);
	}

	@Override
	public @Nullable Set<StringTuple> zInterWithScores(String... sets) {
		return convertAndReturn(delegate.zInterWithScores(serializeMulti(sets)), tupleToStringTuple);
	}

	@Override
	public @Nullable Set<StringTuple> zInterWithScores(Aggregate aggregate, Weights weights, String... sets) {
		return convertAndReturn(delegate.zInterWithScores(aggregate, weights, serializeMulti(sets)), tupleToStringTuple);
	}

	@Override
	public @Nullable Long zInterStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
		return convertAndReturn(delegate.zInterStore(destKey, aggregate, Weights.of(weights), sets),
				Converters.identityConverter());
	}

	@Override
	public Long zInterStore(byte[] destKey, Aggregate aggregate, Weights weights, byte[]... sets) {
		return convertAndReturn(delegate.zInterStore(destKey, aggregate, weights, sets), Converters.identityConverter());
	}

	@Override
	public Long zInterStore(byte[] destKey, byte[]... sets) {
		return convertAndReturn(delegate.zInterStore(destKey, sets), Converters.identityConverter());
	}

	@Override
	public Set<byte[]> zRange(byte[] key, long start, long end) {
		return convertAndReturn(delegate.zRange(key, start, end), Converters.identityConverter());
	}

	@Override
	public Set<byte[]> zRangeByScore(byte[] key, double min, double max, long offset, long count) {
		return convertAndReturn(delegate.zRangeByScore(key, min, max, offset, count), Converters.identityConverter());
	}

	@Override
	public Set<byte[]> zRangeByScore(byte[] key, org.springframework.data.domain.Range<? extends Number> range) {
		return convertAndReturn(delegate.zRangeByScore(key, range), Converters.identityConverter());
	}

	@Override
	public Set<byte[]> zRangeByScore(byte[] key, org.springframework.data.domain.Range<? extends Number> range,
			org.springframework.data.redis.connection.Limit limit) {
		return convertAndReturn(delegate.zRangeByScore(key, range, limit), Converters.identityConverter());
	}

	@Override
	public Set<Tuple> zRangeByScoreWithScores(byte[] key, org.springframework.data.domain.Range<? extends Number> range) {
		return convertAndReturn(delegate.zRangeByScoreWithScores(key, range), Converters.identityConverter());
	}

	@Override
	public Set<byte[]> zRangeByScore(byte[] key, double min, double max) {
		return convertAndReturn(delegate.zRangeByScore(key, min, max), Converters.identityConverter());
	}

	@Override
	public Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max, long offset, long count) {
		return convertAndReturn(delegate.zRangeByScoreWithScores(key, min, max, offset, count),
				Converters.identityConverter());
	}

	@Override
	public Set<Tuple> zRangeByScoreWithScores(byte[] key, org.springframework.data.domain.Range<? extends Number> range,
			org.springframework.data.redis.connection.Limit limit) {
		return convertAndReturn(delegate.zRangeByScoreWithScores(key, range, limit), Converters.identityConverter());
	}

	@Override
	public Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max) {
		return convertAndReturn(delegate.zRangeByScoreWithScores(key, min, max), Converters.identityConverter());
	}

	@Override
	public Set<Tuple> zRangeWithScores(byte[] key, long start, long end) {
		return convertAndReturn(delegate.zRangeWithScores(key, start, end), Converters.identityConverter());
	}

	@Override
	public Set<byte[]> zRevRangeByScore(byte[] key, double min, double max, long offset, long count) {
		return convertAndReturn(delegate.zRevRangeByScore(key, min, max, offset, count), Converters.identityConverter());
	}

	@Override
	public Set<byte[]> zRevRangeByScore(byte[] key, org.springframework.data.domain.Range<? extends Number> range) {
		return convertAndReturn(delegate.zRevRangeByScore(key, range), Converters.identityConverter());
	}

	@Override
	public Set<byte[]> zRevRangeByScore(byte[] key, double min, double max) {
		return convertAndReturn(delegate.zRevRangeByScore(key, min, max), Converters.identityConverter());
	}

	@Override
	public Set<byte[]> zRevRangeByScore(byte[] key, org.springframework.data.domain.Range<? extends Number> range,
			org.springframework.data.redis.connection.Limit limit) {
		return convertAndReturn(delegate.zRevRangeByScore(key, range, limit), Converters.identityConverter());
	}

	@Override
	public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max, long offset, long count) {
		return convertAndReturn(delegate.zRevRangeByScoreWithScores(key, min, max, offset, count),
				Converters.identityConverter());
	}

	@Override
	public Set<Tuple> zRevRangeByScoreWithScores(byte[] key,
			org.springframework.data.domain.Range<? extends Number> range) {
		return convertAndReturn(delegate.zRevRangeByScoreWithScores(key, range), Converters.identityConverter());
	}

	@Override
	public Set<Tuple> zRevRangeByScoreWithScores(byte[] key,
			org.springframework.data.domain.Range<? extends Number> range,
			org.springframework.data.redis.connection.Limit limit) {
		return convertAndReturn(delegate.zRevRangeByScoreWithScores(key, range, limit), Converters.identityConverter());
	}

	@Override
	public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max) {
		return convertAndReturn(delegate.zRevRangeByScoreWithScores(key, min, max), Converters.identityConverter());
	}

	@Override
	public Long zRank(byte[] key, byte[] value) {
		return convertAndReturn(delegate.zRank(key, value), Converters.identityConverter());
	}

	@Override
	public Long zRem(byte[] key, byte[]... values) {
		return convertAndReturn(delegate.zRem(key, values), Converters.identityConverter());
	}

	@Override
	public Long zRemRange(byte[] key, long start, long end) {
		return convertAndReturn(delegate.zRemRange(key, start, end), Converters.identityConverter());
	}

	@Override
	public Long zRemRangeByLex(byte[] key, org.springframework.data.domain.Range<byte[]> range) {
		return convertAndReturn(delegate.zRemRangeByLex(key, range), Converters.identityConverter());
	}

	@Override
	public Long zRemRangeByScore(byte[] key, double min, double max) {
		return convertAndReturn(delegate.zRemRangeByScore(key, min, max), Converters.identityConverter());
	}

	@Override
	public Long zRemRangeByScore(byte[] key, org.springframework.data.domain.Range<? extends Number> range) {
		return convertAndReturn(delegate.zRemRangeByScore(key, range), Converters.identityConverter());
	}

	@Override
	public Set<byte[]> zRevRange(byte[] key, long start, long end) {
		return convertAndReturn(delegate.zRevRange(key, start, end), Converters.identityConverter());
	}

	@Override
	public Set<Tuple> zRevRangeWithScores(byte[] key, long start, long end) {
		return convertAndReturn(delegate.zRevRangeWithScores(key, start, end), Converters.identityConverter());
	}

	@Override
	public Long zRevRank(byte[] key, byte[] value) {
		return convertAndReturn(delegate.zRevRank(key, value), Converters.identityConverter());
	}

	@Override
	public Double zScore(byte[] key, byte[] value) {
		return convertAndReturn(delegate.zScore(key, value), Converters.identityConverter());
	}

	@Override
	public List<Double> zMScore(byte[] key, byte[]... values) {
		return convertAndReturn(delegate.zMScore(key, values), Converters.identityConverter());
	}

	@Override
	public @Nullable Set<byte[]> zUnion(byte[]... sets) {
		return convertAndReturn(delegate.zUnion(sets), Converters.identityConverter());
	}

	@Override
	public @Nullable Set<Tuple> zUnionWithScores(byte[]... sets) {
		return convertAndReturn(delegate.zUnionWithScores(sets), Converters.identityConverter());
	}

	@Override
	public @Nullable Set<Tuple> zUnionWithScores(Aggregate aggregate, Weights weights, byte[]... sets) {
		return convertAndReturn(delegate.zUnionWithScores(aggregate, weights, sets), Converters.identityConverter());
	}

	@Override
	public @Nullable Set<String> zUnion(String... sets) {
		return convertAndReturn(delegate.zUnion(serializeMulti(sets)), byteSetToStringSet);
	}

	@Override
	public @Nullable Set<StringTuple> zUnionWithScores(String... sets) {
		return convertAndReturn(delegate.zUnionWithScores(serializeMulti(sets)), tupleToStringTuple);
	}

	@Override
	public @Nullable Set<StringTuple> zUnionWithScores(Aggregate aggregate, Weights weights, String... sets) {
		return convertAndReturn(delegate.zUnionWithScores(aggregate, weights, serializeMulti(sets)), tupleToStringTuple);
	}

	@Override
	public Long zUnionStore(byte[] destKey, Aggregate aggregate, Weights weights, byte[]... sets) {
		return convertAndReturn(delegate.zUnionStore(destKey, aggregate, weights, sets), Converters.identityConverter());
	}

	@Override
	public @Nullable Long zUnionStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
		return convertAndReturn(delegate.zUnionStore(destKey, aggregate, Weights.of(weights), sets),
				Converters.identityConverter());
	}

	public Long zUnionStore(byte[] destKey, byte[]... sets) {
		return convertAndReturn(delegate.zUnionStore(destKey, sets), Converters.identityConverter());
	}

	@Override
	public Long zLexCount(String key, org.springframework.data.domain.Range<String> range) {
		return delegate.zLexCount(serialize(key), serialize(range));
	}

	@Override
	public Boolean pExpire(byte[] key, long millis, ExpirationOptions.Condition condition) {
		return convertAndReturn(delegate.pExpire(key, millis, condition), Converters.identityConverter());
	}

	@Override
	public Boolean pExpireAt(byte[] key, long unixTimeInMillis, ExpirationOptions.Condition condition) {
		return convertAndReturn(delegate.pExpireAt(key, unixTimeInMillis, condition), Converters.identityConverter());
	}

	@Override
	public Long pTtl(byte[] key) {
		return convertAndReturn(delegate.pTtl(key), Converters.identityConverter());
	}

	@Override
	public Long pTtl(byte[] key, TimeUnit timeUnit) {
		return convertAndReturn(delegate.pTtl(key, timeUnit), Converters.identityConverter());
	}

	@Override
	public byte[] dump(byte[] key) {
		return convertAndReturn(delegate.dump(key), Converters.identityConverter());
	}

	@Override
	public void restore(byte[] key, long ttlInMillis, byte[] serializedValue, boolean replace) {
		delegate.restore(key, ttlInMillis, serializedValue, replace);
	}

	@Override
	public void scriptFlush() {
		delegate.scriptFlush();
	}

	@Override
	public void scriptKill() {
		delegate.scriptKill();
	}

	@Override
	public String scriptLoad(byte[] script) {
		return convertAndReturn(delegate.scriptLoad(script), Converters.identityConverter());
	}

	@Override
	public List<Boolean> scriptExists(String... scriptSha1) {
		return convertAndReturn(delegate.scriptExists(scriptSha1), Converters.identityConverter());
	}

	@Override
	public <T> T eval(byte[] script, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
		return convertAndReturn(delegate.eval(script, returnType, numKeys, keysAndArgs), Converters.identityConverter());
	}

	@Override
	public <T> T evalSha(String scriptSha1, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
		return convertAndReturn(delegate.evalSha(scriptSha1, returnType, numKeys, keysAndArgs),
				Converters.identityConverter());
	}

	@Override
	public <T> T evalSha(byte[] scriptSha1, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
		return convertAndReturn(delegate.evalSha(scriptSha1, returnType, numKeys, keysAndArgs),
				Converters.identityConverter());
	}

	//
	// String methods
	//

	private byte[] serialize(String data) {
		return serializer.serialize(data);
	}

	private org.springframework.data.domain.Range<byte[]> serialize(org.springframework.data.domain.Range<String> range) {

		if (!range.getLowerBound().isBounded() && !range.getUpperBound().isBounded()) {
			return org.springframework.data.domain.Range.unbounded();
		}

		org.springframework.data.domain.Range.Bound<byte[]> lower = rawBound(range.getLowerBound());
		org.springframework.data.domain.Range.Bound<byte[]> upper = rawBound(range.getUpperBound());

		return org.springframework.data.domain.Range.of(lower, upper);
	}

	private org.springframework.data.domain.Range.Bound<byte[]> rawBound(
			org.springframework.data.domain.Range.Bound<String> source) {
		return source.getValue().map(this::serialize)
				.map(it -> source.isInclusive() ? org.springframework.data.domain.Range.Bound.inclusive(it)
						: org.springframework.data.domain.Range.Bound.exclusive(it))
				.orElseGet(org.springframework.data.domain.Range.Bound::unbounded);
	}

	@SuppressWarnings("unchecked")
	private GeoReference<byte[]> serialize(GeoReference<String> data) {
		return data instanceof GeoReference.GeoMemberReference
				? GeoReference
						.fromMember(serializer.serialize(((GeoMemberReference<String>) data).getMember()))
				: (GeoReference) data;
	}

	private StreamOffset<byte[]>[] serialize(StreamOffset<String>[] offsets) {

		return Arrays.stream(offsets).map(it -> StreamOffset.create(serialize(it.getKey()), it.getOffset()))
				.toArray((IntFunction<StreamOffset<byte[]>[]>) StreamOffset[]::new);
	}

	private byte[][] serializeMulti(String... keys) {

		if (keys == null) {
			return EMPTY_2D_BYTE_ARRAY;
		}

		byte[][] ret = new byte[keys.length][];

		for (int i = 0; i < ret.length; i++) {
			ret[i] = serializer.serialize(keys[i]);
		}

		return ret;
	}

	private Map<byte[], byte[]> serialize(Map<String, String> hashes) {
		Map<byte[], byte[]> ret = new LinkedHashMap<>(hashes.size());

		for (Map.Entry<String, String> entry : hashes.entrySet()) {
			ret.put(serializer.serialize(entry.getKey()), serializer.serialize(entry.getValue()));
		}

		return ret;
	}

	@Override
	public Long append(String key, String value) {
		return append(serialize(key), serialize(value));
	}

	@Override
	public List<String> bLPop(int timeout, String... keys) {
		return convertAndReturn(delegate.bLPop(timeout, serializeMulti(keys)), byteListToStringList);
	}

	@Override
	public List<String> bRPop(int timeout, String... keys) {
		return convertAndReturn(delegate.bRPop(timeout, serializeMulti(keys)), byteListToStringList);
	}

	@Override
	public String bRPopLPush(int timeout, String srcKey, String dstKey) {
		return convertAndReturn(delegate.bRPopLPush(timeout, serialize(srcKey), serialize(dstKey)), bytesToString);
	}

	@Override
	public Boolean copy(String sourceKey, String targetKey, boolean replace) {
		return copy(serialize(sourceKey), serialize(targetKey), replace);
	}

	@Override
	public Long decr(String key) {
		return decr(serialize(key));
	}

	@Override
	public Long decrBy(String key, long value) {
		return decrBy(serialize(key), value);
	}

	@Override
	public Long del(String... keys) {
		return del(serializeMulti(keys));
	}

	@Override
	public Long unlink(String... keys) {
		return unlink(serializeMulti(keys));
	}

	@Override
	public String echo(String message) {
		return convertAndReturn(delegate.echo(serialize(message)), bytesToString);
	}

	@Override
	public Boolean exists(String key) {
		return exists(serialize(key));
	}

	@Override
	public Boolean expire(String key, long seconds, ExpirationOptions.Condition condition) {
		return expire(serialize(key), seconds, condition);
	}

	@Override
	public Boolean expireAt(String key, long unixTime, ExpirationOptions.Condition condition) {
		return expireAt(serialize(key), unixTime, condition);
	}

	@Override
	public String get(String key) {
		return convertAndReturn(delegate.get(serialize(key)), bytesToString);
	}

	@Override
	public Boolean getBit(String key, long offset) {
		return getBit(serialize(key), offset);
	}

	@Override
	public String getRange(String key, long start, long end) {
		return convertAndReturn(delegate.getRange(serialize(key), start, end), bytesToString);
	}

	@Override
	public String getSet(String key, String value) {
		return convertAndReturn(delegate.getSet(serialize(key), serialize(value)), bytesToString);
	}

	@Override
	public Long hDel(String key, String... fields) {
		return hDel(serialize(key), serializeMulti(fields));
	}

	@Override
	public Boolean hExists(String key, String field) {
		return hExists(serialize(key), serialize(field));
	}

	@Override
	public String hGet(String key, String field) {
		return convertAndReturn(delegate.hGet(serialize(key), serialize(field)), bytesToString);
	}

	@Override
	public Map<String, String> hGetAll(String key) {
		return convertAndReturn(delegate.hGetAll(serialize(key)), byteMapToStringMap);
	}

	@Override
	public Long hIncrBy(String key, String field, long delta) {
		return hIncrBy(serialize(key), serialize(field), delta);
	}

	@Override
	public Double hIncrBy(String key, String field, double delta) {
		return hIncrBy(serialize(key), serialize(field), delta);
	}


	@Override
	public byte @Nullable[] hRandField(byte[] key) {
		return convertAndReturn(delegate.hRandField(key), Converters.identityConverter());
	}

	@Override
	public @Nullable Entry<byte[], byte[]> hRandFieldWithValues(byte[] key) {
		return convertAndReturn(delegate.hRandFieldWithValues(key), Converters.identityConverter());
	}

	@Override
	public @Nullable List<byte[]> hRandField(byte[] key, long count) {
		return convertAndReturn(delegate.hRandField(key, count), Converters.identityConverter());
	}

	@Override
	public @Nullable List<Entry<byte[], byte[]>> hRandFieldWithValues(byte[] key, long count) {
		return convertAndReturn(delegate.hRandFieldWithValues(key, count), Converters.identityConverter());
	}

	@Override
	public @Nullable String hRandField(String key) {
		return convertAndReturn(delegate.hRandField(serialize(key)), bytesToString);
	}

	@Override
	public @Nullable Entry<String, String> hRandFieldWithValues(String key) {
		return convertAndReturn(delegate.hRandFieldWithValues(serialize(key)),
				(Converter<Entry<byte[], byte[]>, Entry<String, String>>) this::convertEntry);
	}

	@Override
	public @Nullable List<String> hRandField(String key, long count) {
		return convertAndReturn(delegate.hRandField(serialize(key), count), byteListToStringList);
	}

	@Override
	public @Nullable List<Entry<String, String>> hRandFieldWithValues(String key, long count) {
		return convertAndReturn(delegate.hRandFieldWithValues(serialize(key), count),
				new ListConverter<>(this::convertEntry));
	}

	@Override
	public Set<String> hKeys(String key) {
		return convertAndReturn(delegate.hKeys(serialize(key)), byteSetToStringSet);
	}

	@Override
	public Long hLen(String key) {
		return hLen(serialize(key));
	}

	@Override
	public List<String> hMGet(String key, String... fields) {
		return convertAndReturn(delegate.hMGet(serialize(key), serializeMulti(fields)), byteListToStringList);
	}

	@Override
	public void hMSet(String key, Map<String, String> hashes) {
		delegate.hMSet(serialize(key), serialize(hashes));
	}

	@Override
	public Boolean hSet(String key, String field, String value) {
		return hSet(serialize(key), serialize(field), serialize(value));
	}

	@Override
	public Boolean hSetNX(String key, String field, String value) {
		return hSetNX(serialize(key), serialize(field), serialize(value));
	}

	@Override
	public List<String> hVals(String key) {
		return convertAndReturn(delegate.hVals(serialize(key)), byteListToStringList);
	}

    @Override
    public List<String> hGetDel(String key, String... fields) {
        return convertAndReturn(delegate.hGetDel(serialize(key), serializeMulti(fields)), byteListToStringList);
    }

    @Override
    public List<String> hGetEx(String key, Expiration expiration, String... fields) {
        return convertAndReturn(delegate.hGetEx(serialize(key), expiration, serializeMulti(fields)), byteListToStringList);
    }

    @Override
    public Boolean hSetEx(@NonNull String key, @NonNull Map<@NonNull String, String> hashes, HashFieldSetOption condition, Expiration expiration) {
        return convertAndReturn(delegate.hSetEx(serialize(key), serialize(hashes), condition, expiration), Converters.identityConverter());
    }

	@Override
	public Long incr(String key) {
		return incr(serialize(key));
	}

	@Override
	public Long incrBy(String key, long value) {
		return incrBy(serialize(key), value);
	}

	@Override
	public Double incrBy(String key, double value) {
		return incrBy(serialize(key), value);
	}

	@Override
	public Collection<String> keys(String pattern) {
		return convertAndReturn(delegate.keys(serialize(pattern)), byteSetToStringSet);
	}

	@Override
	public String lIndex(String key, long index) {
		return convertAndReturn(delegate.lIndex(serialize(key), index), bytesToString);
	}

	@Override
	public Long lInsert(String key, Position where, String pivot, String value) {
		return lInsert(serialize(key), where, serialize(pivot), serialize(value));
	}

	@Override
	public Long lLen(String key) {
		return lLen(serialize(key));
	}

	@Override
	public String lPop(String key) {
		return convertAndReturn(delegate.lPop(serialize(key)), bytesToString);
	}

	@Override
	public List<String> lPop(String key, long count) {
		return convertAndReturn(delegate.lPop(serialize(key), count), byteListToStringList);
	}

	@Override
	public List<Long> lPos(String key, String element, @Nullable Integer rank, @Nullable Integer count) {
		return lPos(serialize(key), serialize(element), rank, count);
	}

	@Override
	public Long lPush(String key, String... values) {
		return lPush(serialize(key), serializeMulti(values));
	}

	@Override
	public Long lPushX(String key, String value) {
		return lPushX(serialize(key), serialize(value));
	}

	@Override
	public List<String> lRange(String key, long start, long end) {
		return convertAndReturn(delegate.lRange(serialize(key), start, end), byteListToStringList);
	}

	@Override
	public Long lRem(String key, long count, String value) {
		return lRem(serialize(key), count, serialize(value));
	}

	@Override
	public void lSet(String key, long index, String value) {
		delegate.lSet(serialize(key), index, serialize(value));
	}

	@Override
	public void lTrim(String key, long start, long end) {
		delegate.lTrim(serialize(key), start, end);
	}

	@Override
	public List<String> mGet(String... keys) {
		return convertAndReturn(delegate.mGet(serializeMulti(keys)), byteListToStringList);
	}

	@Override
	public Boolean mSetNXString(Map<String, String> tuple) {
		return mSetNX(serialize(tuple));
	}

	@Override
	public Boolean mSetString(Map<String, String> tuple) {
		return mSet(serialize(tuple));
	}

	@Override
	public Boolean persist(String key) {
		return persist(serialize(key));
	}

	@Override
	public Boolean move(String key, int dbIndex) {
		return move(serialize(key), dbIndex);
	}

	@Override
	public void pSubscribe(MessageListener listener, String... patterns) {
		delegate.pSubscribe(listener, serializeMulti(patterns));
	}

	@Override
	public Long publish(String channel, String message) {
		return publish(serialize(channel), serialize(message));
	}

	@Override
	public void rename(String oldKey, String newKey) {
		delegate.rename(serialize(oldKey), serialize(newKey));
	}

	@Override
	public Boolean renameNX(String oldKey, String newKey) {
		return renameNX(serialize(oldKey), serialize(newKey));
	}

	@Override
	public String rPop(String key) {
		return convertAndReturn(delegate.rPop(serialize(key)), bytesToString);
	}

	@Override
	public List<String> rPop(String key, long count) {
		return convertAndReturn(delegate.rPop(serialize(key), count), byteListToStringList);
	}

	@Override
	public String rPopLPush(String srcKey, String dstKey) {
		return convertAndReturn(delegate.rPopLPush(serialize(srcKey), serialize(dstKey)), bytesToString);
	}

	@Override
	public Long rPush(String key, String... values) {
		return rPush(serialize(key), serializeMulti(values));
	}

	@Override
	public Long rPushX(String key, String value) {
		return rPushX(serialize(key), serialize(value));
	}

	@Override
	public Long sAdd(String key, String... values) {
		return sAdd(serialize(key), serializeMulti(values));
	}

	@Override
	public Long sCard(String key) {
		return sCard(serialize(key));
	}

	@Override
	public Set<String> sDiff(String... keys) {
		return convertAndReturn(delegate.sDiff(serializeMulti(keys)), byteSetToStringSet);
	}

	@Override
	public Long sDiffStore(String destKey, String... keys) {
		return sDiffStore(serialize(destKey), serializeMulti(keys));
	}

	@Override
	public Boolean set(String key, String value) {
		return set(serialize(key), serialize(value));
	}

	@Override
	public Boolean set(String key, String value, Expiration expiration, SetOption option) {
		return set(serialize(key), serialize(value), expiration, option);
	}

	@Override
	public Boolean setBit(String key, long offset, boolean value) {
		return setBit(serialize(key), offset, value);
	}

	@Override
	public Boolean setEx(String key, long seconds, String value) {
		return setEx(serialize(key), seconds, serialize(value));
	}

	@Override
	public Boolean pSetEx(String key, long seconds, String value) {
		return pSetEx(serialize(key), seconds, serialize(value));
	}

	@Override
	public Boolean setNX(String key, String value) {
		return setNX(serialize(key), serialize(value));
	}

	@Override
	public void setRange(String key, String value, long start) {
		delegate.setRange(serialize(key), serialize(value), start);
	}

	@Override
	public Set<String> sInter(String... keys) {
		return convertAndReturn(delegate.sInter(serializeMulti(keys)), byteSetToStringSet);
	}

	@Override
	public Long sInterStore(String destKey, String... keys) {
		return sInterStore(serialize(destKey), serializeMulti(keys));
	}

	@Override
	public Long sInterCard(String... keys) {
		return sInterCard(serializeMulti(keys));
	}

	@Override
	public Boolean sIsMember(String key, String value) {
		return sIsMember(serialize(key), serialize(value));
	}

	@Override
	public List<Boolean> sMIsMember(String key, String... values) {
		return sMIsMember(serialize(key), serializeMulti(values));
	}

	@Override
	public Set<String> sMembers(String key) {
		return convertAndReturn(delegate.sMembers(serialize(key)), byteSetToStringSet);
	}

	@Override
	public Boolean sMove(String srcKey, String destKey, String value) {
		return sMove(serialize(srcKey), serialize(destKey), serialize(value));
	}

	@Override
	public Long sort(String key, SortParameters params, String storeKey) {
		return sort(serialize(key), params, serialize(storeKey));
	}

	@Override
	public List<String> sort(String key, SortParameters params) {
		return convertAndReturn(delegate.sort(serialize(key), params), byteListToStringList);
	}

	@Override
	public ValueEncoding encodingOf(String key) {
		return encodingOf(serialize(key));
	}

	@Override
	public Duration idletime(String key) {
		return idletime(serialize(key));
	}

	@Override
	public Long refcount(String key) {
		return refcount(serialize(key));
	}

	@Override
	public String sPop(String key) {
		return convertAndReturn(delegate.sPop(serialize(key)), bytesToString);
	}

	@Override
	public List<String> sPop(String key, long count) {
		return convertAndReturn(delegate.sPop(serialize(key), count), byteListToStringList);
	}

	@Override
	public String sRandMember(String key) {
		return convertAndReturn(delegate.sRandMember(serialize(key)), bytesToString);
	}

	@Override
	public List<String> sRandMember(String key, long count) {
		return convertAndReturn(delegate.sRandMember(serialize(key), count), byteListToStringList);
	}

	@Override
	public Long sRem(String key, String... values) {
		return sRem(serialize(key), serializeMulti(values));
	}

	@Override
	public Long strLen(String key) {
		return strLen(serialize(key));
	}

	@Override
	public Long bitCount(String key) {
		return bitCount(serialize(key));
	}

	@Override
	public Long bitCount(String key, long start, long end) {
		return bitCount(serialize(key), start, end);
	}

	@Override
	public Long bitOp(BitOperation op, String destination, String... keys) {
		return bitOp(op, serialize(destination), serializeMulti(keys));
	}

	@Override
	public @Nullable Long bitPos(String key, boolean bit, org.springframework.data.domain.Range<Long> range) {
		return bitPos(serialize(key), bit, range);
	}

	@Override
	public void subscribe(MessageListener listener, String... channels) {
		delegate.subscribe(listener, serializeMulti(channels));
	}

	@Override
	public Set<String> sUnion(String... keys) {
		return convertAndReturn(delegate.sUnion(serializeMulti(keys)), byteSetToStringSet);
	}

	@Override
	public Long sUnionStore(String destKey, String... keys) {
		return sUnionStore(serialize(destKey), serializeMulti(keys));
	}

	@Override
	public Long ttl(String key) {
		return ttl(serialize(key));
	}

	@Override
	public Long ttl(String key, TimeUnit timeUnit) {
		return ttl(serialize(key), timeUnit);
	}

	@Override
	public DataType type(String key) {
		return type(serialize(key));
	}

	@Override
	public @Nullable Long touch(String... keys) {
		return touch(serializeMulti(keys));
	}

	@Override
	public Boolean zAdd(String key, double score, String value) {
		return zAdd(serialize(key), score, serialize(value));
	}

	@Override
	public Boolean zAdd(String key, double score, String value, ZAddArgs args) {
		return zAdd(serialize(key), score, serialize(value), args);
	}

	@Override
	public Long zAdd(String key, Set<StringTuple> tuples) {
		return zAdd(serialize(key), stringTupleToTuple.convert(tuples));
	}

	@Override
	public Long zAdd(String key, Set<StringTuple> tuples, ZAddArgs args) {
		return zAdd(serialize(key), stringTupleToTuple.convert(tuples), args);
	}

	@Override
	public Long zCard(String key) {
		return zCard(serialize(key));
	}

	@Override
	public Long zCount(String key, double min, double max) {
		return zCount(serialize(key), min, max);
	}

	@Override
	public Long zLexCount(byte[] key, org.springframework.data.domain.Range<byte[]> range) {
		return delegate.zLexCount(key, range);
	}

	@Override
	public @Nullable Tuple zPopMin(byte[] key) {
		return delegate.zPopMin(key);
	}

	@Override
	public @Nullable StringTuple zPopMin(String key) {
		return convertAndReturn(delegate.zPopMin(serialize(key)), tupleConverter);
	}

	@Override
	public @Nullable Set<Tuple> zPopMin(byte[] key, long count) {
		return delegate.zPopMin(key, count);
	}

	@Override
	public @Nullable Set<StringTuple> zPopMin(String key, long count) {
		return convertAndReturn(delegate.zPopMin(serialize(key), count), tupleToStringTuple);
	}

	@Override
	public @Nullable Tuple bZPopMin(byte[] key, long timeout, TimeUnit unit) {
		return delegate.bZPopMin(key, timeout, unit);
	}

	@Override
	public @Nullable StringTuple bZPopMin(String key, long timeout, TimeUnit unit) {
		return convertAndReturn(delegate.bZPopMin(serialize(key), timeout, unit), tupleConverter);
	}

	@Override
	public @Nullable Tuple zPopMax(byte[] key) {
		return delegate.zPopMax(key);
	}

	@Override
	public @Nullable StringTuple zPopMax(String key) {
		return convertAndReturn(delegate.zPopMax(serialize(key)), tupleConverter);
	}

	@Override
	public @Nullable Set<Tuple> zPopMax(byte[] key, long count) {
		return delegate.zPopMax(key, count);
	}

	@Override
	public @Nullable Set<StringTuple> zPopMax(String key, long count) {
		return convertAndReturn(delegate.zPopMax(serialize(key), count), tupleToStringTuple);
	}

	@Override
	public @Nullable Tuple bZPopMax(byte[] key, long timeout, TimeUnit unit) {
		return delegate.bZPopMax(key, timeout, unit);
	}

	@Override
	public @Nullable StringTuple bZPopMax(String key, long timeout, TimeUnit unit) {
		return convertAndReturn(delegate.bZPopMax(serialize(key), timeout, unit), tupleConverter);
	}

	@Override
	public Double zIncrBy(String key, double increment, String value) {
		return zIncrBy(serialize(key), increment, serialize(value));
	}

	@Override
	public Long zInterStore(String destKey, Aggregate aggregate, int[] weights, String... sets) {
		return zInterStore(serialize(destKey), aggregate, weights, serializeMulti(sets));
	}

	@Override
	public Long zInterStore(String destKey, String... sets) {
		return zInterStore(serialize(destKey), serializeMulti(sets));
	}

	@Override
	public byte[] zRandMember(byte[] key) {
		return delegate.zRandMember(key);
	}

	@Override
	public List<byte[]> zRandMember(byte[] key, long count) {
		return delegate.zRandMember(key, count);
	}

	@Override
	public Tuple zRandMemberWithScore(byte[] key) {
		return delegate.zRandMemberWithScore(key);
	}

	@Override
	public List<Tuple> zRandMemberWithScore(byte[] key, long count) {
		return delegate.zRandMemberWithScore(key, count);
	}

	@Override
	public String zRandMember(String key) {
		return convertAndReturn(delegate.zRandMember(serialize(key)), bytesToString);
	}

	@Override
	public List<String> zRandMember(String key, long count) {
		return convertAndReturn(delegate.zRandMember(serialize(key), count), byteListToStringList);
	}

	@Override
	public StringTuple zRandMemberWithScore(String key) {
		return convertAndReturn(delegate.zRandMemberWithScore(serialize(key)), new TupleConverter());
	}

	@Override
	public List<StringTuple> zRandMemberWithScores(String key, long count) {
		return convertAndReturn(delegate.zRandMemberWithScore(serialize(key), count), tupleListToStringTuple);
	}

	@Override
	public Set<String> zRange(String key, long start, long end) {
		return convertAndReturn(delegate.zRange(serialize(key), start, end), byteSetToStringSet);
	}

	@Override
	public Set<String> zRangeByScore(String key, double min, double max, long offset, long count) {
		return convertAndReturn(delegate.zRangeByScore(serialize(key), min, max, offset, count), byteSetToStringSet);
	}

	@Override
	public Set<String> zRangeByScore(String key, double min, double max) {
		return convertAndReturn(delegate.zRangeByScore(serialize(key), min, max), byteSetToStringSet);
	}

	@Override
	public Set<StringTuple> zRangeByScoreWithScores(String key, double min, double max, long offset, long count) {
		return convertAndReturn(delegate.zRangeByScoreWithScores(serialize(key), min, max, offset, count),
				tupleToStringTuple);
	}

	@Override
	public Set<StringTuple> zRangeByScoreWithScores(String key, double min, double max) {
		return convertAndReturn(delegate.zRangeByScoreWithScores(serialize(key), min, max), tupleToStringTuple);
	}

	@Override
	public Set<StringTuple> zRangeWithScores(String key, long start, long end) {
		return convertAndReturn(delegate.zRangeWithScores(serialize(key), start, end), tupleToStringTuple);
	}

	@Override
	public Long zRank(String key, String value) {
		return zRank(serialize(key), serialize(value));
	}

	@Override
	public Long zRem(String key, String... values) {
		return zRem(serialize(key), serializeMulti(values));
	}

	@Override
	public Long zRemRange(String key, long start, long end) {
		return zRemRange(serialize(key), start, end);
	}

	@Override
	public Long zRemRangeByLex(String key, org.springframework.data.domain.Range<String> range) {
		return zRemRangeByLex(serialize(key), serialize(range));
	}

	@Override
	public Long zRemRangeByScore(String key, double min, double max) {
		return zRemRangeByScore(serialize(key), min, max);
	}

	@Override
	public Set<String> zRevRange(String key, long start, long end) {
		return convertAndReturn(delegate.zRevRange(serialize(key), start, end), byteSetToStringSet);
	}

	@Override
	public Set<StringTuple> zRevRangeWithScores(String key, long start, long end) {
		return convertAndReturn(delegate.zRevRangeWithScores(serialize(key), start, end), tupleToStringTuple);
	}

	@Override
	public Set<String> zRevRangeByScore(String key, double min, double max) {
		return convertAndReturn(delegate.zRevRangeByScore(serialize(key), min, max), byteSetToStringSet);
	}

	@Override
	public Set<StringTuple> zRevRangeByScoreWithScores(String key, double min, double max) {
		return convertAndReturn(delegate.zRevRangeByScoreWithScores(serialize(key), min, max), tupleToStringTuple);
	}

	@Override
	public Set<String> zRevRangeByScore(String key, double min, double max, long offset, long count) {
		return convertAndReturn(delegate.zRevRangeByScore(serialize(key), min, max, offset, count), byteSetToStringSet);
	}

	@Override
	public Set<StringTuple> zRevRangeByScoreWithScores(String key, double min, double max, long offset, long count) {
		return convertAndReturn(delegate.zRevRangeByScoreWithScores(serialize(key), min, max, offset, count),
				tupleToStringTuple);
	}

	@Override
	public Long zRevRank(String key, String value) {
		return zRevRank(serialize(key), serialize(value));
	}

	@Override
	public Double zScore(String key, String value) {
		return zScore(serialize(key), serialize(value));
	}

	@Override
	public List<Double> zMScore(String key, String... values) {
		return zMScore(serialize(key), serializeMulti(values));
	}

	@Override
	public Long zUnionStore(String destKey, Aggregate aggregate, int[] weights, String... sets) {
		return zUnionStore(serialize(destKey), aggregate, weights, serializeMulti(sets));
	}

	@Override
	public Long zUnionStore(String destKey, String... sets) {
		return zUnionStore(serialize(destKey), serializeMulti(sets));
	}

	@Override
	public Long geoAdd(byte[] key, Point point, byte[] member) {

		return convertAndReturn(delegate.geoAdd(key, point, member), Converters.identityConverter());
	}

	public Long geoAdd(byte[] key, GeoLocation<byte[]> location) {
		return convertAndReturn(delegate.geoAdd(key, location), Converters.identityConverter());
	}

	@Override
	public Long geoAdd(String key, Point point, String member) {
		return geoAdd(serialize(key), point, serialize(member));
	}

	@Override
	public Long geoAdd(String key, GeoLocation<String> location) {

		Assert.notNull(location, "Location must not be null");
		return geoAdd(key, location.getPoint(), location.getName());
	}

	@Override
	public Long geoAdd(byte[] key, Map<byte[], Point> memberCoordinateMap) {
		return convertAndReturn(delegate.geoAdd(key, memberCoordinateMap), Converters.identityConverter());
	}

	@Override
	public Long geoAdd(byte[] key, Iterable<GeoLocation<byte[]>> locations) {
		return convertAndReturn(delegate.geoAdd(key, locations), Converters.identityConverter());
	}

	@Override
	public Long geoAdd(String key, Map<String, Point> memberCoordinateMap) {

		Assert.notNull(memberCoordinateMap, "MemberCoordinateMap must not be null");

		Map<byte[], Point> byteMap = new HashMap<>();
		for (Entry<String, Point> entry : memberCoordinateMap.entrySet()) {
			byteMap.put(serialize(entry.getKey()), entry.getValue());
		}

		return geoAdd(serialize(key), byteMap);
	}

	@Override
	public Long geoAdd(String key, Iterable<GeoLocation<String>> locations) {

		Assert.notNull(locations, "Locations must not be null");

		Map<byte[], Point> byteMap = new HashMap<>();
		for (GeoLocation<String> location : locations) {
			byteMap.put(serialize(location.getName()), location.getPoint());
		}

		return geoAdd(serialize(key), byteMap);
	}

	@Override
	public Distance geoDist(byte[] key, byte[] member1, byte[] member2) {
		return convertAndReturn(delegate.geoDist(key, member1, member2), Converters.identityConverter());
	}

	@Override
	public Distance geoDist(String key, String member1, String member2) {
		return geoDist(serialize(key), serialize(member1), serialize(member2));
	}

	@Override
	public Distance geoDist(byte[] key, byte[] member1, byte[] member2, Metric metric) {
		return convertAndReturn(delegate.geoDist(key, member1, member2, metric), Converters.identityConverter());
	}

	@Override
	public Distance geoDist(String key, String member1, String member2, Metric metric) {
		return geoDist(serialize(key), serialize(member1), serialize(member2), metric);
	}

	@Override
	public List<String> geoHash(byte[] key, byte[]... members) {
		return convertAndReturn(delegate.geoHash(key, members), Converters.identityConverter());
	}

	@Override
	public List<String> geoHash(String key, String... members) {
		return convertAndReturn(delegate.geoHash(serialize(key), serializeMulti(members)), Converters.identityConverter());
	}

	@Override
	public List<Point> geoPos(byte[] key, byte[]... members) {
		return convertAndReturn(delegate.geoPos(key, members), Converters.identityConverter());
	}

	@Override
	public List<Point> geoPos(String key, String... members) {
		return geoPos(serialize(key), serializeMulti(members));
	}

	@Override
	public GeoResults<GeoLocation<String>> geoRadius(String key, Circle within) {
		return convertAndReturn(delegate.geoRadius(serialize(key), within), byteGeoResultsToStringGeoResults);
	}

	@Override
	public GeoResults<GeoLocation<String>> geoRadius(String key, Circle within, GeoRadiusCommandArgs args) {
		return convertAndReturn(delegate.geoRadius(serialize(key), within, args), byteGeoResultsToStringGeoResults);
	}

	@Override
	public GeoResults<GeoLocation<String>> geoRadiusByMember(String key, String member, double radius) {
		return geoRadiusByMember(key, member, new Distance(radius, DistanceUnit.METERS));
	}

	@Override
	public GeoResults<GeoLocation<String>> geoRadiusByMember(String key, String member, Distance radius) {

		return convertAndReturn(delegate.geoRadiusByMember(serialize(key), serialize(member), radius),
				byteGeoResultsToStringGeoResults);
	}

	@Override
	public GeoResults<GeoLocation<String>> geoRadiusByMember(String key, String member, Distance radius,
			GeoRadiusCommandArgs args) {

		return convertAndReturn(delegate.geoRadiusByMember(serialize(key), serialize(member), radius, args),
				byteGeoResultsToStringGeoResults);
	}

	@Override
	public GeoResults<GeoLocation<byte[]>> geoRadius(byte[] key, Circle within) {
		return convertAndReturn(delegate.geoRadius(key, within), Converters.identityConverter());
	}

	@Override
	public GeoResults<GeoLocation<byte[]>> geoRadius(byte[] key, Circle within, GeoRadiusCommandArgs args) {
		return convertAndReturn(delegate.geoRadius(key, within, args), Converters.identityConverter());
	}

	@Override
	public GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte[] key, byte[] member, double radius) {
		return geoRadiusByMember(key, member, new Distance(radius, DistanceUnit.METERS));
	}

	@Override
	public GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte[] key, byte[] member, Distance radius) {
		return convertAndReturn(delegate.geoRadiusByMember(key, member, radius), Converters.identityConverter());
	}

	@Override
	public GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte[] key, byte[] member, Distance radius,
			GeoRadiusCommandArgs args) {

		return convertAndReturn(delegate.geoRadiusByMember(key, member, radius, args), Converters.identityConverter());
	}

	@Override
	public Long geoRemove(byte[] key, byte[]... members) {
		return zRem(key, members);
	}

	@Override
	public Long geoRemove(String key, String... members) {
		return geoRemove(serialize(key), serializeMulti(members));
	}

	@Override
	public GeoResults<GeoLocation<byte[]>> geoSearch(byte[] key, GeoReference<byte[]> reference, GeoShape predicate,
			GeoSearchCommandArgs args) {
		return convertAndReturn(delegate.geoSearch(key, reference, predicate, args), Converters.identityConverter());
	}

	@Override
	public Long geoSearchStore(byte[] destKey, byte[] key, GeoReference<byte[]> reference, GeoShape predicate,
			GeoSearchStoreCommandArgs args) {
		return convertAndReturn(delegate.geoSearchStore(destKey, key, reference, predicate, args),
				Converters.identityConverter());
	}

	@Override
	public GeoResults<GeoLocation<String>> geoSearch(String key, GeoReference<String> reference, GeoShape predicate,
			GeoSearchCommandArgs args) {
		return convertAndReturn(delegate.geoSearch(serialize(key), serialize(reference), predicate, args),
				byteGeoResultsToStringGeoResults);
	}

	@Override
	public Long geoSearchStore(String destKey, String key, GeoReference<String> reference, GeoShape predicate,
			GeoSearchStoreCommandArgs args) {
		return convertAndReturn(
				delegate.geoSearchStore(serialize(destKey), serialize(key), serialize(reference), predicate, args),
				Converters.identityConverter());
	}

	@Override
	public List<Object> closePipeline() {

		try {
			return convertResults(delegate.closePipeline(), pipelineConverters);
		} finally {
			pipelineConverters.clear();
		}
	}

	@Override
	public boolean isPipelined() {
		return delegate.isPipelined();
	}

	@Override
	public void openPipeline() {
		delegate.openPipeline();
	}

	@Override
	public Object execute(String command) {
		return execute(command, EMPTY_2D_BYTE_ARRAY);
	}

	@Override
	public Object execute(String command, byte[]... args) {
		return convertAndReturn(delegate.execute(command, args), Converters.identityConverter());
	}

	@Override
	public Object execute(String command, String... args) {
		return execute(command, serializeMulti(args));
	}

	@Override
	public Boolean pExpire(String key, long millis, ExpirationOptions.Condition condition) {
		return pExpire(serialize(key), millis, condition);
	}

	@Override
	public Boolean pExpireAt(String key, long unixTimeInMillis, ExpirationOptions.Condition condition) {
		return pExpireAt(serialize(key), unixTimeInMillis, condition);
	}

	@Override
	public Long pTtl(String key) {
		return pTtl(serialize(key));
	}

	@Override
	public Long pTtl(String key, TimeUnit timeUnit) {
		return pTtl(serialize(key), timeUnit);
	}

	@Override
	public String scriptLoad(String script) {
		return scriptLoad(serialize(script));
	}

	/**
	 * NOTE: This method will not deserialize Strings returned by Lua scripts, as they may not be encoded with the same
	 * serializer used here. They will be returned as byte[]s
	 */
	public <T> T eval(String script, ReturnType returnType, int numKeys, String... keysAndArgs) {
		return eval(serialize(script), returnType, numKeys, serializeMulti(keysAndArgs));
	}

	/**
	 * NOTE: This method will not deserialize Strings returned by Lua scripts, as they may not be encoded with the same
	 * serializer used here. They will be returned as byte[]s
	 */
	public <T> T evalSha(String scriptSha1, ReturnType returnType, int numKeys, String... keysAndArgs) {
		return evalSha(scriptSha1, returnType, numKeys, serializeMulti(keysAndArgs));
	}

	@Override
	public Long time() {
		return convertAndReturn(this.delegate.time(), Converters.identityConverter());
	}

	@Override
	public Long time(TimeUnit timeUnit) {
		return convertAndReturn(this.delegate.time(timeUnit), Converters.identityConverter());
	}

	@Override
	public List<RedisClientInfo> getClientList() {
		return convertAndReturn(this.delegate.getClientList(), Converters.identityConverter());
	}

	@Override
	public void replicaOf(String host, int port) {
		this.delegate.replicaOf(host, port);
	}

	@Override
	public void replicaOfNoOne() {
		this.delegate.replicaOfNoOne();
	}

	@Override
	public Cursor<byte[]> scan(ScanOptions options) {
		return this.delegate.scan(options);
	}

	@Override
	public Cursor<Tuple> zScan(byte[] key, ScanOptions options) {
		return this.delegate.zScan(key, options);
	}

	@Override
	public Cursor<byte[]> sScan(byte[] key, ScanOptions options) {
		return this.delegate.sScan(key, options);
	}

	@Override
	public Cursor<Entry<byte[], byte[]>> hScan(byte[] key, ScanOptions options) {
		return this.delegate.hScan(key, options);
	}

	@Override
	public Long hStrLen(byte[] key, byte[] field) {
		return convertAndReturn(delegate.hStrLen(key, field), Converters.identityConverter());
	}

	public @Nullable List<Long> applyHashFieldExpiration(byte[] key,
			org.springframework.data.redis.core.types.Expiration expiration,
			ExpirationOptions options, byte[]... fields) {
		return this.delegate.applyHashFieldExpiration(key, expiration, options, fields);
	}

	@Override
	public List<Long> hExpire(byte[] key, long seconds, ExpirationOptions.Condition condition, byte[]... fields) {
		return this.delegate.hExpire(key, seconds, condition, fields);
	}

	@Override
	public List<Long> hpExpire(byte[] key, long millis, ExpirationOptions.Condition condition, byte[]... fields) {
		return this.delegate.hpExpire(key, millis, condition, fields);
	}

	@Override
	public List<Long> hExpireAt(byte[] key, long unixTime, ExpirationOptions.Condition condition, byte[]... fields) {
		return this.delegate.hExpireAt(key, unixTime, condition, fields);
	}

	@Override
	public List<Long> hpExpireAt(byte[] key, long unixTimeInMillis, ExpirationOptions.Condition condition,
			byte[]... fields) {
		return this.delegate.hpExpireAt(key, unixTimeInMillis, condition, fields);
	}

	@Override
	public List<Long> hPersist(byte[] key, byte[]... fields) {
		return this.delegate.hPersist(key, fields);
	}

	@Override
	public List<Long> hTtl(byte[] key, byte[]... fields) {
		return this.delegate.hTtl(key, fields);
	}

	@Override
	public List<Long> hpTtl(byte[] key, byte[]... fields) {
		return this.delegate.hpTtl(key, fields);
	}

	@Override
	public List<Long> hTtl(byte[] key, TimeUnit timeUnit, byte[]... fields) {
		return this.delegate.hTtl(key, timeUnit, fields);
	}

    @Override
		public List<byte[]> hGetDel(@NonNull byte[] key, @NonNull byte[]... fields) {
        return convertAndReturn(delegate.hGetDel(key, fields), Converters.identityConverter());
    }

    @Override
		public List<byte[]> hGetEx(@NonNull byte[] key, Expiration expiration, @NonNull byte[]... fields) {
        return convertAndReturn(delegate.hGetEx(key, expiration, fields), Converters.identityConverter());
    }

    @Override
		public Boolean hSetEx(@NonNull byte[] key, @NonNull Map<byte[], byte[]> hashes, HashFieldSetOption condition,
				Expiration expiration) {
        return convertAndReturn(delegate.hSetEx(key, hashes, condition, expiration), Converters.identityConverter());
    }

	public @Nullable List<Long> applyExpiration(String key,
			org.springframework.data.redis.core.types.Expiration expiration,
			ExpirationOptions options, String... fields) {
		return this.applyHashFieldExpiration(serialize(key), expiration, options, serializeMulti(fields));
	}

	@Override
	public List<Long> hExpire(String key, long seconds, ExpirationOptions.Condition condition, String... fields) {
		return hExpire(serialize(key), seconds, condition, serializeMulti(fields));
	}

	@Override
	public List<Long> hpExpire(String key, long millis, ExpirationOptions.Condition condition, String... fields) {
		return hpExpire(serialize(key), millis, condition, serializeMulti(fields));
	}

	@Override
	public List<Long> hExpireAt(String key, long unixTime, ExpirationOptions.Condition condition, String... fields) {
		return hExpireAt(serialize(key), unixTime, condition, serializeMulti(fields));
	}

	@Override
	public List<Long> hpExpireAt(String key, long unixTimeInMillis, ExpirationOptions.Condition condition,
			String... fields) {
		return hpExpireAt(serialize(key), unixTimeInMillis, condition, serializeMulti(fields));
	}

	@Override
	public List<Long> hPersist(String key, String... fields) {
		return hPersist(serialize(key), serializeMulti(fields));
	}

	@Override
	public List<Long> hTtl(String key, String... fields) {
		return hTtl(serialize(key), serializeMulti(fields));
	}

	@Override
	public List<Long> hTtl(String key, TimeUnit timeUnit, String... fields) {
		return hTtl(serialize(key), timeUnit, serializeMulti(fields));
	}

	@Override
	public List<Long> hpTtl(String key, String... fields) {
		return hTtl(serialize(key), serializeMulti(fields));
	}

	@Override
	public void setClientName(byte[] name) {
		this.delegate.setClientName(name);
	}

	@Override
	public void setClientName(String name) {
		setClientName(this.serializer.serialize(name));
	}

	@Override
	public void killClient(String host, int port) {
		this.delegate.killClient(host, port);
	}

	/*
	 * @see org.springframework.data.redis.connection.RedisServerCommands#getClientName()
	 */
	@Override
	public String getClientName() {
		return convertAndReturn(this.delegate.getClientName(), Converters.identityConverter());
	}

	@Override
	public Cursor<Entry<String, String>> hScan(String key, ScanOptions options) {

		return new ConvertingCursor<>(this.delegate.hScan(this.serialize(key), options), this::convertEntry);
	}

	@Override
	public Long hStrLen(String key, String field) {
		return hStrLen(serialize(key), serialize(field));
	}

	@Override
	public Cursor<String> sScan(String key, ScanOptions options) {
		return new ConvertingCursor<>(this.delegate.sScan(this.serialize(key), options), bytesToString);
	}

	@Override
	public Cursor<StringTuple> zScan(String key, ScanOptions options) {
		return new ConvertingCursor<>(delegate.zScan(this.serialize(key), options), new TupleConverter());
	}

	@Override
	public RedisSentinelConnection getSentinelConnection() {
		return delegate.getSentinelConnection();
	}

	@Override
	public Set<String> zRangeByScore(String key, String min, String max) {
		return convertAndReturn(delegate.zRangeByScore(serialize(key), min, max), byteSetToStringSet);
	}

	@Override
	public Set<String> zRangeByScore(String key, String min, String max, long offset, long count) {
		return convertAndReturn(delegate.zRangeByScore(serialize(key), min, max, offset, count), byteSetToStringSet);
	}

	@Override
	public Set<byte[]> zRangeByScore(byte[] key, String min, String max) {
		return convertAndReturn(delegate.zRangeByScore(key, min, max), Converters.identityConverter());
	}

	@Override
	public Set<byte[]> zRangeByScore(byte[] key, String min, String max, long offset, long count) {
		return convertAndReturn(delegate.zRangeByScore(key, min, max, offset, count), Converters.identityConverter());
	}

	@Override
	public Long pfAdd(byte[] key, byte[]... values) {
		return convertAndReturn(delegate.pfAdd(key, values), Converters.identityConverter());
	}

	@Override
	public Long pfAdd(String key, String... values) {
		return pfAdd(serialize(key), serializeMulti(values));
	}

	@Override
	public Long pfCount(byte[]... keys) {
		return convertAndReturn(delegate.pfCount(keys), Converters.identityConverter());
	}

	@Override
	public Long pfCount(String... keys) {
		return pfCount(serializeMulti(keys));
	}

	@Override
	public void pfMerge(byte[] destinationKey, byte[]... sourceKeys) {
		delegate.pfMerge(destinationKey, sourceKeys);
	}

	@Override
	public void pfMerge(String destinationKey, String... sourceKeys) {
		this.pfMerge(serialize(destinationKey), serializeMulti(sourceKeys));
	}

	@Override
	public Set<byte[]> zRangeByLex(byte[] key) {
		return convertAndReturn(delegate.zRangeByLex(key), Converters.identityConverter());
	}

	@Override
	public Set<byte[]> zRangeByLex(byte[] key, org.springframework.data.domain.Range<byte[]> range) {
		return convertAndReturn(delegate.zRangeByLex(key, range), Converters.identityConverter());
	}

	@Override
	public Set<byte[]> zRangeByLex(byte[] key, org.springframework.data.domain.Range<byte[]> range,
			org.springframework.data.redis.connection.Limit limit) {
		return convertAndReturn(delegate.zRangeByLex(key, range, limit), Converters.identityConverter());
	}

	@Override
	public Set<String> zRangeByLex(String key) {
		return zRangeByLex(key, org.springframework.data.domain.Range.unbounded());
	}

	@Override
	public Set<String> zRangeByLex(String key, org.springframework.data.domain.Range<String> range) {
		return zRangeByLex(key, range, org.springframework.data.redis.connection.Limit.unlimited());
	}

	@Override
	public Set<String> zRangeByLex(String key, org.springframework.data.domain.Range<String> range,
			org.springframework.data.redis.connection.Limit limit) {
		return convertAndReturn(delegate.zRangeByLex(serialize(key), serialize(range), limit), byteSetToStringSet);
	}

	@Override
	public Set<byte[]> zRevRangeByLex(byte[] key, org.springframework.data.domain.Range<byte[]> range,
			org.springframework.data.redis.connection.Limit limit) {
		return convertAndReturn(delegate.zRevRangeByLex(key, range, limit), Converters.identityConverter());
	}

	@Override
	public Set<String> zRevRangeByLex(String key, org.springframework.data.domain.Range<String> range,
			org.springframework.data.redis.connection.Limit limit) {
		return convertAndReturn(delegate.zRevRangeByLex(serialize(key), serialize(range), limit), byteSetToStringSet);
	}

	@Override
	public Long zRangeStoreByLex(byte[] dstKey, byte[] srcKey,
								 org.springframework.data.domain.Range<byte[]> range,
								 org.springframework.data.redis.connection.Limit limit) {
		return convertAndReturn(delegate.zRangeStoreByLex(dstKey, srcKey, range, limit),
				Converters.identityConverter());
	}

	@Override
	public Long zRangeStoreByLex(String dstKey, String srcKey,
			org.springframework.data.domain.Range<String> range, org.springframework.data.redis.connection.Limit limit) {
		return convertAndReturn(delegate.zRangeStoreByLex(serialize(dstKey), serialize(srcKey), serialize(range), limit),
				Converters.identityConverter());
	}

	@Override
	public Long zRangeStoreRevByLex(byte[] dstKey, byte[] srcKey, org.springframework.data.domain.Range<byte[]> range,
			org.springframework.data.redis.connection.Limit limit) {
		return convertAndReturn(delegate.zRangeStoreRevByLex(dstKey, srcKey, range, limit), Converters.identityConverter());
	}

	@Override
	public Long zRangeStoreRevByLex(String dstKey, String srcKey, org.springframework.data.domain.Range<String> range,
			org.springframework.data.redis.connection.Limit limit) {
		return convertAndReturn(delegate.zRangeStoreRevByLex(serialize(dstKey), serialize(srcKey), serialize(range), limit),
				Converters.identityConverter());
	}

	@Override
	public Long zRangeStoreByScore(byte[] dstKey, byte[] srcKey,
			org.springframework.data.domain.Range<? extends Number> range,
								   org.springframework.data.redis.connection.Limit limit) {
		return convertAndReturn(delegate.zRangeStoreByScore(dstKey, srcKey, range, limit),
				Converters.identityConverter());
	}

	@Override
	public Long zRangeStoreByScore(String dstKey, String srcKey,
			org.springframework.data.domain.Range<? extends Number> range,
			org.springframework.data.redis.connection.Limit limit) {
		return convertAndReturn(delegate.zRangeStoreByScore(serialize(dstKey), serialize(srcKey), range, limit),
				Converters.identityConverter());
	}

	@Override
	public Long zRangeStoreRevByScore(byte[] dstKey, byte[] srcKey,
			org.springframework.data.domain.Range<? extends Number> range,
			org.springframework.data.redis.connection.Limit limit) {
		return convertAndReturn(delegate.zRangeStoreRevByScore(dstKey, srcKey, range, limit),
				Converters.identityConverter());
	}

	@Override
	public Long zRangeStoreRevByScore(String dstKey, String srcKey,
			org.springframework.data.domain.Range<? extends Number> range,
			org.springframework.data.redis.connection.Limit limit) {
		return convertAndReturn(delegate.zRangeStoreRevByScore(serialize(dstKey), serialize(srcKey), range, limit),
				Converters.identityConverter());
	}

	@Override
	public void migrate(byte[] key, RedisNode target, int dbIndex, @Nullable MigrateOption option) {
		delegate.migrate(key, target, dbIndex, option);
	}

	@Override
	public void migrate(byte[] key, RedisNode target, int dbIndex, @Nullable MigrateOption option, long timeout) {
		delegate.migrate(key, target, dbIndex, option, timeout);
	}

	@Override
	public Long xAck(String key, String group, RecordId... recordIds) {
		return convertAndReturn(delegate.xAck(this.serialize(key), group, recordIds), Converters.identityConverter());
	}

	@Override
	public RecordId xAdd(StringRecord record, XAddOptions options) {
		return convertAndReturn(delegate.xAdd(record.serialize(serializer), options), Converters.identityConverter());
	}

	@Override
	public List<RecordId> xClaimJustId(String key, String group, String consumer, XClaimOptions options) {
		return convertAndReturn(delegate.xClaimJustId(serialize(key), group, consumer, options),
				Converters.identityConverter());
	}

	@Override
	public List<StringRecord> xClaim(String key, String group, String consumer, XClaimOptions options) {
		return convertAndReturn(delegate.xClaim(serialize(key), group, consumer, options),
				listByteMapRecordToStringMapRecordConverter);
	}

	@Override
	public Long xDel(String key, RecordId... recordIds) {
		return convertAndReturn(delegate.xDel(serialize(key), recordIds), Converters.identityConverter());
	}

	@Override
	public String xGroupCreate(String key, ReadOffset readOffset, String group) {
		return convertAndReturn(delegate.xGroupCreate(serialize(key), group, readOffset), Converters.identityConverter());
	}

	@Override
	public String xGroupCreate(String key, ReadOffset readOffset, String group, boolean mkStream) {
		return convertAndReturn(delegate.xGroupCreate(serialize(key), group, readOffset, mkStream),
				Converters.identityConverter());
	}

	@Override
	public Boolean xGroupDelConsumer(String key, Consumer consumer) {
		return convertAndReturn(delegate.xGroupDelConsumer(serialize(key), consumer), Converters.identityConverter());
	}

	@Override
	public Boolean xGroupDestroy(String key, String group) {
		return convertAndReturn(delegate.xGroupDestroy(serialize(key), group), Converters.identityConverter());
	}

	@Override
	public XInfoStream xInfo(String key) {
		return convertAndReturn(delegate.xInfo(serialize(key)), Converters.identityConverter());
	}

	@Override
	public XInfoGroups xInfoGroups(String key) {
		return convertAndReturn(delegate.xInfoGroups(serialize(key)), Converters.identityConverter());
	}

	@Override
	public XInfoConsumers xInfoConsumers(String key, String groupName) {
		return convertAndReturn(delegate.xInfoConsumers(serialize(key), groupName), Converters.identityConverter());
	}

	@Override
	public Long xLen(String key) {
		return convertAndReturn(delegate.xLen(serialize(key)), Converters.identityConverter());
	}

	@Override
	public PendingMessagesSummary xPending(String key, String groupName) {
		return convertAndReturn(delegate.xPending(serialize(key), groupName), Converters.identityConverter());
	}

	@Override
	public PendingMessages xPending(String key, String groupName, String consumer,
			org.springframework.data.domain.Range<String> range, Long count) {
		return convertAndReturn(delegate.xPending(serialize(key), groupName, consumer, range, count),
				Converters.identityConverter());
	}

	@Override
	public PendingMessages xPending(String key, String groupName, String consumerName,
			org.springframework.data.domain.Range<String> range, Long count, Duration minIdleTime) {
		return convertAndReturn(delegate.xPending(serialize(key), groupName, consumerName, range, count, minIdleTime),
				Converters.identityConverter());
	}

	@Override
	public PendingMessages xPending(String key, String groupName, org.springframework.data.domain.Range<String> range,
			Long count) {
		return convertAndReturn(delegate.xPending(serialize(key), groupName, range, count), Converters.identityConverter());
	}

	@Override
	public PendingMessages xPending(String key, String groupName, org.springframework.data.domain.Range<String> range,
			Long count, Duration minIdleTime) {
		return convertAndReturn(delegate.xPending(serialize(key), groupName, range, count, minIdleTime),
				Converters.identityConverter());
	}

	@Override
	public PendingMessages xPending(String key, String groupName, XPendingOptions options) {
		return convertAndReturn(delegate.xPending(serialize(key), groupName, options), Converters.identityConverter());
	}

	@Override
	public List<StringRecord> xRange(String key, org.springframework.data.domain.Range<String> range,
			org.springframework.data.redis.connection.Limit limit) {
		return convertAndReturn(delegate.xRange(serialize(key), range, limit), listByteMapRecordToStringMapRecordConverter);
	}

	@Override
	public List<StringRecord> xReadAsString(StreamReadOptions readOptions, StreamOffset<String>... streams) {
		return convertAndReturn(delegate.xRead(readOptions, serialize(streams)),
				listByteMapRecordToStringMapRecordConverter);
	}

	@Override
	public List<StringRecord> xReadGroupAsString(Consumer consumer, StreamReadOptions readOptions,
			StreamOffset<String>... streams) {

		return convertAndReturn(delegate.xReadGroup(consumer, readOptions, serialize(streams)),
				listByteMapRecordToStringMapRecordConverter);
	}

	@Override
	public List<StringRecord> xRevRange(String key, org.springframework.data.domain.Range<String> range,
			org.springframework.data.redis.connection.Limit limit) {

		return convertAndReturn(delegate.xRevRange(serialize(key), range, limit),
				listByteMapRecordToStringMapRecordConverter);
	}

	@Override
	public Long xTrim(String key, long count) {
		return xTrim(key, count, false);
	}

	@Override
	public Long xTrim(String key, long count, boolean approximateTrimming) {
		return convertAndReturn(delegate.xTrim(serialize(key), count, approximateTrimming), Converters.identityConverter());
	}

	@Override
	public Long xAck(byte[] key, String group, RecordId... recordIds) {
		return delegate.xAck(key, group, recordIds);
	}

	@Override
	public RecordId xAdd(MapRecord<byte[], byte[], byte[]> record, XAddOptions options) {
		return delegate.xAdd(record, options);
	}

	@Override
	public List<RecordId> xClaimJustId(byte[] key, String group, String newOwner, XClaimOptions options) {
		return delegate.xClaimJustId(key, group, newOwner, options);
	}

	@Override
	public List<ByteRecord> xClaim(byte[] key, String group, String newOwner, XClaimOptions options) {
		return delegate.xClaim(key, group, newOwner, options);
	}

	@Override
	public Long xDel(byte[] key, RecordId... recordIds) {
		return delegate.xDel(key, recordIds);
	}

	@Override
	public String xGroupCreate(byte[] key, String groupName, ReadOffset readOffset) {
		return delegate.xGroupCreate(key, groupName, readOffset);
	}

	@Override
	public String xGroupCreate(byte[] key, String groupName, ReadOffset readOffset, boolean mkStream) {
		return delegate.xGroupCreate(key, groupName, readOffset, mkStream);
	}

	@Override
	public Boolean xGroupDelConsumer(byte[] key, Consumer consumer) {
		return delegate.xGroupDelConsumer(key, consumer);
	}

	@Override
	public Boolean xGroupDestroy(byte[] key, String groupName) {
		return delegate.xGroupDestroy(key, groupName);
	}

	@Override
	public XInfoStream xInfo(byte[] key) {
		return delegate.xInfo(key);
	}

	@Override
	public XInfoGroups xInfoGroups(byte[] key) {
		return delegate.xInfoGroups(key);
	}

	@Override
	public XInfoConsumers xInfoConsumers(byte[] key, String groupName) {
		return delegate.xInfoConsumers(key, groupName);
	}

	@Override
	public Long xLen(byte[] key) {
		return delegate.xLen(key);
	}

	@Override
	public PendingMessagesSummary xPending(byte[] key, String groupName) {
		return delegate.xPending(key, groupName);
	}

	@Override
	public PendingMessages xPending(byte[] key, String groupName, XPendingOptions options) {
		return delegate.xPending(key, groupName, options);
	}

	@Override
	public List<ByteRecord> xRange(byte[] key, org.springframework.data.domain.Range<String> range,
			org.springframework.data.redis.connection.Limit limit) {
		return delegate.xRange(key, range, limit);
	}

	@Override
	public List<ByteRecord> xRead(StreamReadOptions readOptions, StreamOffset<byte[]>... streams) {
		return delegate.xRead(readOptions, streams);
	}

	@Override
	public List<ByteRecord> xReadGroup(Consumer consumer, StreamReadOptions readOptions,
			StreamOffset<byte[]>... streams) {
		return delegate.xReadGroup(consumer, readOptions, streams);
	}

	@Override
	public List<ByteRecord> xRevRange(byte[] key, org.springframework.data.domain.Range<String> range,
			org.springframework.data.redis.connection.Limit limit) {
		return delegate.xRevRange(key, range, limit);
	}

	@Override
	public Long xTrim(byte[] key, long count) {
		return xTrim(key, count, false);
	}

	@Override
	public Long xTrim(byte[] key, long count, boolean approximateTrimming) {
		return delegate.xTrim(key, count, approximateTrimming);
	}

	/**
	 * Specifies if pipelined and tx results should be deserialized to Strings. If false, results of
	 * {@link #closePipeline()} and {@link #exec()} will be of the type returned by the underlying connection
	 *
	 * @param deserializePipelineAndTxResults Whether or not to deserialize pipeline and tx results
	 */
	public void setDeserializePipelineAndTxResults(boolean deserializePipelineAndTxResults) {
		this.deserializePipelineAndTxResults = deserializePipelineAndTxResults;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private <T> @Nullable T convertAndReturn(@Nullable Object value, Converter converter) {

		if (isFutureConversion()) {

			addResultConverter(converter);
			return null;
		}

		if (!(converter instanceof ListConverter) && value instanceof List list) {
			return (T) new ListConverter<>(converter).convert(list);
		}

		return value == null ? null
				: ObjectUtils.nullSafeEquals(converter, Converters.identityConverter()) ? (T) value
						: (T) converter.convert(value);
	}

	private void addResultConverter(Converter<?, ?> converter) {
		if (isQueueing()) {
			txConverters.add(converter);
		} else {
			pipelineConverters.add(converter);
		}
	}

	private boolean isFutureConversion() {
		return isPipelined() || isQueueing();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private List<Object> convertResults(@Nullable List<Object> results, Queue<Converter> converters) {
		if (!deserializePipelineAndTxResults || results == null) {
			return results;
		}
		if (results.size() != converters.size()) {
			// Some of the commands were done directly on the delegate, don't attempt to convert
			log.warn("Delegate returned an unexpected number of results; Abandoning type conversion.");
			return results;
		}
		List<Object> convertedResults = new ArrayList<>(results.size());
		for (Object result : results) {

			Converter converter = converters.remove();
			convertedResults.add(result == null ? null : converter.convert(result));
		}
		return convertedResults;
	}

	@Override
	public List<Long> bitField(byte[] key, BitFieldSubCommands subCommands) {
		return delegate.bitField(key, subCommands);
	}

	@Override
	public List<Long> bitfield(String key, BitFieldSubCommands operation) {

		List<Long> results = delegate.bitField(serialize(key), operation);
		if (isFutureConversion()) {
			addResultConverter(Converters.identityConverter());
		}
		return results;
	}

	@Override
	public RedisConnection getDelegate() {
		return delegate;
	}
}
