/*
 * Copyright 2011-2021 the original author or authors.
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
import org.springframework.data.redis.core.ConvertingCursor;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.core.types.RedisClientInfo;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.lang.Nullable;
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
 */
public class DefaultStringRedisConnection implements StringRedisConnection, DecoratedRedisConnection {

	private static final byte[][] EMPTY_2D_BYTE_ARRAY = new byte[0][];

	private final Log log = LogFactory.getLog(DefaultStringRedisConnection.class);
	private final RedisConnection delegate;
	private final RedisSerializer<String> serializer;
	private Converter<byte[], String> bytesToString = new DeserializingConverter();
	private Converter<String, byte[]> stringToBytes = new SerializingConverter();
	private SetConverter<Tuple, StringTuple> tupleToStringTuple = new SetConverter<>(new TupleConverter());
	private SetConverter<StringTuple, Tuple> stringTupleToTuple = new SetConverter<>(new StringTupleConverter());
	private ListConverter<byte[], String> byteListToStringList = new ListConverter<>(bytesToString);
	private MapConverter<byte[], String> byteMapToStringMap = new MapConverter<>(bytesToString);
	private MapConverter<String, byte[]> stringMapToByteMap = new MapConverter<>(stringToBytes);
	private SetConverter<byte[], String> byteSetToStringSet = new SetConverter<>(bytesToString);
	private Converter<GeoResults<GeoLocation<byte[]>>, GeoResults<GeoLocation<String>>> byteGeoResultsToStringGeoResults;
	private Converter<ByteRecord, StringRecord> byteMapRecordToStringMapRecordConverter = new Converter<ByteRecord, StringRecord>() {

		@Nullable
		@Override
		public StringRecord convert(ByteRecord source) {
			return StringRecord.of(source.deserialize(serializer));
		}
	};

	private ListConverter<ByteRecord, StringRecord> listByteMapRecordToStringMapRecordConverter = new ListConverter<>(
			byteMapRecordToStringMapRecordConverter);

	@SuppressWarnings("rawtypes") private Queue<Converter> pipelineConverters = new LinkedList<>();
	@SuppressWarnings("rawtypes") private Queue<Converter> txConverters = new LinkedList<>();
	private boolean deserializePipelineAndTxResults = false;

	private class DeserializingConverter implements Converter<byte[], String> {
		public String convert(byte[] source) {
			return serializer.deserialize(source);
		}
	}

	private class SerializingConverter implements Converter<String, byte[]> {

		@Nullable
		@Override
		public byte[] convert(String source) {
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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#append(byte[], byte[])
	 */
	@Override
	public Long append(byte[] key, byte[] value) {
		return convertAndReturn(delegate.append(key, value), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#bgSave()
	 */
	@Override
	public void bgSave() {
		delegate.bgSave();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#bgReWriteAof()
	 */
	@Override
	public void bgReWriteAof() {
		delegate.bgReWriteAof();
	}

	/**
	 * @deprecated As of 1.3, use {@link #bgReWriteAof}.
	 */
	@Deprecated
	public void bgWriteAof() {
		bgReWriteAof();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#bLPop(int, byte[][])
	 */
	@Override
	public List<byte[]> bLPop(int timeout, byte[]... keys) {
		return convertAndReturn(delegate.bLPop(timeout, keys), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#bRPop(int, byte[][])
	 */
	@Override
	public List<byte[]> bRPop(int timeout, byte[]... keys) {
		return convertAndReturn(delegate.bRPop(timeout, keys), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#bRPopLPush(int, byte[], byte[])
	 */
	@Override
	public byte[] bRPopLPush(int timeout, byte[] srcKey, byte[] dstKey) {
		return convertAndReturn(delegate.bRPopLPush(timeout, srcKey, dstKey), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#close()
	 */
	@Override
	public void close() throws RedisSystemException {
		delegate.close();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#copy(byte[], byte[], boolean)
	 */
	@Override
	public Boolean copy(byte[] sourceKey, byte[] targetKey, boolean replace) {
		return convertAndReturn(delegate.copy(sourceKey, targetKey, replace), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#dbSize()
	 */
	@Override
	public Long dbSize() {
		return convertAndReturn(delegate.dbSize(), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#decr(byte[])
	 */
	@Override
	public Long decr(byte[] key) {
		return convertAndReturn(delegate.decr(key), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#decrBy(byte[], long)
	 */
	@Override
	public Long decrBy(byte[] key, long value) {
		return convertAndReturn(delegate.decrBy(key, value), Converters.identityConverter());
	}


	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#del(byte[][])
	 */
	@Override
	public Long del(byte[]... keys) {
		return convertAndReturn(delegate.del(keys), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#unlink(byte[][])
	 */
	@Override
	public Long unlink(byte[]... keys) {
		return convertAndReturn(delegate.unlink(keys), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisTxCommands#discard()
	 */
	@Override
	public void discard() {
		try {
			delegate.discard();
		} finally {
			txConverters.clear();
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnectionCommands#echo(byte[])
	 */
	@Override
	public byte[] echo(byte[] message) {
		return convertAndReturn(delegate.echo(message), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisTxCommands#exec()
	 */
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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#exists(byte[])
	 */
	@Override
	public Boolean exists(byte[] key) {
		return convertAndReturn(delegate.exists(key), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#exists(String[])
	 */
	@Override
	public Long exists(String... keys) {
		return convertAndReturn(delegate.exists(serializeMulti(keys)), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#exists(byte[][])
	 */
	@Override
	public Long exists(byte[]... keys) {
		return convertAndReturn(delegate.exists(keys), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#expire(byte[], long)
	 */
	@Override
	public Boolean expire(byte[] key, long seconds) {
		return convertAndReturn(delegate.expire(key, seconds), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#expireAt(byte[], long)
	 */
	@Override
	public Boolean expireAt(byte[] key, long unixTime) {
		return convertAndReturn(delegate.expireAt(key, unixTime), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#flushAll()
	 */
	@Override
	public void flushAll() {
		delegate.flushAll();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#flushDb()
	 */
	@Override
	public void flushDb() {
		delegate.flushDb();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#get(byte[])
	 */
	@Override
	public byte[] get(byte[] key) {
		return convertAndReturn(delegate.get(key), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#getBit(byte[], long)
	 */
	@Override
	public Boolean getBit(byte[] key, long offset) {
		return convertAndReturn(delegate.getBit(key, offset), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#getConfig(java.lang.String)
	 */
	@Override
	public Properties getConfig(String pattern) {
		return convertAndReturn(delegate.getConfig(pattern), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#getNativeConnection()
	 */
	@Override
	public Object getNativeConnection() {
		return convertAndReturn(delegate.getNativeConnection(), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#getRange(byte[], long, long)
	 */
	@Override
	public byte[] getRange(byte[] key, long start, long end) {
		return convertAndReturn(delegate.getRange(key, start, end), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#getSet(byte[], byte[])
	 */
	@Override
	public byte[] getSet(byte[] key, byte[] value) {
		return convertAndReturn(delegate.getSet(key, value), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisPubSubCommands#getSubscription()
	 */
	@Override
	public Subscription getSubscription() {
		return delegate.getSubscription();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hDel(byte[], byte[][])
	 */
	@Override
	public Long hDel(byte[] key, byte[]... fields) {
		return convertAndReturn(delegate.hDel(key, fields), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hExists(byte[], byte[])
	 */
	@Override
	public Boolean hExists(byte[] key, byte[] field) {
		return convertAndReturn(delegate.hExists(key, field), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hGet(byte[], byte[])
	 */
	@Override
	public byte[] hGet(byte[] key, byte[] field) {
		return convertAndReturn(delegate.hGet(key, field), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hGetAll(byte[])
	 */
	@Override
	public Map<byte[], byte[]> hGetAll(byte[] key) {
		return convertAndReturn(delegate.hGetAll(key), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hIncrBy(byte[], byte[], long)
	 */
	@Override
	public Long hIncrBy(byte[] key, byte[] field, long delta) {
		return convertAndReturn(delegate.hIncrBy(key, field, delta), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hIncrBy(byte[], byte[], double)
	 */
	@Override
	public Double hIncrBy(byte[] key, byte[] field, double delta) {
		return convertAndReturn(delegate.hIncrBy(key, field, delta), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hKeys(byte[])
	 */
	@Override
	public Set<byte[]> hKeys(byte[] key) {
		return convertAndReturn(delegate.hKeys(key), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hLen(byte[])
	 */
	@Override
	public Long hLen(byte[] key) {
		return convertAndReturn(delegate.hLen(key), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hMGet(byte[], byte[][])
	 */
	@Override
	public List<byte[]> hMGet(byte[] key, byte[]... fields) {
		return convertAndReturn(delegate.hMGet(key, fields), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hMSet(byte[], java.util.Map)
	 */
	@Override
	public void hMSet(byte[] key, Map<byte[], byte[]> hashes) {
		delegate.hMSet(key, hashes);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hSet(byte[], byte[], byte[])
	 */
	@Override
	public Boolean hSet(byte[] key, byte[] field, byte[] value) {
		return convertAndReturn(delegate.hSet(key, field, value), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hSetNX(byte[], byte[], byte[])
	 */
	@Override
	public Boolean hSetNX(byte[] key, byte[] field, byte[] value) {
		return convertAndReturn(delegate.hSetNX(key, field, value), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hVals(byte[])
	 */
	@Override
	public List<byte[]> hVals(byte[] key) {
		return convertAndReturn(delegate.hVals(key), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#incr(byte[])
	 */
	@Override
	public Long incr(byte[] key) {
		return convertAndReturn(delegate.incr(key), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#incrBy(byte[], long)
	 */
	@Override
	public Long incrBy(byte[] key, long value) {

		return convertAndReturn(delegate.incrBy(key, value), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#incrBy(byte[], double)
	 */
	@Override
	public Double incrBy(byte[] key, double value) {
		return convertAndReturn(delegate.incrBy(key, value), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#info()
	 */
	@Override
	public Properties info() {
		return convertAndReturn(delegate.info(), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#info(java.lang.String)
	 */
	@Override
	public Properties info(String section) {
		return convertAndReturn(delegate.info(section), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#isClosed()
	 */
	@Override
	public boolean isClosed() {
		return delegate.isClosed();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#isQueueing()
	 */
	@Override
	public boolean isQueueing() {
		return delegate.isQueueing();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisPubSubCommands#isSubscribed()
	 */
	@Override
	public boolean isSubscribed() {
		return delegate.isSubscribed();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#keys(byte[])
	 */
	@Override
	public Set<byte[]> keys(byte[] pattern) {
		return convertAndReturn(delegate.keys(pattern), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#lastSave()
	 */
	@Override
	public Long lastSave() {
		return convertAndReturn(delegate.lastSave(), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#lIndex(byte[], long)
	 */
	@Override
	public byte[] lIndex(byte[] key, long index) {
		return convertAndReturn(delegate.lIndex(key, index), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#lInsert(byte[], org.springframework.data.redis.connection.RedisListCommands.Position, byte[], byte[])
	 */
	@Override
	public Long lInsert(byte[] key, Position where, byte[] pivot, byte[] value) {
		return convertAndReturn(delegate.lInsert(key, where, pivot, value), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#lLen(byte[])
	 */
	@Override
	public Long lLen(byte[] key) {
		return convertAndReturn(delegate.lLen(key), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#lPop(byte[])
	 */
	@Override
	public byte[] lPop(byte[] key) {
		return convertAndReturn(delegate.lPop(key), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#lPop(byte[], long)
	 */
	@Override
	public List<byte[]> lPop(byte[] key, long count) {
		return convertAndReturn(delegate.lPop(key, count), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#lPos(byte[], byte[], java.lang.Integer, java.lang.Integer)
	 */
	@Override
	public List<Long> lPos(byte[] key, byte[] element, @Nullable Integer rank, @Nullable Integer count) {
		return convertAndReturn(delegate.lPos(key, element, rank, count), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#lPush(byte[], byte[][])
	 */
	@Override
	public Long lPush(byte[] key, byte[]... values) {
		return convertAndReturn(delegate.lPush(key, values), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#lPushX(byte[], byte[])
	 */
	@Override
	public Long lPushX(byte[] key, byte[] value) {
		return convertAndReturn(delegate.lPushX(key, value), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#lRange(byte[], long, long)
	 */
	@Override
	public List<byte[]> lRange(byte[] key, long start, long end) {
		return convertAndReturn(delegate.lRange(key, start, end), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#lRem(byte[], long, byte[])
	 */
	@Override
	public Long lRem(byte[] key, long count, byte[] value) {

		return convertAndReturn(delegate.lRem(key, count, value), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#lSet(byte[], long, byte[])
	 */
	@Override
	public void lSet(byte[] key, long index, byte[] value) {
		delegate.lSet(key, index, value);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#lTrim(byte[], long, long)
	 */
	@Override
	public void lTrim(byte[] key, long start, long end) {
		delegate.lTrim(key, start, end);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#mGet(byte[][])
	 */
	@Override
	public List<byte[]> mGet(byte[]... keys) {
		return convertAndReturn(delegate.mGet(keys), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#mSet(java.util.Map)
	 */
	@Override
	public Boolean mSet(Map<byte[], byte[]> tuple) {
		return convertAndReturn(delegate.mSet(tuple), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#mSetNX(java.util.Map)
	 */
	@Override
	public Boolean mSetNX(Map<byte[], byte[]> tuple) {
		return convertAndReturn(delegate.mSetNX(tuple), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisTxCommands#multi()
	 */
	@Override
	public void multi() {
		delegate.multi();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#persist(byte[])
	 */
	@Override
	public Boolean persist(byte[] key) {
		return convertAndReturn(delegate.persist(key), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#move(byte[], int)
	 */
	@Override
	public Boolean move(byte[] key, int dbIndex) {
		return convertAndReturn(delegate.move(key, dbIndex), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnectionCommands#ping()
	 */
	@Override
	public String ping() {
		return convertAndReturn(delegate.ping(), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisPubSubCommands#pSubscribe(org.springframework.data.redis.connection.MessageListener, byte[][])
	 */
	@Override
	public void pSubscribe(MessageListener listener, byte[]... patterns) {
		delegate.pSubscribe(listener, patterns);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisPubSubCommands#publish(byte[], byte[])
	 */
	@Override
	public Long publish(byte[] channel, byte[] message) {
		return convertAndReturn(delegate.publish(channel, message), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#randomKey()
	 */
	@Override
	public byte[] randomKey() {
		return convertAndReturn(delegate.randomKey(), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#rename(byte[], byte[])
	 */
	@Override
	public void rename(byte[] sourceKey, byte[] targetKey) {
		delegate.rename(sourceKey, targetKey);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#renameNX(byte[], byte[])
	 */
	@Override
	public Boolean renameNX(byte[] sourceKey, byte[] targetKey) {
		return convertAndReturn(delegate.renameNX(sourceKey, targetKey), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#resetConfigStats()
	 */
	@Override
	public void resetConfigStats() {
		delegate.resetConfigStats();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#rewriteConfig()
	 */
	@Override
	public void rewriteConfig() {
		delegate.rewriteConfig();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#rPop(byte[])
	 */
	@Override
	public byte[] rPop(byte[] key) {
		return convertAndReturn(delegate.rPop(key), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#rPop(byte[], long)
	 */
	@Override
	public List<byte[]> rPop(byte[] key, long count) {
		return convertAndReturn(delegate.rPop(key, count), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#rPopLPush(byte[], byte[])
	 */
	@Override
	public byte[] rPopLPush(byte[] srcKey, byte[] dstKey) {
		return convertAndReturn(delegate.rPopLPush(srcKey, dstKey), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#rPush(byte[], byte[][])
	 */
	@Override
	public Long rPush(byte[] key, byte[]... values) {
		return convertAndReturn(delegate.rPush(key, values), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#rPushX(byte[], byte[])
	 */
	@Override
	public Long rPushX(byte[] key, byte[] value) {
		return convertAndReturn(delegate.rPushX(key, value), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sAdd(byte[], byte[][])
	 */
	@Override
	public Long sAdd(byte[] key, byte[]... values) {
		return convertAndReturn(delegate.sAdd(key, values), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#save()
	 */
	@Override
	public void save() {
		delegate.save();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sCard(byte[])
	 */
	@Override
	public Long sCard(byte[] key) {
		return convertAndReturn(delegate.sCard(key), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sDiff(byte[][])
	 */
	@Override
	public Set<byte[]> sDiff(byte[]... keys) {
		return convertAndReturn(delegate.sDiff(keys), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sDiffStore(byte[], byte[][])
	 */
	@Override
	public Long sDiffStore(byte[] destKey, byte[]... keys) {
		return convertAndReturn(delegate.sDiffStore(destKey, keys), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnectionCommands#select(int)
	 */
	@Override
	public void select(int dbIndex) {
		delegate.select(dbIndex);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#set(byte[], byte[])
	 */
	@Override
	public Boolean set(byte[] key, byte[] value) {
		return convertAndReturn(delegate.set(key, value), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#set(byte[], byte[], org.springframework.data.redis.core.types.Expiration, org.springframework.data.redis.connection.RedisStringCommands.SetOptions)
	 */
	@Override
	public Boolean set(byte[] key, byte[] value, Expiration expiration, SetOption option) {
		return convertAndReturn(delegate.set(key, value, expiration, option), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#setBit(byte[], long, boolean)
	 */
	@Override
	public Boolean setBit(byte[] key, long offset, boolean value) {
		return convertAndReturn(delegate.setBit(key, offset, value), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#setConfig(java.lang.String, java.lang.String)
	 */
	@Override
	public void setConfig(String param, String value) {
		delegate.setConfig(param, value);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#setEx(byte[], long, byte[])
	 */
	@Override
	public Boolean setEx(byte[] key, long seconds, byte[] value) {
		return convertAndReturn(delegate.setEx(key, seconds, value), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#pSetEx(byte[], long, byte[])
	 */
	@Override
	public Boolean pSetEx(byte[] key, long milliseconds, byte[] value) {
		return convertAndReturn(delegate.pSetEx(key, milliseconds, value), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#setNX(byte[], byte[])
	 */
	@Override
	public Boolean setNX(byte[] key, byte[] value) {
		return convertAndReturn(delegate.setNX(key, value), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#setRange(byte[], byte[], long)
	 */
	@Override
	public void setRange(byte[] key, byte[] value, long start) {
		delegate.setRange(key, value, start);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#shutdown()
	 */
	@Override
	public void shutdown() {
		delegate.shutdown();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#shutdown(org.springframework.data.redis.connection.RedisServerCommands.ShutdownOption)
	 */
	@Override
	public void shutdown(ShutdownOption option) {
		delegate.shutdown(option);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sInter(byte[][])
	 */
	@Override
	public Set<byte[]> sInter(byte[]... keys) {
		return convertAndReturn(delegate.sInter(keys), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sInterStore(byte[], byte[][])
	 */
	@Override
	public Long sInterStore(byte[] destKey, byte[]... keys) {
		return convertAndReturn(delegate.sInterStore(destKey, keys), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sIsMember(byte[], byte[])
	 */
	@Override
	public Boolean sIsMember(byte[] key, byte[] value) {
		return convertAndReturn(delegate.sIsMember(key, value), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sMembers(byte[])
	 */
	@Override
	public Set<byte[]> sMembers(byte[] key) {
		return convertAndReturn(delegate.sMembers(key), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sMove(byte[], byte[], byte[])
	 */
	@Override
	public Boolean sMove(byte[] srcKey, byte[] destKey, byte[] value) {
		return convertAndReturn(delegate.sMove(srcKey, destKey, value), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#sort(byte[], org.springframework.data.redis.connection.SortParameters, byte[])
	 */
	@Override
	public Long sort(byte[] key, SortParameters params, byte[] storeKey) {
		return convertAndReturn(delegate.sort(key, params, storeKey), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#sort(byte[], org.springframework.data.redis.connection.SortParameters)
	 */
	@Override
	public List<byte[]> sort(byte[] key, SortParameters params) {
		return convertAndReturn(delegate.sort(key, params), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#encoding(byte[])
	 */
	@Override
	public ValueEncoding encodingOf(byte[] key) {
		return convertAndReturn(delegate.encodingOf(key), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#idletime(byte[])
	 */
	@Override
	public Duration idletime(byte[] key) {
		return convertAndReturn(delegate.idletime(key), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#refcount(byte[])
	 */
	@Override
	public Long refcount(byte[] key) {
		return convertAndReturn(delegate.refcount(key), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sPop(byte[])
	 */
	@Override
	public byte[] sPop(byte[] key) {
		return convertAndReturn(delegate.sPop(key), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sPop(byte[], long)
	 */
	@Override
	public List<byte[]> sPop(byte[] key, long count) {
		return convertAndReturn(delegate.sPop(key, count), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sRandMember(byte[])
	 */
	@Override
	public byte[] sRandMember(byte[] key) {
		return convertAndReturn(delegate.sRandMember(key), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sRandMember(byte[], long)
	 */
	@Override
	public List<byte[]> sRandMember(byte[] key, long count) {
		return convertAndReturn(delegate.sRandMember(key, count), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sRem(byte[], byte[][])
	 */
	@Override
	public Long sRem(byte[] key, byte[]... values) {
		return convertAndReturn(delegate.sRem(key, values), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#strLen(byte[])
	 */
	@Override
	public Long strLen(byte[] key) {
		return convertAndReturn(delegate.strLen(key), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#bitCount(byte[])
	 */
	@Override
	public Long bitCount(byte[] key) {
		return convertAndReturn(delegate.bitCount(key), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#bitCount(byte[], long, long)
	 */
	@Override
	public Long bitCount(byte[] key, long start, long end) {
		return convertAndReturn(delegate.bitCount(key, start, end), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#bitOp(org.springframework.data.redis.connection.RedisStringCommands.BitOperation, byte[], byte[][])
	 */
	@Override
	public Long bitOp(BitOperation op, byte[] destination, byte[]... keys) {
		return convertAndReturn(delegate.bitOp(op, destination, keys), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#bitPos(byte[], boolean, org.springframework.data.domain.Range)
	 */
	@Nullable
	@Override
	public Long bitPos(byte[] key, boolean bit, org.springframework.data.domain.Range<Long> range) {
		return convertAndReturn(delegate.bitPos(key, bit, range), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisPubSubCommands#subscribe(org.springframework.data.redis.connection.MessageListener, byte[][])
	 */
	@Override
	public void subscribe(MessageListener listener, byte[]... channels) {
		delegate.subscribe(listener, channels);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sUnion(byte[][])
	 */
	@Override
	public Set<byte[]> sUnion(byte[]... keys) {
		return convertAndReturn(delegate.sUnion(keys), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sUnionStore(byte[], byte[][])
	 */
	@Override
	public Long sUnionStore(byte[] destKey, byte[]... keys) {
		return convertAndReturn(delegate.sUnionStore(destKey, keys), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#ttl(byte[])
	 */
	@Override
	public Long ttl(byte[] key) {
		return convertAndReturn(delegate.ttl(key), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#ttl(byte[], java.util.concurrent.TimeUnit)
	 */
	@Override
	public Long ttl(byte[] key, TimeUnit timeUnit) {
		return convertAndReturn(delegate.ttl(key, timeUnit), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#type(byte[])
	 */
	@Override
	public DataType type(byte[] key) {
		return convertAndReturn(delegate.type(key), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#touch(byte[][])
	 */
	@Override
	public Long touch(byte[]... keys) {
		return convertAndReturn(delegate.touch(keys), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisTxCommands#unwatch()
	 */
	@Override
	public void unwatch() {
		delegate.unwatch();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisTxCommands#watch(byte[][])
	 */
	@Override
	public void watch(byte[]... keys) {
		delegate.watch(keys);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zAdd(byte[], double, byte[], org.springframework.data.redis.connection.RedisZSetCommands.ZAddArgs)
	 */
	@Override
	public Boolean zAdd(byte[] key, double score, byte[] value, ZAddArgs args) {
		return convertAndReturn(delegate.zAdd(key, score, value, args), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zAdd(byte[], java.util.Set, org.springframework.data.redis.connection.RedisZSetCommands.ZAddArgs)
	 */
	@Override
	public Long zAdd(byte[] key, Set<Tuple> tuples, ZAddArgs args) {
		return convertAndReturn(delegate.zAdd(key, tuples, args), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zCard(byte[])
	 */
	@Override
	public Long zCard(byte[] key) {
		return convertAndReturn(delegate.zCard(key), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zCount(byte[], double, double)
	 */
	@Override
	public Long zCount(byte[] key, double min, double max) {
		return convertAndReturn(delegate.zCount(key, min, max), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zCount(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range)
	 */
	@Override
	public Long zCount(byte[] key, Range range) {
		return convertAndReturn(delegate.zCount(key, range), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zIncrBy(byte[], double, byte[])
	 */
	@Override
	public Double zIncrBy(byte[] key, double increment, byte[] value) {
		return convertAndReturn(delegate.zIncrBy(key, increment, value), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zInterStore(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Aggregate, org.springframework.data.redis.connection.RedisZSetCommands.Weights, byte[][])
	 */
	@Override
	public Long zInterStore(byte[] destKey, Aggregate aggregate, Weights weights, byte[]... sets) {
		return convertAndReturn(delegate.zInterStore(destKey, aggregate, weights, sets), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zInterStore(byte[], byte[][])
	 */
	@Override
	public Long zInterStore(byte[] destKey, byte[]... sets) {
		return convertAndReturn(delegate.zInterStore(destKey, sets), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRange(byte[], long, long)
	 */
	@Override
	public Set<byte[]> zRange(byte[] key, long start, long end) {
		return convertAndReturn(delegate.zRange(key, start, end), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByScore(byte[], double, double, long, long)
	 */
	@Override
	public Set<byte[]> zRangeByScore(byte[] key, double min, double max, long offset, long count) {
		return convertAndReturn(delegate.zRangeByScore(key, min, max, offset, count), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByScore(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range)
	 */
	@Override
	public Set<byte[]> zRangeByScore(byte[] key, Range range) {
		return convertAndReturn(delegate.zRangeByScore(key, range), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByScore(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public Set<byte[]> zRangeByScore(byte[] key, Range range, Limit limit) {
		return convertAndReturn(delegate.zRangeByScore(key, range, limit), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByScoreWithScores(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range)
	 */
	@Override
	public Set<Tuple> zRangeByScoreWithScores(byte[] key, Range range) {
		return convertAndReturn(delegate.zRangeByScoreWithScores(key, range), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByScore(byte[], double, double)
	 */
	@Override
	public Set<byte[]> zRangeByScore(byte[] key, double min, double max) {
		return convertAndReturn(delegate.zRangeByScore(key, min, max), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByScoreWithScores(byte[], double, double, long, long)
	 */
	@Override
	public Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max, long offset, long count) {
		return convertAndReturn(delegate.zRangeByScoreWithScores(key, min, max, offset, count), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByScoreWithScores(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public Set<Tuple> zRangeByScoreWithScores(byte[] key, Range range, Limit limit) {
		return convertAndReturn(delegate.zRangeByScoreWithScores(key, range, limit), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByScoreWithScores(byte[], double, double)
	 */
	@Override
	public Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max) {
		return convertAndReturn(delegate.zRangeByScoreWithScores(key, min, max), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeWithScores(byte[], long, long)
	 */
	@Override
	public Set<Tuple> zRangeWithScores(byte[] key, long start, long end) {
		return convertAndReturn(delegate.zRangeWithScores(key, start, end), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRangeByScore(byte[], double, double, long, long)
	 */
	@Override
	public Set<byte[]> zRevRangeByScore(byte[] key, double min, double max, long offset, long count) {
		return convertAndReturn(delegate.zRevRangeByScore(key, min, max, offset, count), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRangeByScore(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range)
	 */
	@Override
	public Set<byte[]> zRevRangeByScore(byte[] key, Range range) {
		return convertAndReturn(delegate.zRevRangeByScore(key, range), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRangeByScore(byte[], double, double)
	 */
	@Override
	public Set<byte[]> zRevRangeByScore(byte[] key, double min, double max) {
		return convertAndReturn(delegate.zRevRangeByScore(key, min, max), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRangeByScore(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public Set<byte[]> zRevRangeByScore(byte[] key, Range range, Limit limit) {
		return convertAndReturn(delegate.zRevRangeByScore(key, range, limit), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRangeByScoreWithScores(byte[], double, double, long, long)
	 */
	@Override
	public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max, long offset, long count) {
		return convertAndReturn(delegate.zRevRangeByScoreWithScores(key, min, max, offset, count), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRangeByScoreWithScores(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range)
	 */
	@Override
	public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, Range range) {
		return convertAndReturn(delegate.zRevRangeByScoreWithScores(key, range), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRangeByScoreWithScores(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, Range range, Limit limit) {
		return convertAndReturn(delegate.zRevRangeByScoreWithScores(key, range, limit), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRangeByScoreWithScores(byte[], double, double)
	 */
	@Override
	public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max) {
		return convertAndReturn(delegate.zRevRangeByScoreWithScores(key, min, max), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRank(byte[], byte[])
	 */
	@Override
	public Long zRank(byte[] key, byte[] value) {
		return convertAndReturn(delegate.zRank(key, value), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRem(byte[], byte[][])
	 */
	@Override
	public Long zRem(byte[] key, byte[]... values) {
		return convertAndReturn(delegate.zRem(key, values), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRemRange(byte[], long, long)
	 */
	@Override
	public Long zRemRange(byte[] key, long start, long end) {
		return convertAndReturn(delegate.zRemRange(key, start, end), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRemRangeByLex(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range)
	 */
	@Override
	public Long zRemRangeByLex(byte[] key, Range range) {
		return convertAndReturn(delegate.zRemRangeByLex(key, range), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRemRangeByScore(byte[], double, double)
	 */
	@Override
	public Long zRemRangeByScore(byte[] key, double min, double max) {
		return convertAndReturn(delegate.zRemRangeByScore(key, min, max), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRemRangeByScore(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range)
	 */
	@Override
	public Long zRemRangeByScore(byte[] key, Range range) {
		return convertAndReturn(delegate.zRemRangeByScore(key, range), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRange(byte[], long, long)
	 */
	@Override
	public Set<byte[]> zRevRange(byte[] key, long start, long end) {
		return convertAndReturn(delegate.zRevRange(key, start, end), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRangeWithScores(byte[], long, long)
	 */
	@Override
	public Set<Tuple> zRevRangeWithScores(byte[] key, long start, long end) {
		return convertAndReturn(delegate.zRevRangeWithScores(key, start, end), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRank(byte[], byte[])
	 */
	@Override
	public Long zRevRank(byte[] key, byte[] value) {
		return convertAndReturn(delegate.zRevRank(key, value), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zScore(byte[], byte[])
	 */
	@Override
	public Double zScore(byte[] key, byte[] value) {
		return convertAndReturn(delegate.zScore(key, value), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zUnionStore(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Aggregate, org.springframework.data.redis.connection.RedisZSetCommands.Weights, byte[][])
	 */
	@Override
	public Long zUnionStore(byte[] destKey, Aggregate aggregate, Weights weights, byte[]... sets) {
		return convertAndReturn(delegate.zUnionStore(destKey, aggregate, weights, sets), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zUnionStore(byte[], byte[][])
	 */
	public Long zUnionStore(byte[] destKey, byte[]... sets) {
		return convertAndReturn(delegate.zUnionStore(destKey, sets), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#zLexCount(java.lang.String, org.springframework.data.redis.connection.RedisZSetCommands.Range)
	 */
	@Override
	public Long zLexCount(String key, Range range) {
		return delegate.zLexCount(serialize(key), range);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#pExpire(byte[], long)
	 */
	@Override
	public Boolean pExpire(byte[] key, long millis) {
		return convertAndReturn(delegate.pExpire(key, millis), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#pExpireAt(byte[], long)
	 */
	@Override
	public Boolean pExpireAt(byte[] key, long unixTimeInMillis) {
		return convertAndReturn(delegate.pExpireAt(key, unixTimeInMillis), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#pTtl(byte[])
	 */
	@Override
	public Long pTtl(byte[] key) {
		return convertAndReturn(delegate.pTtl(key), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#pTtl(byte[], java.util.concurrent.TimeUnit)
	 */
	@Override
	public Long pTtl(byte[] key, TimeUnit timeUnit) {
		return convertAndReturn(delegate.pTtl(key, timeUnit), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#dump(byte[])
	 */
	@Override
	public byte[] dump(byte[] key) {
		return convertAndReturn(delegate.dump(key), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#restore(byte[], long, byte[], boolean)
	 */
	@Override
	public void restore(byte[] key, long ttlInMillis, byte[] serializedValue, boolean replace) {
		delegate.restore(key, ttlInMillis, serializedValue, replace);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisScriptingCommands#scriptFlush()
	 */
	@Override
	public void scriptFlush() {
		delegate.scriptFlush();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisScriptingCommands#scriptKill()
	 */
	@Override
	public void scriptKill() {
		delegate.scriptKill();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisScriptingCommands#scriptLoad(byte[])
	 */
	@Override
	public String scriptLoad(byte[] script) {
		return convertAndReturn(delegate.scriptLoad(script), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisScriptingCommands#scriptExists(java.lang.String[])
	 */
	@Override
	public List<Boolean> scriptExists(String... scriptSha1) {
		return convertAndReturn(delegate.scriptExists(scriptSha1), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisScriptingCommands#eval(byte[], org.springframework.data.redis.connection.ReturnType, int, byte[][])
	 */
	@Override
	public <T> T eval(byte[] script, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
		return convertAndReturn(delegate.eval(script, returnType, numKeys, keysAndArgs), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisScriptingCommands#evalSha(java.lang.String, org.springframework.data.redis.connection.ReturnType, int, byte[][])
	 */
	@Override
	public <T> T evalSha(String scriptSha1, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
		return convertAndReturn(delegate.evalSha(scriptSha1, returnType, numKeys, keysAndArgs), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisScriptingCommands#evalSha(byte[], org.springframework.data.redis.connection.ReturnType, int, byte[][])
	 */
	@Override
	public <T> T evalSha(byte[] scriptSha1, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
		return convertAndReturn(delegate.evalSha(scriptSha1, returnType, numKeys, keysAndArgs), Converters.identityConverter());
	}

	//
	// String methods
	//

	private byte[] serialize(String data) {
		return serializer.serialize(data);
	}

	@SuppressWarnings("unchecked")
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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#append(java.lang.String, java.lang.String)
	 */
	@Override
	public Long append(String key, String value) {
		return append(serialize(key), serialize(value));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#bLPop(int, java.lang.String[])
	 */
	@Override
	public List<String> bLPop(int timeout, String... keys) {
		return convertAndReturn(delegate.bLPop(timeout, serializeMulti(keys)), byteListToStringList);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#bRPop(int, java.lang.String[])
	 */
	@Override
	public List<String> bRPop(int timeout, String... keys) {
		return convertAndReturn(delegate.bRPop(timeout, serializeMulti(keys)), byteListToStringList);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#bRPopLPush(int, java.lang.String, java.lang.String)
	 */
	@Override
	public String bRPopLPush(int timeout, String srcKey, String dstKey) {
		return convertAndReturn(delegate.bRPopLPush(timeout, serialize(srcKey), serialize(dstKey)), bytesToString);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#copy(java.lang.String, java.lang.String, boolean)
	 */
	@Override
	public Boolean copy(String sourceKey, String targetKey, boolean replace) {
		return copy(serialize(sourceKey), serialize(targetKey), replace);
	}
	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#decr(java.lang.String)
	 */
	@Override
	public Long decr(String key) {
		return decr(serialize(key));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#decrBy(java.lang.String, long)
	 */
	@Override
	public Long decrBy(String key, long value) {
		return decrBy(serialize(key), value);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#del(java.lang.String[])
	 */
	@Override
	public Long del(String... keys) {
		return del(serializeMulti(keys));
	}


	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#unlink(java.lang.String[])
	 */
	@Override
	public Long unlink(String... keys) {
		return unlink(serializeMulti(keys));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#echo(java.lang.String)
	 */
	@Override
	public String echo(String message) {
		return convertAndReturn(delegate.echo(serialize(message)), bytesToString);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#exists(java.lang.String)
	 */
	@Override
	public Boolean exists(String key) {
		return exists(serialize(key));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#expire(java.lang.String, long)
	 */
	@Override
	public Boolean expire(String key, long seconds) {
		return expire(serialize(key), seconds);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#expireAt(java.lang.String, long)
	 */
	@Override
	public Boolean expireAt(String key, long unixTime) {
		return expireAt(serialize(key), unixTime);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#get(java.lang.String)
	 */
	@Override
	public String get(String key) {
		return convertAndReturn(delegate.get(serialize(key)), bytesToString);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#getBit(java.lang.String, long)
	 */
	@Override
	public Boolean getBit(String key, long offset) {
		return getBit(serialize(key), offset);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#getRange(java.lang.String, long, long)
	 */
	@Override
	public String getRange(String key, long start, long end) {
		return convertAndReturn(delegate.getRange(serialize(key), start, end), bytesToString);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#getSet(java.lang.String, java.lang.String)
	 */
	@Override
	public String getSet(String key, String value) {
		return convertAndReturn(delegate.getSet(serialize(key), serialize(value)), bytesToString);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#hDel(java.lang.String, java.lang.String[])
	 */
	@Override
	public Long hDel(String key, String... fields) {
		return hDel(serialize(key), serializeMulti(fields));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#hExists(java.lang.String, java.lang.String)
	 */
	@Override
	public Boolean hExists(String key, String field) {
		return hExists(serialize(key), serialize(field));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#hGet(java.lang.String, java.lang.String)
	 */
	@Override
	public String hGet(String key, String field) {
		return convertAndReturn(delegate.hGet(serialize(key), serialize(field)), bytesToString);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#hGetAll(java.lang.String)
	 */
	@Override
	public Map<String, String> hGetAll(String key) {
		return convertAndReturn(delegate.hGetAll(serialize(key)), byteMapToStringMap);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#hIncrBy(java.lang.String, java.lang.String, long)
	 */
	@Override
	public Long hIncrBy(String key, String field, long delta) {
		return hIncrBy(serialize(key), serialize(field), delta);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#hIncrBy(java.lang.String, java.lang.String, double)
	 */
	@Override
	public Double hIncrBy(String key, String field, double delta) {
		return hIncrBy(serialize(key), serialize(field), delta);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#hKeys(java.lang.String)
	 */
	@Override
	public Set<String> hKeys(String key) {
		return convertAndReturn(delegate.hKeys(serialize(key)), byteSetToStringSet);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#hLen(java.lang.String)
	 */
	@Override
	public Long hLen(String key) {
		return hLen(serialize(key));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#hMGet(java.lang.String, java.lang.String[])
	 */
	@Override
	public List<String> hMGet(String key, String... fields) {
		return convertAndReturn(delegate.hMGet(serialize(key), serializeMulti(fields)), byteListToStringList);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#hMSet(java.lang.String, java.util.Map)
	 */
	@Override
	public void hMSet(String key, Map<String, String> hashes) {
		delegate.hMSet(serialize(key), serialize(hashes));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#hSet(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public Boolean hSet(String key, String field, String value) {
		return hSet(serialize(key), serialize(field), serialize(value));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#hSetNX(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public Boolean hSetNX(String key, String field, String value) {
		return hSetNX(serialize(key), serialize(field), serialize(value));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#hVals(java.lang.String)
	 */
	@Override
	public List<String> hVals(String key) {
		return convertAndReturn(delegate.hVals(serialize(key)), byteListToStringList);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#incr(java.lang.String)
	 */
	@Override
	public Long incr(String key) {
		return incr(serialize(key));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#incrBy(java.lang.String, long)
	 */
	@Override
	public Long incrBy(String key, long value) {
		return incrBy(serialize(key), value);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#incrBy(java.lang.String, double)
	 */
	@Override
	public Double incrBy(String key, double value) {
		return incrBy(serialize(key), value);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#keys(java.lang.String)
	 */
	@Override
	public Collection<String> keys(String pattern) {
		return convertAndReturn(delegate.keys(serialize(pattern)), byteSetToStringSet);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#lIndex(java.lang.String, long)
	 */
	@Override
	public String lIndex(String key, long index) {
		return convertAndReturn(delegate.lIndex(serialize(key), index), bytesToString);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#lInsert(java.lang.String, org.springframework.data.redis.connection.RedisListCommands.Position, java.lang.String, java.lang.String)
	 */
	@Override
	public Long lInsert(String key, Position where, String pivot, String value) {
		return lInsert(serialize(key), where, serialize(pivot), serialize(value));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#lLen(java.lang.String)
	 */
	@Override
	public Long lLen(String key) {
		return lLen(serialize(key));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#lPop(java.lang.String)
	 */
	@Override
	public String lPop(String key) {
		return convertAndReturn(delegate.lPop(serialize(key)), bytesToString);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#lPop(java.lang.String, long)
	 */
	@Override
	public List<String> lPop(String key, long count) {
		return convertAndReturn(delegate.lPop(serialize(key), count), byteListToStringList);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#lPos(java.lang.String, java.lang.String, java.lang.Integer, java.lang.Integer)
	 */
	@Override
	public List<Long> lPos(String key, String element, @Nullable Integer rank, @Nullable Integer count) {
		return lPos(serialize(key), serialize(element), rank, count);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#lPush(java.lang.String, java.lang.String[])
	 */
	@Override
	public Long lPush(String key, String... values) {
		return lPush(serialize(key), serializeMulti(values));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#lPushX(java.lang.String, java.lang.String)
	 */
	@Override
	public Long lPushX(String key, String value) {
		return lPushX(serialize(key), serialize(value));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#lRange(java.lang.String, long, long)
	 */
	@Override
	public List<String> lRange(String key, long start, long end) {
		return convertAndReturn(delegate.lRange(serialize(key), start, end), byteListToStringList);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#lRem(java.lang.String, long, java.lang.String)
	 */
	@Override
	public Long lRem(String key, long count, String value) {
		return lRem(serialize(key), count, serialize(value));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#lSet(java.lang.String, long, java.lang.String)
	 */
	@Override
	public void lSet(String key, long index, String value) {
		delegate.lSet(serialize(key), index, serialize(value));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#lTrim(java.lang.String, long, long)
	 */
	@Override
	public void lTrim(String key, long start, long end) {
		delegate.lTrim(serialize(key), start, end);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#mGet(java.lang.String[])
	 */
	@Override
	public List<String> mGet(String... keys) {
		return convertAndReturn(delegate.mGet(serializeMulti(keys)), byteListToStringList);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#mSetNXString(java.util.Map)
	 */
	@Override
	public Boolean mSetNXString(Map<String, String> tuple) {
		return mSetNX(serialize(tuple));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#mSetString(java.util.Map)
	 */
	@Override
	public Boolean mSetString(Map<String, String> tuple) {
		return mSet(serialize(tuple));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#persist(java.lang.String)
	 */
	@Override
	public Boolean persist(String key) {
		return persist(serialize(key));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#move(java.lang.String, int)
	 */
	@Override
	public Boolean move(String key, int dbIndex) {
		return move(serialize(key), dbIndex);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#pSubscribe(org.springframework.data.redis.connection.MessageListener, java.lang.String[])
	 */
	@Override
	public void pSubscribe(MessageListener listener, String... patterns) {
		delegate.pSubscribe(listener, serializeMulti(patterns));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#publish(java.lang.String, java.lang.String)
	 */
	@Override
	public Long publish(String channel, String message) {
		return publish(serialize(channel), serialize(message));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#rename(java.lang.String, java.lang.String)
	 */
	@Override
	public void rename(String oldName, String newName) {
		delegate.rename(serialize(oldName), serialize(newName));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#renameNX(java.lang.String, java.lang.String)
	 */
	@Override
	public Boolean renameNX(String oldName, String newName) {
		return renameNX(serialize(oldName), serialize(newName));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#rPop(java.lang.String)
	 */
	@Override
	public String rPop(String key) {
		return convertAndReturn(delegate.rPop(serialize(key)), bytesToString);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#rPop(java.lang.String, long)
	 */
	@Override
	public List<String> rPop(String key, long count) {
		return convertAndReturn(delegate.rPop(serialize(key), count), byteListToStringList);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#rPopLPush(java.lang.String, java.lang.String)
	 */
	@Override
	public String rPopLPush(String srcKey, String dstKey) {
		return convertAndReturn(delegate.rPopLPush(serialize(srcKey), serialize(dstKey)), bytesToString);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#rPush(java.lang.String, java.lang.String[])
	 */
	@Override
	public Long rPush(String key, String... values) {
		return rPush(serialize(key), serializeMulti(values));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#rPushX(java.lang.String, java.lang.String)
	 */
	@Override
	public Long rPushX(String key, String value) {
		return rPushX(serialize(key), serialize(value));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#sAdd(java.lang.String, java.lang.String[])
	 */
	@Override
	public Long sAdd(String key, String... values) {
		return sAdd(serialize(key), serializeMulti(values));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#sCard(java.lang.String)
	 */
	@Override
	public Long sCard(String key) {
		return sCard(serialize(key));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#sDiff(java.lang.String[])
	 */
	@Override
	public Set<String> sDiff(String... keys) {
		return convertAndReturn(delegate.sDiff(serializeMulti(keys)), byteSetToStringSet);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#sDiffStore(java.lang.String, java.lang.String[])
	 */
	@Override
	public Long sDiffStore(String destKey, String... keys) {
		return sDiffStore(serialize(destKey), serializeMulti(keys));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#set(java.lang.String, java.lang.String)
	 */
	@Override
	public Boolean set(String key, String value) {
		return set(serialize(key), serialize(value));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#set(java.lang.String, java.lang.String, org.springframework.data.redis.core.types.Expiration, org.springframework.data.redis.connection.RedisStringCommands.SetOptions)
	 */
	@Override
	public Boolean set(String key, String value, Expiration expiration, SetOption option) {
		return set(serialize(key), serialize(value), expiration, option);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#setBit(java.lang.String, long, boolean)
	 */
	@Override
	public Boolean setBit(String key, long offset, boolean value) {
		return setBit(serialize(key), offset, value);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#setEx(java.lang.String, long, java.lang.String)
	 */
	@Override
	public Boolean setEx(String key, long seconds, String value) {
		return setEx(serialize(key), seconds, serialize(value));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#pSetEx(java.lang.String, long, java.lang.String)
	 */
	@Override
	public Boolean pSetEx(String key, long seconds, String value) {
		return pSetEx(serialize(key), seconds, serialize(value));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#setNX(java.lang.String, java.lang.String)
	 */
	@Override
	public Boolean setNX(String key, String value) {
		return setNX(serialize(key), serialize(value));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#setRange(java.lang.String, java.lang.String, long)
	 */
	@Override
	public void setRange(String key, String value, long start) {
		delegate.setRange(serialize(key), serialize(value), start);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#sInter(java.lang.String[])
	 */
	@Override
	public Set<String> sInter(String... keys) {
		return convertAndReturn(delegate.sInter(serializeMulti(keys)), byteSetToStringSet);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#sInterStore(java.lang.String, java.lang.String[])
	 */
	@Override
	public Long sInterStore(String destKey, String... keys) {
		return sInterStore(serialize(destKey), serializeMulti(keys));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#sIsMember(java.lang.String, java.lang.String)
	 */
	@Override
	public Boolean sIsMember(String key, String value) {
		return sIsMember(serialize(key), serialize(value));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#sMembers(java.lang.String)
	 */
	@Override
	public Set<String> sMembers(String key) {
		return convertAndReturn(delegate.sMembers(serialize(key)), byteSetToStringSet);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#sMove(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public Boolean sMove(String srcKey, String destKey, String value) {
		return sMove(serialize(srcKey), serialize(destKey), serialize(value));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#sort(java.lang.String, org.springframework.data.redis.connection.SortParameters, java.lang.String)
	 */
	@Override
	public Long sort(String key, SortParameters params, String storeKey) {
		return sort(serialize(key), params, serialize(storeKey));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#sort(java.lang.String, org.springframework.data.redis.connection.SortParameters)
	 */
	@Override
	public List<String> sort(String key, SortParameters params) {
		return convertAndReturn(delegate.sort(serialize(key), params), byteListToStringList);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#encoding(java.lang.String)
	 */
	@Override
	public ValueEncoding encodingOf(String key) {
		return encodingOf(serialize(key));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#idletime(java.lang.String)
	 */
	@Override
	public Duration idletime(String key) {
		return idletime(serialize(key));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#refcount(java.lang.String)
	 */
	@Override
	public Long refcount(String key) {
		return refcount(serialize(key));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#sPop(java.lang.String)
	 */
	@Override
	public String sPop(String key) {
		return convertAndReturn(delegate.sPop(serialize(key)), bytesToString);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#sPop(java.lang.String, long)
	 */
	@Override
	public List<String> sPop(String key, long count) {
		return convertAndReturn(delegate.sPop(serialize(key), count), byteListToStringList);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#sRandMember(java.lang.String)
	 */
	@Override
	public String sRandMember(String key) {
		return convertAndReturn(delegate.sRandMember(serialize(key)), bytesToString);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#sRandMember(java.lang.String, long)
	 */
	@Override
	public List<String> sRandMember(String key, long count) {
		return convertAndReturn(delegate.sRandMember(serialize(key), count), byteListToStringList);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#sRem(java.lang.String, java.lang.String[])
	 */
	@Override
	public Long sRem(String key, String... values) {
		return sRem(serialize(key), serializeMulti(values));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#strLen(java.lang.String)
	 */
	@Override
	public Long strLen(String key) {
		return strLen(serialize(key));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#bitCount(java.lang.String)
	 */
	@Override
	public Long bitCount(String key) {
		return bitCount(serialize(key));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#bitCount(java.lang.String, long, long)
	 */
	@Override
	public Long bitCount(String key, long start, long end) {
		return bitCount(serialize(key), start, end);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#bitOp(org.springframework.data.redis.connection.RedisStringCommands.BitOperation, java.lang.String, java.lang.String[])
	 */
	@Override
	public Long bitOp(BitOperation op, String destination, String... keys) {
		return bitOp(op, serialize(destination), serializeMulti(keys));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#bitPos(java.lang.String, boolean, org.springframework.data.domain.Range)
	 */
	@Nullable
	@Override
	public Long bitPos(String key, boolean bit, org.springframework.data.domain.Range<Long> range) {
		return bitPos(serialize(key), bit, range);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#subscribe(org.springframework.data.redis.connection.MessageListener, java.lang.String[])
	 */
	@Override
	public void subscribe(MessageListener listener, String... channels) {
		delegate.subscribe(listener, serializeMulti(channels));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#sUnion(java.lang.String[])
	 */
	@Override
	public Set<String> sUnion(String... keys) {
		return convertAndReturn(delegate.sUnion(serializeMulti(keys)), byteSetToStringSet);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#sUnionStore(java.lang.String, java.lang.String[])
	 */
	@Override
	public Long sUnionStore(String destKey, String... keys) {
		return sUnionStore(serialize(destKey), serializeMulti(keys));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#ttl(java.lang.String)
	 */
	@Override
	public Long ttl(String key) {
		return ttl(serialize(key));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#ttl(java.lang.String, java.util.concurrent.TimeUnit)
	 */
	@Override
	public Long ttl(String key, TimeUnit timeUnit) {
		return ttl(serialize(key), timeUnit);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#type(java.lang.String)
	 */
	@Override
	public DataType type(String key) {
		return type(serialize(key));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#touch(java.lang.String[])
	 */
	@Nullable
	@Override
	public Long touch(String... keys) {
		return touch(serializeMulti(keys));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#zAdd(java.lang.String, double, java.lang.String)
	 */
	@Override
	public Boolean zAdd(String key, double score, String value) {
		return zAdd(serialize(key), score, serialize(value));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#zAdd(java.lang.String, double, java.lang.String, org.springframework.data.redis.connection.RedisZSetCommands.ZAddArgs)
	 */
	@Override
	public Boolean zAdd(String key, double score, String value, ZAddArgs args) {
		return zAdd(serialize(key), score, serialize(value), args);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#zAdd(java.lang.String, java.util.Set)
	 */
	@Override
	public Long zAdd(String key, Set<StringTuple> tuples) {
		return zAdd(serialize(key), stringTupleToTuple.convert(tuples));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#zAdd(java.lang.String, java.util.Set, org.springframework.data.redis.connection.RedisZSetCommands.ZAddArgs)
	 */
	@Override
	public Long zAdd(String key, Set<StringTuple> tuples, ZAddArgs args) {
		return zAdd(serialize(key), stringTupleToTuple.convert(tuples), args);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#zCard(java.lang.String)
	 */
	@Override
	public Long zCard(String key) {
		return zCard(serialize(key));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#zCount(java.lang.String, double, double)
	 */
	@Override
	public Long zCount(String key, double min, double max) {
		return zCount(serialize(key), min, max);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zLexCount(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range)
	 */
	@Override
	public Long zLexCount(byte[] key, Range range) {
		return delegate.zLexCount(key, range);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#zIncrBy(java.lang.String, double, java.lang.String)
	 */
	@Override
	public Double zIncrBy(String key, double increment, String value) {
		return zIncrBy(serialize(key), increment, serialize(value));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#zInterStore(java.lang.String, org.springframework.data.redis.connection.RedisZSetCommands.Aggregate, int[], java.lang.String[])
	 */
	@Override
	public Long zInterStore(String destKey, Aggregate aggregate, int[] weights, String... sets) {
		return zInterStore(serialize(destKey), aggregate, weights, serializeMulti(sets));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#zInterStore(java.lang.String, java.lang.String[])
	 */
	@Override
	public Long zInterStore(String destKey, String... sets) {
		return zInterStore(serialize(destKey), serializeMulti(sets));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#zRange(java.lang.String, long, long)
	 */
	@Override
	public Set<String> zRange(String key, long start, long end) {
		return convertAndReturn(delegate.zRange(serialize(key), start, end), byteSetToStringSet);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#zRangeByScore(java.lang.String, double, double, long, long)
	 */
	@Override
	public Set<String> zRangeByScore(String key, double min, double max, long offset, long count) {
		return convertAndReturn(delegate.zRangeByScore(serialize(key), min, max, offset, count), byteSetToStringSet);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#zRangeByScore(java.lang.String, double, double)
	 */
	@Override
	public Set<String> zRangeByScore(String key, double min, double max) {
		return convertAndReturn(delegate.zRangeByScore(serialize(key), min, max), byteSetToStringSet);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#zRangeByScoreWithScores(java.lang.String, double, double, long, long)
	 */
	@Override
	public Set<StringTuple> zRangeByScoreWithScores(String key, double min, double max, long offset, long count) {
		return convertAndReturn(delegate.zRangeByScoreWithScores(serialize(key), min, max, offset, count),
				tupleToStringTuple);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#zRangeByScoreWithScores(java.lang.String, double, double)
	 */
	@Override
	public Set<StringTuple> zRangeByScoreWithScores(String key, double min, double max) {
		return convertAndReturn(delegate.zRangeByScoreWithScores(serialize(key), min, max), tupleToStringTuple);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#zRangeWithScores(java.lang.String, long, long)
	 */
	@Override
	public Set<StringTuple> zRangeWithScores(String key, long start, long end) {
		return convertAndReturn(delegate.zRangeWithScores(serialize(key), start, end), tupleToStringTuple);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#zRank(java.lang.String, java.lang.String)
	 */
	@Override
	public Long zRank(String key, String value) {
		return zRank(serialize(key), serialize(value));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#zRem(java.lang.String, java.lang.String[])
	 */
	@Override
	public Long zRem(String key, String... values) {
		return zRem(serialize(key), serializeMulti(values));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#zRemRange(java.lang.String, long, long)
	 */
	@Override
	public Long zRemRange(String key, long start, long end) {
		return zRemRange(serialize(key), start, end);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#zRemRange(java.lang.String, org.springframework.data.redis.connection.RedisZSetCommands.Range)
	 */
	@Override
	public Long zRemRangeByLex(String key, Range range) {
		return zRemRangeByLex(serialize(key), range);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#zRemRangeByScore(java.lang.String, double, double)
	 */
	@Override
	public Long zRemRangeByScore(String key, double min, double max) {
		return zRemRangeByScore(serialize(key), min, max);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#zRevRange(java.lang.String, long, long)
	 */
	@Override
	public Set<String> zRevRange(String key, long start, long end) {
		return convertAndReturn(delegate.zRevRange(serialize(key), start, end), byteSetToStringSet);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#zRevRangeWithScores(java.lang.String, long, long)
	 */
	@Override
	public Set<StringTuple> zRevRangeWithScores(String key, long start, long end) {
		return convertAndReturn(delegate.zRevRangeWithScores(serialize(key), start, end), tupleToStringTuple);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#zRevRangeByScore(java.lang.String, double, double)
	 */
	@Override
	public Set<String> zRevRangeByScore(String key, double min, double max) {
		return convertAndReturn(delegate.zRevRangeByScore(serialize(key), min, max), byteSetToStringSet);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#zRevRangeByScoreWithScores(java.lang.String, double, double)
	 */
	@Override
	public Set<StringTuple> zRevRangeByScoreWithScores(String key, double min, double max) {
		return convertAndReturn(delegate.zRevRangeByScoreWithScores(serialize(key), min, max), tupleToStringTuple);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#zRevRangeByScore(java.lang.String, double, double, long, long)
	 */
	@Override
	public Set<String> zRevRangeByScore(String key, double min, double max, long offset, long count) {
		return convertAndReturn(delegate.zRevRangeByScore(serialize(key), min, max, offset, count), byteSetToStringSet);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#zRevRangeByScoreWithScores(java.lang.String, double, double, long, long)
	 */
	@Override
	public Set<StringTuple> zRevRangeByScoreWithScores(String key, double min, double max, long offset, long count) {
		return convertAndReturn(delegate.zRevRangeByScoreWithScores(serialize(key), min, max, offset, count),
				tupleToStringTuple);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#zRevRank(java.lang.String, java.lang.String)
	 */
	@Override
	public Long zRevRank(String key, String value) {
		return zRevRank(serialize(key), serialize(value));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#zScore(java.lang.String, java.lang.String)
	 */
	@Override
	public Double zScore(String key, String value) {
		return zScore(serialize(key), serialize(value));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#zUnionStore(java.lang.String, org.springframework.data.redis.connection.RedisZSetCommands.Aggregate, int[], java.lang.String[])
	 */
	@Override
	public Long zUnionStore(String destKey, Aggregate aggregate, int[] weights, String... sets) {
		return zUnionStore(serialize(destKey), aggregate, weights, serializeMulti(sets));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#zUnionStore(java.lang.String, java.lang.String[])
	 */
	@Override
	public Long zUnionStore(String destKey, String... sets) {
		return zUnionStore(serialize(destKey), serializeMulti(sets));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoAdd(byte[], org.springframework.data.geo.Point, byte[])
	 */
	@Override
	public Long geoAdd(byte[] key, Point point, byte[] member) {

		return convertAndReturn(delegate.geoAdd(key, point, member), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoAdd(byte[], org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation)
	 */
	public Long geoAdd(byte[] key, GeoLocation<byte[]> location) {
		return convertAndReturn(delegate.geoAdd(key, location), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#geoAdd(java.lang.String, org.springframework.data.geo.Point, java.lang.String)
	 */
	@Override
	public Long geoAdd(String key, Point point, String member) {
		return geoAdd(serialize(key), point, serialize(member));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#geoAdd(java.lang.String, org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation)
	 */
	@Override
	public Long geoAdd(String key, GeoLocation<String> location) {

		Assert.notNull(location, "Location must not be null!");
		return geoAdd(key, location.getPoint(), location.getName());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoAdd(byte[], java.util.Map)
	 */
	@Override
	public Long geoAdd(byte[] key, Map<byte[], Point> memberCoordinateMap) {
		return convertAndReturn(delegate.geoAdd(key, memberCoordinateMap), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoAdd(byte[], java.lang.Iterable)
	 */
	@Override
	public Long geoAdd(byte[] key, Iterable<GeoLocation<byte[]>> locations) {
		return convertAndReturn(delegate.geoAdd(key, locations), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#geoAdd(java.lang.String, java.util.Map)
	 */
	@Override
	public Long geoAdd(String key, Map<String, Point> memberCoordinateMap) {

		Assert.notNull(memberCoordinateMap, "MemberCoordinateMap must not be null!");

		Map<byte[], Point> byteMap = new HashMap<>();
		for (Entry<String, Point> entry : memberCoordinateMap.entrySet()) {
			byteMap.put(serialize(entry.getKey()), entry.getValue());
		}

		return geoAdd(serialize(key), byteMap);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#geoAdd(java.lang.String, java.lang.Iterable)
	 */
	@Override
	public Long geoAdd(String key, Iterable<GeoLocation<String>> locations) {

		Assert.notNull(locations, "Locations must not be null!");

		Map<byte[], Point> byteMap = new HashMap<>();
		for (GeoLocation<String> location : locations) {
			byteMap.put(serialize(location.getName()), location.getPoint());
		}

		return geoAdd(serialize(key), byteMap);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoDist(byte[], byte[], byte[])
	 */
	@Override
	public Distance geoDist(byte[] key, byte[] member1, byte[] member2) {
		return convertAndReturn(delegate.geoDist(key, member1, member2), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#geoDist(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public Distance geoDist(String key, String member1, String member2) {
		return geoDist(serialize(key), serialize(member1), serialize(member2));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoDist(byte[], byte[], byte[], org.springframework.data.geo.Metric)
	 */
	@Override
	public Distance geoDist(byte[] key, byte[] member1, byte[] member2, Metric metric) {
		return convertAndReturn(delegate.geoDist(key, member1, member2, metric), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#geoDist(java.lang.String, java.lang.String, java.lang.String, org.springframework.data.redis.connection.RedisGeoCommands.DistanceUnit)
	 */
	@Override
	public Distance geoDist(String key, String member1, String member2, Metric metric) {
		return geoDist(serialize(key), serialize(member1), serialize(member2), metric);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoHash(byte[], byte[][])
	 */
	@Override
	public List<String> geoHash(byte[] key, byte[]... members) {
		return convertAndReturn(delegate.geoHash(key, members), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#geoHash(java.lang.String, java.lang.String[])
	 */
	@Override
	public List<String> geoHash(String key, String... members) {
		return convertAndReturn(delegate.geoHash(serialize(key), serializeMulti(members)), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoPos(byte[], byte[][])
	 */
	@Override
	public List<Point> geoPos(byte[] key, byte[]... members) {
		return convertAndReturn(delegate.geoPos(key, members), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#geoPos(java.lang.String, java.lang.String[])
	 */
	@Override
	public List<Point> geoPos(String key, String... members) {
		return geoPos(serialize(key), serializeMulti(members));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#geoRadius(java.lang.String, org.springframework.data.geo.Circle)
	 */
	@Override
	public GeoResults<GeoLocation<String>> geoRadius(String key, Circle within) {
		return convertAndReturn(delegate.geoRadius(serialize(key), within), byteGeoResultsToStringGeoResults);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#geoRadius(java.lang.String, org.springframework.data.geo.Circle, org.springframework.data.redis.core.GeoRadiusCommandArgs)
	 */
	@Override
	public GeoResults<GeoLocation<String>> geoRadius(String key, Circle within, GeoRadiusCommandArgs args) {
		return convertAndReturn(delegate.geoRadius(serialize(key), within, args), byteGeoResultsToStringGeoResults);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#geoRadiusByMember(java.lang.String, java.lang.String, double)
	 */
	@Override
	public GeoResults<GeoLocation<String>> geoRadiusByMember(String key, String member, double radius) {
		return geoRadiusByMember(key, member, new Distance(radius, DistanceUnit.METERS));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#geoRadiusByMember(java.lang.String, java.lang.String, org.springframework.data.geo.Distance)
	 */
	@Override
	public GeoResults<GeoLocation<String>> geoRadiusByMember(String key, String member, Distance radius) {

		return convertAndReturn(delegate.geoRadiusByMember(serialize(key), serialize(member), radius),
				byteGeoResultsToStringGeoResults);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#geoRadiusByMember(java.lang.String, java.lang.String, org.springframework.data.geo.Distance, org.springframework.data.redis.core.GeoRadiusCommandArgs)
	 */
	@Override
	public GeoResults<GeoLocation<String>> geoRadiusByMember(String key, String member, Distance radius,
			GeoRadiusCommandArgs args) {

		return convertAndReturn(delegate.geoRadiusByMember(serialize(key), serialize(member), radius, args),
				byteGeoResultsToStringGeoResults);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoRadius(byte[], org.springframework.data.geo.Circle)
	 */
	@Override
	public GeoResults<GeoLocation<byte[]>> geoRadius(byte[] key, Circle within) {
		return convertAndReturn(delegate.geoRadius(key, within), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoRadius(byte[], org.springframework.data.geo.Circle, org.springframework.data.redis.core.GeoRadiusCommandArgs)
	 */
	@Override
	public GeoResults<GeoLocation<byte[]>> geoRadius(byte[] key, Circle within, GeoRadiusCommandArgs args) {
		return convertAndReturn(delegate.geoRadius(key, within, args), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoRadiusByMember(byte[], byte[], double)
	 */
	@Override
	public GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte[] key, byte[] member, double radius) {
		return geoRadiusByMember(key, member, new Distance(radius, DistanceUnit.METERS));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoRadiusByMember(byte[], byte[], org.springframework.data.geo.Distance)
	 */
	@Override
	public GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte[] key, byte[] member, Distance radius) {
		return convertAndReturn(delegate.geoRadiusByMember(key, member, radius), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoRadiusByMember(byte[], byte[], org.springframework.data.geo.Distance, org.springframework.data.redis.core.GeoRadiusCommandArgs)
	 */
	@Override
	public GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte[] key, byte[] member, Distance radius,
			GeoRadiusCommandArgs args) {

		return convertAndReturn(delegate.geoRadiusByMember(key, member, radius, args), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoRemove(byte[], byte[][])
	 */
	@Override
	public Long geoRemove(byte[] key, byte[]... members) {
		return zRem(key, members);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#geoRemove(java.lang.String, java.lang.String[])
	 */
	@Override
	public Long geoRemove(String key, String... members) {
		return geoRemove(serialize(key), serializeMulti(members));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#closePipeline()
	 */
	@Override
	public List<Object> closePipeline() {

		try {
			return convertResults(delegate.closePipeline(), pipelineConverters);
		} finally {
			pipelineConverters.clear();
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#isPipelined()
	 */
	@Override
	public boolean isPipelined() {
		return delegate.isPipelined();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#openPipeline()
	 */
	@Override
	public void openPipeline() {
		delegate.openPipeline();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#execute(java.lang.String)
	 */
	@Override
	public Object execute(String command) {
		return execute(command, EMPTY_2D_BYTE_ARRAY);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisCommands#execute(java.lang.String, byte[][])
	 */
	@Override
	public Object execute(String command, byte[]... args) {
		return convertAndReturn(delegate.execute(command, args), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#execute(java.lang.String, java.lang.String[])
	 */
	@Override
	public Object execute(String command, String... args) {
		return execute(command, serializeMulti(args));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#pExpire(java.lang.String, long)
	 */
	@Override
	public Boolean pExpire(String key, long millis) {
		return pExpire(serialize(key), millis);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#pExpireAt(java.lang.String, long)
	 */
	@Override
	public Boolean pExpireAt(String key, long unixTimeInMillis) {
		return pExpireAt(serialize(key), unixTimeInMillis);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#pTtl(java.lang.String)
	 */
	@Override
	public Long pTtl(String key) {
		return pTtl(serialize(key));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#pTtl(java.lang.String, java.util.concurrent.TimeUnit)
	 */
	@Override
	public Long pTtl(String key, TimeUnit timeUnit) {
		return pTtl(serialize(key), timeUnit);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#scriptLoad(java.lang.String)
	 */
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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#time()
	 */
	@Override
	public Long time() {
		return convertAndReturn(this.delegate.time(), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#time(TimeUnit)
	 */
	@Override
	public Long time(TimeUnit timeUnit) {
		return convertAndReturn(this.delegate.time(timeUnit), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#getClientList()
	 */
	@Override
	public List<RedisClientInfo> getClientList() {
		return convertAndReturn(this.delegate.getClientList(), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#slaveOf(java.lang.String, int)
	 */
	@Override
	public void slaveOf(String host, int port) {
		this.delegate.slaveOf(host, port);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#slaveOfNoOne()
	 */
	@Override
	public void slaveOfNoOne() {
		this.delegate.slaveOfNoOne();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#scan(org.springframework.data.redis.core.ScanOptions)
	 */
	@Override
	public Cursor<byte[]> scan(ScanOptions options) {
		return this.delegate.scan(options);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zScan(byte[], org.springframework.data.redis.core.ScanOptions)
	 */
	@Override
	public Cursor<Tuple> zScan(byte[] key, ScanOptions options) {
		return this.delegate.zScan(key, options);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#scan(byte[], org.springframework.data.redis.core.ScanOptions)
	 */
	@Override
	public Cursor<byte[]> sScan(byte[] key, ScanOptions options) {
		return this.delegate.sScan(key, options);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hscan(byte[], org.springframework.data.redis.core.ScanOptions)
	 */
	@Override
	public Cursor<Entry<byte[], byte[]>> hScan(byte[] key, ScanOptions options) {
		return this.delegate.hScan(key, options);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#hStrLen(byte[], byte[])
	 */
	@Nullable
	@Override
	public Long hStrLen(byte[] key, byte[] field) {
		return convertAndReturn(delegate.hStrLen(key, field), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#setClientName(java.lang.String)
	 */
	@Override
	public void setClientName(byte[] name) {
		this.delegate.setClientName(name);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#setClientName(java.lang.String)
	 */
	@Override
	public void setClientName(String name) {
		setClientName(this.serializer.serialize(name));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#killClient(byte[])
	 */
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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#hScan(java.lang.String, org.springframework.data.redis.core.ScanOptions)
	 */
	@Override
	public Cursor<Entry<String, String>> hScan(String key, ScanOptions options) {

		return new ConvertingCursor<>(this.delegate.hScan(this.serialize(key), options),
				source -> new Entry<String, String>() {

					@Override
					public String getKey() {
						return bytesToString.convert(source.getKey());
					}

					@Override
					public String getValue() {
						return bytesToString.convert(source.getValue());
					}

					@Override
					public String setValue(String value) {
						throw new UnsupportedOperationException("Cannot set value for entry in cursor");
					}
				});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#hStrLen(java.lang.String, java.lang.String)
	 */
	@Override
	public Long hStrLen(String key, String field) {
		return hStrLen(serialize(key), serialize(field));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#sScan(java.lang.String, org.springframework.data.redis.core.ScanOptions)
	 */
	@Override
	public Cursor<String> sScan(String key, ScanOptions options) {
		return new ConvertingCursor<>(this.delegate.sScan(this.serialize(key), options), bytesToString);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#zScan(java.lang.String, org.springframework.data.redis.core.ScanOptions)
	 */
	@Override
	public Cursor<StringTuple> zScan(String key, ScanOptions options) {
		return new ConvertingCursor<>(delegate.zScan(this.serialize(key), options), new TupleConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#getSentinelConnection()
	 */
	@Override
	public RedisSentinelConnection getSentinelConnection() {
		return delegate.getSentinelConnection();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#zRangeByScore(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public Set<String> zRangeByScore(String key, String min, String max) {
		return convertAndReturn(delegate.zRangeByScore(serialize(key), min, max), byteSetToStringSet);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#zRangeByScore(java.lang.String, java.lang.String, java.lang.String, long, long)
	 */
	@Override
	public Set<String> zRangeByScore(String key, String min, String max, long offset, long count) {
		return convertAndReturn(delegate.zRangeByScore(serialize(key), min, max, offset, count), byteSetToStringSet);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByScore(byte[], java.lang.String, java.lang.String)
	 */
	@Override
	public Set<byte[]> zRangeByScore(byte[] key, String min, String max) {
		return convertAndReturn(delegate.zRangeByScore(key, min, max), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByScore(byte[], java.lang.String, java.lang.String, long, long)
	 */
	@Override
	public Set<byte[]> zRangeByScore(byte[] key, String min, String max, long offset, long count) {
		return convertAndReturn(delegate.zRangeByScore(key, min, max, offset, count), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHyperLogLogCommands#pfAdd(byte[], byte[][])
	 */
	@Override
	public Long pfAdd(byte[] key, byte[]... values) {
		return convertAndReturn(delegate.pfAdd(key, values), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#pfAdd(java.lang.String, java.lang.String[])
	 */
	@Override
	public Long pfAdd(String key, String... values) {
		return pfAdd(serialize(key), serializeMulti(values));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHyperLogLogCommands#pfCount(byte[][])
	 */
	@Override
	public Long pfCount(byte[]... keys) {
		return convertAndReturn(delegate.pfCount(keys), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#pfCount(java.lang.String[])
	 */
	@Override
	public Long pfCount(String... keys) {
		return pfCount(serializeMulti(keys));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHyperLogLogCommands#pfMerge(byte[], byte[][])
	 */
	@Override
	public void pfMerge(byte[] destinationKey, byte[]... sourceKeys) {
		delegate.pfMerge(destinationKey, sourceKeys);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#pfMerge(java.lang.String, java.lang.String[][])
	 */
	@Override
	public void pfMerge(String destinationKey, String... sourceKeys) {
		this.pfMerge(serialize(destinationKey), serializeMulti(sourceKeys));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByLex(byte[])
	 */
	@Override
	public Set<byte[]> zRangeByLex(byte[] key) {
		return convertAndReturn(delegate.zRangeByLex(key), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByLex(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range)
	 */
	@Override
	public Set<byte[]> zRangeByLex(byte[] key, Range range) {
		return convertAndReturn(delegate.zRangeByLex(key, range), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByLex(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public Set<byte[]> zRangeByLex(byte[] key, Range range, Limit limit) {
		return convertAndReturn(delegate.zRangeByLex(key, range, limit), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#zRangeByLex(java.lang.String)
	 */
	@Override
	public Set<String> zRangeByLex(String key) {
		return zRangeByLex(key, Range.unbounded());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#zRangeByLex(java.lang.String, org.springframework.data.redis.connection.RedisZSetCommands.Range)
	 */
	@Override
	public Set<String> zRangeByLex(String key, Range range) {
		return zRangeByLex(key, range, Limit.unlimited());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#zRangeByLex(java.lang.String, org.springframework.data.redis.connection.RedisZSetCommands.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public Set<String> zRangeByLex(String key, Range range, Limit limit) {
		return convertAndReturn(delegate.zRangeByLex(serialize(key), range, limit), byteSetToStringSet);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRangeByLex(java.lang.String, org.springframework.data.redis.connection.RedisZSetCommands.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public Set<byte[]> zRevRangeByLex(byte[] key, Range range, Limit limit) {
		return convertAndReturn(delegate.zRevRangeByLex(key, range, limit), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#zRevRangeByLex(java.lang.String, org.springframework.data.redis.connection.RedisZSetCommands.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public Set<String> zRevRangeByLex(String key, Range range, Limit limit) {
		return convertAndReturn(delegate.zRevRangeByLex(serialize(key), range, limit), byteSetToStringSet);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#migrate(byte[], org.springframework.data.redis.connection.RedisNode, int, org.springframework.data.redis.connection.RedisServerCommands.MigrateOption)
	 */
	@Override
	public void migrate(byte[] key, RedisNode target, int dbIndex, @Nullable MigrateOption option) {
		delegate.migrate(key, target, dbIndex, option);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#migrate(byte[], org.springframework.data.redis.connection.RedisNode, int, org.springframework.data.redis.connection.RedisServerCommands.MigrateOption, long)
	 */
	@Override
	public void migrate(byte[] key, RedisNode target, int dbIndex, @Nullable MigrateOption option, long timeout) {
		delegate.migrate(key, target, dbIndex, option, timeout);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#xAck(java.lang.String, java.lang.String, RecordId[])
	 */
	@Override
	public Long xAck(String key, String group, RecordId... recordIds) {
		return convertAndReturn(delegate.xAck(this.serialize(key), group, recordIds), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#xAdd(StringRecord, XAddOptions)
	 */
	@Override
	public RecordId xAdd(StringRecord record, XAddOptions options) {
		return convertAndReturn(delegate.xAdd(record.serialize(serializer), options), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#xClaimJustId(java.lang.String, java.lang.String, java.lang.String, org.springframework.data.redis.connection.RedisStreamCommands.XClaimOptions)
	 */
	@Override
	public List<RecordId> xClaimJustId(String key, String group, String consumer, XClaimOptions options) {
		return convertAndReturn(delegate.xClaimJustId(serialize(key), group, consumer, options), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#xClaim(java.lang.String, java.lang.String, java.lang.String, org.springframework.data.redis.connection.RedisStreamCommands.XClaimOptions)
	 */
	@Override
	public List<StringRecord> xClaim(String key, String group, String consumer, XClaimOptions options) {
		return convertAndReturn(delegate.xClaim(serialize(key), group, consumer, options),
				listByteMapRecordToStringMapRecordConverter);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#xDel(java.lang.String, java.lang.String[])
	 */
	@Override
	public Long xDel(String key, RecordId... recordIds) {
		return convertAndReturn(delegate.xDel(serialize(key), recordIds), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#xGroupCreate(java.lang.String, org.springframework.data.redis.connection.RedisStreamCommands.ReadOffset, java.lang.String)
	 */
	@Override
	public String xGroupCreate(String key, ReadOffset readOffset, String group) {
		return convertAndReturn(delegate.xGroupCreate(serialize(key), group, readOffset), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#xGroupCreate(java.lang.String, org.springframework.data.redis.connection.RedisStreamCommands.ReadOffset, java.lang.String, boolean)
	 */
	@Override
	public String xGroupCreate(String key, ReadOffset readOffset, String group, boolean mkStream) {
		return convertAndReturn(delegate.xGroupCreate(serialize(key), group, readOffset, mkStream), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#xGroupDelConsumer(java.lang.String, org.springframework.data.redis.connection.RedisStreamCommands.Consumer)
	 */
	@Override
	public Boolean xGroupDelConsumer(String key, Consumer consumer) {
		return convertAndReturn(delegate.xGroupDelConsumer(serialize(key), consumer), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#xGroupDestroy(java.lang.String, java.lang.String)
	 */
	@Override
	public Boolean xGroupDestroy(String key, String group) {
		return convertAndReturn(delegate.xGroupDestroy(serialize(key), group), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#xInfo(java.lang.String)
	 */
	@Override
	public XInfoStream xInfo(String key) {
		return convertAndReturn(delegate.xInfo(serialize(key)), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#xInfoGroups(java.lang.String)
	 */
	@Override
	public XInfoGroups xInfoGroups(String key) {
		return convertAndReturn(delegate.xInfoGroups(serialize(key)), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#xInfoConsumers(java.lang.String, java.lang.String)
	 */
	@Override
	public XInfoConsumers xInfoConsumers(String key, String groupName) {
		return convertAndReturn(delegate.xInfoConsumers(serialize(key), groupName), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#xLen(java.lang.String)
	 */
	@Override
	public Long xLen(String key) {
		return convertAndReturn(delegate.xLen(serialize(key)), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#xPending(java.lang.String, java.lang.String)
	 */
	@Override
	public PendingMessagesSummary xPending(String key, String groupName) {
		return convertAndReturn(delegate.xPending(serialize(key), groupName), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#xPending(java.lang.String, java.lang.String, java.lang.String, org.springframework.data.domain.Range, java.lang.Long)
	 */
	@Override
	public PendingMessages xPending(String key, String groupName, String consumer,
			org.springframework.data.domain.Range<String> range, Long count) {
		return convertAndReturn(delegate.xPending(serialize(key), groupName, consumer, range, count, null), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#xPending(java.lang.String, java.lang.String, java.lang.String, org.springframework.data.domain.Range, java.lang.Long)
	 */
	@Override
	public PendingMessages xPending(String key, String groupName, String consumer,
			org.springframework.data.domain.Range<String> range, Long count, Long idleMilliSeconds) {
		return convertAndReturn(delegate.xPending(serialize(key), groupName, consumer, range, count, idleMilliSeconds),
				Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#xPending(java.lang.String, java.lang.String, org.springframework.data.domain.Range, java.lang.Long)
	 */
	@Override
	public PendingMessages xPending(String key, String groupName, org.springframework.data.domain.Range<String> range,
			Long count) {
		return convertAndReturn(delegate.xPending(serialize(key), groupName, range, count, null), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#xPending(java.lang.String, java.lang.String, org.springframework.data.domain.Range, java.lang.Long, java.lang.Long)
	 */
	@Override
	public PendingMessages xPending(String key, String groupName, org.springframework.data.domain.Range<String> range,
			Long count, Long idleMilliSeconds) {
		return convertAndReturn(delegate.xPending(serialize(key), groupName, range, count, idleMilliSeconds),
				Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#xPending(java.lang.String, org.springframework.data.redis.connection.RedisStreamCommands.XPendingOptions)
	 */
	@Override
	public PendingMessages xPending(String key, String groupName, XPendingOptions options) {
		return convertAndReturn(delegate.xPending(serialize(key), groupName, options), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#xRange(java.lang.String, org.springframework.data.domain.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public List<StringRecord> xRange(String key, org.springframework.data.domain.Range<String> range, Limit limit) {
		return convertAndReturn(delegate.xRange(serialize(key), range, limit), listByteMapRecordToStringMapRecordConverter);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#xReadAsString(org.springframework.data.redis.connection.RedisStreamCommands.StreamReadOptions, org.springframework.data.redis.connection.RedisStreamCommands.StreamOffset[])
	 */
	@Override
	public List<StringRecord> xReadAsString(StreamReadOptions readOptions, StreamOffset<String>... streams) {
		return convertAndReturn(delegate.xRead(readOptions, serialize(streams)),
				listByteMapRecordToStringMapRecordConverter);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#xReadGroupAsString(org.springframework.data.redis.connection.RedisStreamCommands.Consumer, org.springframework.data.redis.connection.RedisStreamCommands.StreamReadOptions, org.springframework.data.redis.connection.RedisStreamCommands.StreamOffset[])
	 */
	@Override
	public List<StringRecord> xReadGroupAsString(Consumer consumer, StreamReadOptions readOptions,
			StreamOffset<String>... streams) {

		return convertAndReturn(delegate.xReadGroup(consumer, readOptions, serialize(streams)),
				listByteMapRecordToStringMapRecordConverter);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#xRevRange(java.lang.String, org.springframework.data.domain.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public List<StringRecord> xRevRange(String key, org.springframework.data.domain.Range<String> range, Limit limit) {

		return convertAndReturn(delegate.xRevRange(serialize(key), range, limit),
				listByteMapRecordToStringMapRecordConverter);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#xTrim(java.lang.String, long)
	 */
	@Override
	public Long xTrim(String key, long count) {
		return xTrim(key, count, false);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#xTrim(java.lang.String, long, boolean)
	 */
	@Override
	public Long xTrim(String key, long count, boolean approximateTrimming) {
		return convertAndReturn(delegate.xTrim(serialize(key), count, approximateTrimming), Converters.identityConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xAck(byte[], java.lang.String, java.lang.String[])
	 */
	@Override
	public Long xAck(byte[] key, String group, RecordId... recordIds) {
		return delegate.xAck(key, group, recordIds);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xAdd(MapRecord, XAddOptions)
	 */
	@Override
	public RecordId xAdd(MapRecord<byte[], byte[], byte[]> record, XAddOptions options) {
		return delegate.xAdd(record, options);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xClaimJustId(byte[], java.lang.String, java.lag.String, org.springframework.data.redis.connection.RedisStreamCommands.XCLaimOptions)
	 */
	@Override
	public List<RecordId> xClaimJustId(byte[] key, String group, String newOwner, XClaimOptions options) {
		return delegate.xClaimJustId(key, group, newOwner, options);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xClaim(byte[], java.lang.String, java.lag.String, org.springframework.data.redis.connection.RedisStreamCommands.XCLaimOptions)
	 */
	@Override
	public List<ByteRecord> xClaim(byte[] key, String group, String newOwner, XClaimOptions options) {
		return delegate.xClaim(key, group, newOwner, options);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xDel(byte[], RecordId)
	 */
	@Override
	public Long xDel(byte[] key, RecordId... recordIds) {
		return delegate.xDel(key, recordIds);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xGroupCreate(byte[], org.springframework.data.redis.connection.RedisStreamCommands.ReadOffset, java.lang.String)
	 */
	@Override
	public String xGroupCreate(byte[] key, String groupName, ReadOffset readOffset) {
		return delegate.xGroupCreate(key, groupName, readOffset);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xGroupCreate(byte[], org.springframework.data.redis.connection.RedisStreamCommands.ReadOffset, java.lang.String, boolean)
	 */
	@Override
	public String xGroupCreate(byte[] key, String groupName, ReadOffset readOffset, boolean mkStream) {
		return delegate.xGroupCreate(key, groupName, readOffset, mkStream);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xGroupDelConsumer(byte[], org.springframework.data.redis.connection.RedisStreamCommands.Consumer)
	 */
	@Override
	public Boolean xGroupDelConsumer(byte[] key, Consumer consumer) {
		return delegate.xGroupDelConsumer(key, consumer);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xGroupDestroy(byte[], java.lang.String)
	 */
	@Override
	public Boolean xGroupDestroy(byte[] key, String groupName) {
		return delegate.xGroupDestroy(key, groupName);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xInfo(byte[])
	 */
	@Override
	public XInfoStream xInfo(byte[] key) {
		return delegate.xInfo(key);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xInfoGroups(byte[])
	 */
	@Override
	public XInfoGroups xInfoGroups(byte[] key) {
		return delegate.xInfoGroups(key);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xInfoConsumers(byte[], java.lang.String)
	 */
	@Override
	public XInfoConsumers xInfoConsumers(byte[] key, String groupName) {
		return delegate.xInfoConsumers(key, groupName);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xLen(byte[])
	 */
	@Override
	public Long xLen(byte[] key) {
		return delegate.xLen(key);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xPending(byte[], java.lang.String)
	 */
	@Override
	public PendingMessagesSummary xPending(byte[] key, String groupName) {
		return delegate.xPending(key, groupName);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xPending(byte[], java.lang.String)
	 */
	@Override
	public PendingMessages xPending(byte[] key, String groupName, XPendingOptions options) {
		return delegate.xPending(key, groupName, options);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xRange(byte[], org.springframework.data.domain.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public List<ByteRecord> xRange(byte[] key, org.springframework.data.domain.Range<String> range, Limit limit) {
		return delegate.xRange(key, range, limit);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xRead(org.springframework.data.redis.connection.RedisStreamCommands.StreamReadOptions, org.springframework.data.redis.connection.RedisStreamCommands.StreamOffset[])
	 */
	@Override
	public List<ByteRecord> xRead(StreamReadOptions readOptions, StreamOffset<byte[]>... streams) {
		return delegate.xRead(readOptions, streams);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xReadGroup(org.springframework.data.redis.connection.RedisStreamCommands.Consumer, org.springframework.data.redis.connection.RedisStreamCommands.StreamReadOptions, org.springframework.data.redis.connection.RedisStreamCommands.StreamOffset[])
	 */
	@Override
	public List<ByteRecord> xReadGroup(Consumer consumer, StreamReadOptions readOptions,
			StreamOffset<byte[]>... streams) {
		return delegate.xReadGroup(consumer, readOptions, streams);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xRevRange(byte[], org.springframework.data.domain.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public List<ByteRecord> xRevRange(byte[] key, org.springframework.data.domain.Range<String> range, Limit limit) {
		return delegate.xRevRange(key, range, limit);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xTrim(byte[], long)
	 */
	@Override
	public Long xTrim(byte[] key, long count) {
		return xTrim(key, count, false);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xTrim(byte[], long, boolean)
	 */
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

	@SuppressWarnings("unchecked")
	@Nullable
	private <T> T convertAndReturn(@Nullable Object value, Converter converter) {

		if (isFutureConversion()) {

			addResultConverter(converter);
			return null;
		}

		if (!(converter instanceof ListConverter) && value instanceof List) {
			return (T) new ListConverter<>(converter).convert((List) value);
		}

		return value == null ? null
				: ObjectUtils.nullSafeEquals(converter, Converters.identityConverter()) ? (T) value : (T) converter.convert(value);
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
			log.warn("Delegate returned an unexpected number of results. Abandoning type conversion.");
			return results;
		}
		List<Object> convertedResults = new ArrayList<>(results.size());
		for (Object result : results) {

			Converter converter = converters.remove();
			convertedResults.add(result == null ? null : converter.convert(result));
		}
		return convertedResults;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#bitfield(byte[], BitfieldCommand)
	 */
	@Override
	public List<Long> bitField(byte[] key, BitFieldSubCommands subCommands) {
		return delegate.bitField(key, subCommands);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.StringRedisConnection#bitfield(byte[], BitfieldCommand)
	 */
	@Override
	public List<Long> bitfield(String key, BitFieldSubCommands operation) {

		List<Long> results = delegate.bitField(serialize(key), operation);
		if (isFutureConversion()) {
			addResultConverter(Converters.identityConverter());
		}
		return results;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.DecoratedRedisConnection#getDelegate()
	 */
	@Override
	public RedisConnection getDelegate() {
		return delegate;
	}
}
