/*
 * Copyright 2013-2022 the original author or authors.
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

import redis.clients.jedis.GeoCoordinate;
import redis.clients.jedis.args.BitOP;
import redis.clients.jedis.args.FlushMode;
import redis.clients.jedis.args.GeoUnit;
import redis.clients.jedis.args.ListPosition;
import redis.clients.jedis.params.GeoRadiusParam;
import redis.clients.jedis.params.GeoSearchParam;
import redis.clients.jedis.params.GetExParams;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.params.SetParams;
import redis.clients.jedis.params.SortingParams;
import redis.clients.jedis.params.ZAddParams;
import redis.clients.jedis.resps.GeoRadiusResponse;
import redis.clients.jedis.util.SafeEncoder;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.domain.Sort;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Metric;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.connection.BitFieldSubCommands.BitFieldIncrBy;
import org.springframework.data.redis.connection.BitFieldSubCommands.BitFieldSet;
import org.springframework.data.redis.connection.BitFieldSubCommands.BitFieldSubCommand;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.connection.RedisGeoCommands.DistanceUnit;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoRadiusCommandArgs;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoRadiusCommandArgs.Flag;
import org.springframework.data.redis.connection.RedisListCommands.Position;
import org.springframework.data.redis.connection.RedisServer;
import org.springframework.data.redis.connection.RedisServerCommands;
import org.springframework.data.redis.connection.RedisStringCommands.BitOperation;
import org.springframework.data.redis.connection.RedisStringCommands.SetOption;
import org.springframework.data.redis.connection.RedisZSetCommands.ZAddArgs;
import org.springframework.data.redis.connection.SortParameters;
import org.springframework.data.redis.connection.SortParameters.Order;
import org.springframework.data.redis.connection.SortParameters.Range;
import org.springframework.data.redis.connection.ValueEncoding;
import org.springframework.data.redis.connection.convert.Converters;
import org.springframework.data.redis.connection.convert.ListConverter;
import org.springframework.data.redis.connection.convert.SetConverter;
import org.springframework.data.redis.connection.convert.StringToRedisClientInfoConverter;
import org.springframework.data.redis.connection.zset.DefaultTuple;
import org.springframework.data.redis.connection.zset.Tuple;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.core.types.RedisClientInfo;
import org.springframework.data.redis.domain.geo.BoundingBox;
import org.springframework.data.redis.domain.geo.BoxShape;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.data.redis.domain.geo.GeoShape;
import org.springframework.data.redis.domain.geo.RadiusShape;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * Jedis type converters.
 *
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Jungtaek Lim
 * @author Mark Paluch
 * @author Ninad Divadkar
 * @author Guy Korland
 * @author dengliming
 */
@SuppressWarnings("ConstantConditions")
abstract class JedisConverters extends Converters {

	public static final byte[] PLUS_BYTES;
	public static final byte[] MINUS_BYTES;
	public static final byte[] POSITIVE_INFINITY_BYTES;
	public static final byte[] NEGATIVE_INFINITY_BYTES;

	static {

		PLUS_BYTES = toBytes("+");
		MINUS_BYTES = toBytes("-");
		POSITIVE_INFINITY_BYTES = toBytes("+inf");
		NEGATIVE_INFINITY_BYTES = toBytes("-inf");
	}

	public static Converter<String, byte[]> stringToBytes() {
		return JedisConverters::toBytes;
	}

	/**
	 * {@link ListConverter} converting jedis {@link redis.clients.jedis.resps.Tuple} to {@link Tuple}.
	 *
	 * @return
	 * @since 1.4
	 */
	static ListConverter<redis.clients.jedis.resps.Tuple, Tuple> tuplesToTuples() {
		return new ListConverter<>(JedisConverters::toTuple);
	}

	static ListConverter<String, byte[]> stringListToByteList() {
		return new ListConverter<>(stringToBytes());
	}

	static Set<Tuple> toTupleSet(Set<redis.clients.jedis.resps.Tuple> source) {
		return new SetConverter<>(JedisConverters::toTuple).convert(source);
	}

	public static Tuple toTuple(redis.clients.jedis.resps.Tuple source) {
		return new DefaultTuple(source.getBinaryElement(), source.getScore());
	}

	/**
	 * Map a {@link Set} of {@link Tuple} by {@code value} to its {@code score}.
	 *
	 * @param tuples must not be {@literal null}.
	 * @return
	 * @since 2.0
	 */
	public static Map<byte[], Double> toTupleMap(Set<Tuple> tuples) {

		Assert.notNull(tuples, "Tuple set must not be null!");

		Map<byte[], Double> args = new LinkedHashMap<>(tuples.size(), 1);

		for (Tuple tuple : tuples) {
			args.put(tuple.getValue(), tuple.getScore());
		}

		return args;
	}

	public static byte[] toBytes(Integer source) {
		return String.valueOf(source).getBytes();
	}

	public static byte[] toBytes(Long source) {
		return String.valueOf(source).getBytes();
	}

	/**
	 * @param source
	 * @return
	 * @since 1.6
	 */
	public static byte[] toBytes(Double source) {
		return toBytes(String.valueOf(source));
	}

	@Nullable
	public static byte[] toBytes(@Nullable String source) {
		return source == null ? null : SafeEncoder.encode(source);
	}

	@Nullable
	public static String toString(@Nullable byte[] source) {
		return source == null ? null : SafeEncoder.encode(source);
	}

	public static Long toLong(byte[] source) {
		return Long.valueOf(toString(source));
	}

	/**
	 * Convert the given {@code source} value to the corresponding {@link ValueEncoding}.
	 *
	 * @param source can be {@literal null}.
	 * @return the {@link ValueEncoding} for given {@code source}. Never {@literal null}.
	 * @since 2.1
	 */
	public static ValueEncoding toEncoding(byte[] source) {
		return ValueEncoding.of(toString(source));
	}

	/**
	 * @param source
	 * @return
	 * @since 1.7
	 */
	@SuppressWarnings("unchecked")
	public static RedisClusterNode toNode(Object source) {

		List<Object> values = (List<Object>) source;
		RedisClusterNode.SlotRange range = new RedisClusterNode.SlotRange(((Number) values.get(0)).intValue(),
				((Number) values.get(1)).intValue());
		List<Object> nodeInfo = (List<Object>) values.get(2);
		return new RedisClusterNode(toString((byte[]) nodeInfo.get(0)), ((Number) nodeInfo.get(1)).intValue(), range);

	}

	/**
	 * @param source
	 * @return
	 * @since 1.3
	 */
	public static List<RedisClientInfo> toListOfRedisClientInformation(String source) {

		if (!StringUtils.hasText(source)) {
			return Collections.emptyList();
		}

		return StringToRedisClientInfoConverter.INSTANCE.convert(source.split("\\r?\\n"));
	}

	/**
	 * @param source
	 * @return
	 * @since 1.4
	 */
	public static List<RedisServer> toListOfRedisServer(List<Map<String, String>> source) {
		return toList(it -> RedisServer.newServerFrom(Converters.toProperties(it)), source);
	}

	public static ListPosition toListPosition(Position source) {
		Assert.notNull(source, "list positions are mandatory");
		return (Position.AFTER.equals(source) ? ListPosition.AFTER : ListPosition.BEFORE);
	}

	public static byte[][] toByteArrays(Map<byte[], byte[]> source) {
		byte[][] result = new byte[source.size() * 2][];
		int index = 0;
		for (Map.Entry<byte[], byte[]> entry : source.entrySet()) {
			result[index++] = entry.getKey();
			result[index++] = entry.getValue();
		}
		return result;
	}

	@Nullable
	public static SortingParams toSortingParams(@Nullable SortParameters params) {

		SortingParams jedisParams = null;
		if (params != null) {
			jedisParams = new SortingParams();
			byte[] byPattern = params.getByPattern();
			if (byPattern != null) {
				jedisParams.by(params.getByPattern());
			}
			byte[][] getPattern = params.getGetPattern();
			if (getPattern != null) {
				jedisParams.get(getPattern);
			}
			Range limit = params.getLimit();
			if (limit != null) {
				jedisParams.limit((int) limit.getStart(), (int) limit.getCount());
			}
			Order order = params.getOrder();
			if (order != null && order.equals(Order.DESC)) {
				jedisParams.desc();
			}
			Boolean isAlpha = params.isAlphabetic();
			if (isAlpha != null && isAlpha) {
				jedisParams.alpha();
			}
		}
		return jedisParams;
	}

	public static BitOP toBitOp(BitOperation bitOp) {
		switch (bitOp) {
			case AND:
				return BitOP.AND;
			case OR:
				return BitOP.OR;
			case NOT:
				return BitOP.NOT;
			case XOR:
				return BitOP.XOR;
			default:
				throw new IllegalArgumentException();
		}
	}

	/**
	 * Converts a given {@link Bound} to its binary representation suitable for {@literal ZRANGEBY*} commands, despite
	 * {@literal ZRANGEBYLEX}.
	 *
	 * @param boundary
	 * @param defaultValue
	 * @return
	 * @since 1.6
	 */
	public static byte[] boundaryToBytesForZRange(@Nullable org.springframework.data.domain.Range.Bound<?> boundary,
			byte[] defaultValue) {

		if (boundary == null || !boundary.isBounded()) {
			return defaultValue;
		}

		return boundaryToBytes(boundary, new byte[] {}, toBytes("("));
	}

	/**
	 * Converts a given {@link Bound} to its binary representation suitable for ZRANGEBYLEX command.
	 *
	 * @param boundary
	 * @return
	 * @since 1.6
	 */
	public static byte[] boundaryToBytesForZRangeByLex(
			@Nullable org.springframework.data.domain.Range.Bound<byte[]> boundary, byte[] defaultValue) {

		if (boundary == null || !boundary.isBounded()) {
			return defaultValue;
		}

		return boundaryToBytes(boundary, toBytes("["), toBytes("("));
	}

	/**
	 * Converts a given {@link Expiration} to the according {@code SET} command argument.
	 * <dl>
	 * <dt>{@link TimeUnit#MILLISECONDS}</dt>
	 * <dd>{@code PX}</dd>
	 * <dt>{@link TimeUnit#SECONDS}</dt>
	 * <dd>{@code EX}</dd>
	 * </dl>
	 *
	 * @param expiration must not be {@literal null}.
	 * @return
	 * @since 2.2
	 */
	public static SetParams toSetCommandExPxArgument(Expiration expiration) {
		return toSetCommandExPxArgument(expiration, SetParams.setParams());
	}

	/**
	 * Converts a given {@link Expiration} to the according {@code SET} command argument.
	 * <dl>
	 * <dt>{@link TimeUnit#MILLISECONDS}</dt>
	 * <dd>{@code PX}</dd>
	 * <dt>{@link TimeUnit#SECONDS}</dt>
	 * <dd>{@code EX}</dd>
	 * </dl>
	 *
	 * @param expiration must not be {@literal null}.
	 * @param params
	 * @return
	 * @since 2.2
	 */
	public static SetParams toSetCommandExPxArgument(Expiration expiration, SetParams params) {

		SetParams paramsToUse = params == null ? SetParams.setParams() : params;

		if (expiration.isKeepTtl()) {
			return paramsToUse.keepttl();
		}

		if (expiration.isPersistent()) {
			return paramsToUse;
		}

		if (expiration.getTimeUnit() == TimeUnit.MILLISECONDS) {
			return expiration.isUnixTimestamp() ? paramsToUse.pxAt(expiration.getExpirationTime())
					: paramsToUse.px(expiration.getExpirationTime());
		}

		return expiration.isUnixTimestamp() ? paramsToUse.exAt(expiration.getConverted(TimeUnit.SECONDS))
				: paramsToUse.ex(expiration.getConverted(TimeUnit.SECONDS));
	}

	/**
	 * Converts a given {@link Expiration} to the according {@code GETEX} command argument depending on
	 * {@link Expiration#isUnixTimestamp()}.
	 * <dl>
	 * <dt>{@link TimeUnit#MILLISECONDS}</dt>
	 * <dd>{@code PX|PXAT}</dd>
	 * <dt>{@link TimeUnit#SECONDS}</dt>
	 * <dd>{@code EX|EXAT}</dd>
	 * </dl>
	 *
	 * @param expiration must not be {@literal null}.
	 * @return
	 * @since 2.6
	 */
	static GetExParams toGetExParams(Expiration expiration) {

		GetExParams params = new GetExParams();

		if (expiration.isPersistent()) {
			return params.persist();
		}

		if (expiration.getTimeUnit() == TimeUnit.MILLISECONDS) {
			if (expiration.isUnixTimestamp()) {
				return params.pxAt(expiration.getExpirationTime());
			}
			return params.px(expiration.getExpirationTime());
		}

		return expiration.isUnixTimestamp() ? params.exAt(expiration.getConverted(TimeUnit.SECONDS))
				: params.ex(expiration.getConverted(TimeUnit.SECONDS));
	}

	/**
	 * Converts a given {@link SetOption} to the according {@code SET} command argument.<br />
	 * <dl>
	 * <dt>{@link SetOption#SET_IF_PRESENT}</dt>
	 * <dd>{@code XX}</dd>
	 * <dt>{@link SetOption#SET_IF_ABSENT}</dt>
	 * <dd>{@code NX}</dd>
	 * <dt>{@link SetOption#UPSERT}</dt>
	 * <dd>{@code byte[0]}</dd>
	 * </dl>
	 *
	 * @param option must not be {@literal null}.
	 * @return
	 * @since 2.2
	 */
	public static SetParams toSetCommandNxXxArgument(SetOption option) {
		return toSetCommandNxXxArgument(option, SetParams.setParams());
	}

	/**
	 * Converts a given {@link SetOption} to the according {@code SET} command argument.<br />
	 * <dl>
	 * <dt>{@link SetOption#SET_IF_PRESENT}</dt>
	 * <dd>{@code XX}</dd>
	 * <dt>{@link SetOption#SET_IF_ABSENT}</dt>
	 * <dd>{@code NX}</dd>
	 * <dt>{@link SetOption#UPSERT}</dt>
	 * <dd>{@code byte[0]}</dd>
	 * </dl>
	 *
	 * @param option must not be {@literal null}.
	 * @param params
	 * @return
	 * @since 2.2
	 */
	public static SetParams toSetCommandNxXxArgument(SetOption option, SetParams params) {

		SetParams paramsToUse = params == null ? SetParams.setParams() : params;

		switch (option) {
			case SET_IF_PRESENT:
				return paramsToUse.xx();
			case SET_IF_ABSENT:
				return paramsToUse.nx();
			default:
				return paramsToUse;
		}
	}

	private static byte[] boundaryToBytes(org.springframework.data.domain.Range.Bound<?> boundary, byte[] inclPrefix,
			byte[] exclPrefix) {

		byte[] prefix = boundary.isInclusive() ? inclPrefix : exclPrefix;
		byte[] value = null;
		Object theValue = boundary.getValue().get();
		if (theValue instanceof byte[]) {
			value = (byte[]) theValue;
		} else if (theValue instanceof Double) {
			value = toBytes((Double) theValue);
		} else if (theValue instanceof Long) {
			value = toBytes((Long) theValue);
		} else if (theValue instanceof Integer) {
			value = toBytes((Integer) theValue);
		} else if (theValue instanceof String) {
			value = toBytes((String) theValue);
		} else {
			throw new IllegalArgumentException(String.format("Cannot convert %s to binary format", boundary.getValue()));
		}

		ByteBuffer buffer = ByteBuffer.allocate(prefix.length + value.length);
		buffer.put(prefix);
		buffer.put(value);
		return buffer.array();

	}

	/**
	 * Convert {@link ScanOptions} to Jedis {@link ScanParams}.
	 *
	 * @param options
	 * @return
	 */
	public static ScanParams toScanParams(ScanOptions options) {

		ScanParams sp = new ScanParams();

		if (!options.equals(ScanOptions.NONE)) {
			if (options.getCount() != null) {
				sp.count(options.getCount().intValue());
			}
			byte[] pattern = options.getBytePattern();
			if (pattern != null) {
				sp.match(pattern);
			}
		}
		return sp;
	}

	static Long toTime(List<String> source, TimeUnit timeUnit) {

		Assert.notEmpty(source, "Received invalid result from server. Expected 2 items in collection.");
		Assert.isTrue(source.size() == 2,
				"Received invalid nr of arguments from redis server. Expected 2 received " + source.size());
		Assert.notNull(timeUnit, "TimeUnit must not be null.");

		return toTimeMillis(source.get(0), source.get(1), timeUnit);
	}

	/**
	 * @param source
	 * @return
	 * @since 1.8
	 */
	public static List<String> toStrings(List<byte[]> source) {
		return toList(JedisConverters::toString, source);
	}

	private static <S, T> List<T> toList(Converter<S, T> converter, @Nullable Collection<S> source) {

		if (source == null || source.isEmpty()) {
			return Collections.emptyList();
		}

		List<T> target = new ArrayList<>(source.size());

		for (S s : source) {
			target.add(converter.convert(s));
		}

		return target;
	}

	/**
	 * @return
	 * @since 1.8
	 */
	public static ListConverter<redis.clients.jedis.GeoCoordinate, Point> geoCoordinateToPointConverter() {
		return new ListConverter<>(JedisConverters::toPoint);
	}

	/**
	 * @return
	 * @since 2.5
	 */
	@Nullable
	static Point toPoint(@Nullable redis.clients.jedis.GeoCoordinate geoCoordinate) {
		return geoCoordinate == null ? null : new Point(geoCoordinate.getLongitude(), geoCoordinate.getLatitude());
	}

	/**
	 * Convert {@link Point} into {@link GeoCoordinate}.
	 *
	 * @param source
	 * @return
	 * @since 1.8
	 */
	public static GeoCoordinate toGeoCoordinate(Point source) {
		return new redis.clients.jedis.GeoCoordinate(source.getX(), source.getY());
	}

	/**
	 * Get a {@link Converter} capable of converting {@link GeoRadiusResponse} into {@link GeoResults}.
	 *
	 * @param metric
	 * @return
	 * @since 1.8
	 */
	public static Converter<List<GeoRadiusResponse>, GeoResults<GeoLocation<byte[]>>> geoRadiusResponseToGeoResultsConverter(
			Metric metric) {
		return GeoResultsConverterFactory.INSTANCE.forMetric(metric);
	}

	/**
	 * Convert {@link Metric} into {@link GeoUnit}.
	 *
	 * @param metric
	 * @return
	 * @since 1.8
	 */
	public static GeoUnit toGeoUnit(Metric metric) {

		Metric metricToUse = metric == null || ObjectUtils.nullSafeEquals(Metrics.NEUTRAL, metric) ? DistanceUnit.METERS
				: metric;
		return ObjectUtils.caseInsensitiveValueOf(GeoUnit.values(), metricToUse.getAbbreviation());
	}

	/**
	 * Convert {@link ZAddArgs} to {@link ZAddParams}.
	 *
	 * @param source must not be {@literal null}.
	 * @return new instance of {@link ZAddParams}.
	 * @since 2.5
	 */
	static ZAddParams toZAddParams(ZAddArgs source) {

		if (!source.isEmpty()) {
			return new ZAddParams();
		}

		ZAddParams target = new ZAddParams() {

			{
				if (source.contains(ZAddArgs.Flag.GT)) {
					addParam("gt");
				}
				if (source.contains(ZAddArgs.Flag.LT)) {
					addParam("lt");
				}
			}
		};

		if (source.contains(ZAddArgs.Flag.XX)) {
			target.xx();
		}
		if (source.contains(ZAddArgs.Flag.NX)) {
			target.nx();
		}
		if (source.contains(ZAddArgs.Flag.CH)) {
			target.ch();
		}
		return target;
	}

	/**
	 * Convert {@link GeoRadiusCommandArgs} into {@link GeoRadiusParam}.
	 *
	 * @param source
	 * @return
	 * @since 1.8
	 */
	public static GeoRadiusParam toGeoRadiusParam(GeoRadiusCommandArgs source) {

		GeoRadiusParam param = GeoRadiusParam.geoRadiusParam();
		if (source == null) {
			return param;
		}

		if (source.hasFlags()) {
			for (Flag flag : source.getFlags()) {
				switch (flag) {
					case WITHCOORD:
						param.withCoord();
						break;
					case WITHDIST:
						param.withDist();
						break;
				}
			}
		}

		if (source.hasSortDirection()) {
			switch (source.getSortDirection()) {
				case ASC:
					param.sortAscending();
					break;
				case DESC:
					param.sortDescending();
					break;
			}
		}

		if (source.hasLimit()) {
			param.count(source.getLimit().intValue());
		}

		return param;
	}

	/**
	 * Convert a timeout to seconds using {@code double} representation including fraction of seconds.
	 *
	 * @param timeout
	 * @param unit
	 * @return
	 * @since 2.6
	 */
	static double toSeconds(long timeout, TimeUnit unit) {

		switch (unit) {
			case MILLISECONDS:
			case MICROSECONDS:
			case NANOSECONDS:
				return unit.toMillis(timeout) / 1000d;
			default:
				return unit.toSeconds(timeout);
		}
	}

	/**
	 * Convert given {@link BitFieldSubCommands} into argument array.
	 *
	 * @param source
	 * @return never {@literal null}.
	 * @since 1.8
	 */
	public static byte[][] toBitfieldCommandArguments(BitFieldSubCommands source) {

		List<byte[]> args = new ArrayList<>(source.getSubCommands().size() * 4);

		for (BitFieldSubCommand command : source.getSubCommands()) {

			if (command instanceof BitFieldIncrBy) {

				BitFieldIncrBy.Overflow overflow = ((BitFieldIncrBy) command).getOverflow();
				if (overflow != null) {
					args.add(JedisConverters.toBytes("OVERFLOW"));
					args.add(JedisConverters.toBytes(overflow.name()));
				}
			}

			args.add(JedisConverters.toBytes(command.getCommand()));
			args.add(JedisConverters.toBytes(command.getType().asString()));
			args.add(JedisConverters.toBytes(command.getOffset().asString()));

			if (command instanceof BitFieldSet) {
				args.add(JedisConverters.toBytes(((BitFieldSet) command).getValue()));
			} else if (command instanceof BitFieldIncrBy) {
				args.add(JedisConverters.toBytes(((BitFieldIncrBy) command).getValue()));
			}
		}

		return args.toArray(new byte[0][0]);
	}

	static FlushMode toFlushMode(@Nullable RedisServerCommands.FlushOption option) {

		if (option == null) {
			return FlushMode.SYNC;
		}

		switch (option) {
			case ASYNC:
				return FlushMode.ASYNC;
			case SYNC:
				return FlushMode.SYNC;
			default:
				throw new IllegalArgumentException("Flush option " + option + " is not supported.");
		}
	}

	static GeoSearchParam toGeoSearchParams(GeoReference<byte[]> reference, GeoShape predicate,
			RedisGeoCommands.GeoCommandArgs args) {

		Assert.notNull(reference, "GeoReference must not be null!");
		Assert.notNull(predicate, "GeoShape must not be null!");
		Assert.notNull(args, "GeoSearchCommandArgs must not be null!");

		GeoSearchParam param = GeoSearchParam.geoSearchParam();

		configureGeoReference(reference, param);

		if (args.getLimit() != null) {

			boolean hasAnyLimit = args.getFlags().contains(Flag.ANY);
			param.count(Math.toIntExact(args.getLimit()), hasAnyLimit);
		}

		if (args.getSortDirection() != null) {

			if (args.getSortDirection() == Sort.Direction.ASC) {
				param.asc();
			} else {
				param.desc();
			}
		}

		if (args.getFlags().contains(Flag.WITHDIST)) {
			param.withDist();
		}

		if (args.getFlags().contains(Flag.WITHCOORD)) {
			param.withCoord();
		}

		return getGeoSearchParam(predicate, param);
	}

	private static GeoSearchParam getGeoSearchParam(GeoShape predicate, GeoSearchParam param) {

		if (predicate instanceof RadiusShape) {

			Distance radius = ((RadiusShape) predicate).getRadius();

			param.byRadius(radius.getValue(), toGeoUnit(radius.getMetric()));

			return param;
		}

		if (predicate instanceof BoxShape) {

			BoxShape boxPredicate = (BoxShape) predicate;
			BoundingBox boundingBox = boxPredicate.getBoundingBox();

			param.byBox(boundingBox.getWidth().getValue(), boundingBox.getHeight().getValue(),
					toGeoUnit(boxPredicate.getMetric()));

			return param;
		}

		throw new IllegalArgumentException(String.format("Cannot convert %s to Jedis GeoSearchParam", predicate));
	}

	private static void configureGeoReference(GeoReference<byte[]> reference, GeoSearchParam param) {

		if (reference instanceof GeoReference.GeoMemberReference) {

			param.fromMember(toString(((GeoReference.GeoMemberReference<byte[]>) reference).getMember()));
			return;
		}

		if (reference instanceof GeoReference.GeoCoordinateReference) {

			GeoReference.GeoCoordinateReference<?> coordinates = (GeoReference.GeoCoordinateReference<?>) reference;
			param.fromLonLat(coordinates.getLongitude(), coordinates.getLatitude());
			return;
		}

		throw new IllegalArgumentException(String.format("Cannot extract Geo Reference from %s", reference));
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.8
	 */
	enum GeoResultsConverterFactory {

		INSTANCE;

		Converter<List<GeoRadiusResponse>, GeoResults<GeoLocation<byte[]>>> forMetric(Metric metric) {
			return new GeoResultsConverter(
					ObjectUtils.nullSafeEquals(Metrics.NEUTRAL, metric) ? DistanceUnit.METERS : metric);
		}

		private static class GeoResultsConverter
				implements Converter<List<GeoRadiusResponse>, GeoResults<GeoLocation<byte[]>>> {

			private final Metric metric;

			public GeoResultsConverter(Metric metric) {
				this.metric = metric;
			}

			@Override
			public GeoResults<GeoLocation<byte[]>> convert(List<GeoRadiusResponse> source) {

				List<GeoResult<GeoLocation<byte[]>>> results = new ArrayList<>(source.size());

				Converter<GeoRadiusResponse, GeoResult<GeoLocation<byte[]>>> converter = GeoResultConverterFactory.INSTANCE
						.forMetric(metric);
				for (GeoRadiusResponse result : source) {
					results.add(converter.convert(result));
				}

				return new GeoResults<>(results, metric);
			}
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.8
	 */
	enum GeoResultConverterFactory {

		INSTANCE;

		Converter<GeoRadiusResponse, GeoResult<GeoLocation<byte[]>>> forMetric(Metric metric) {
			return new GeoResultConverter(metric);
		}

		private static class GeoResultConverter implements Converter<GeoRadiusResponse, GeoResult<GeoLocation<byte[]>>> {

			private final Metric metric;

			public GeoResultConverter(Metric metric) {
				this.metric = metric;
			}

			@Override
			public GeoResult<GeoLocation<byte[]>> convert(GeoRadiusResponse source) {

				Point point = JedisConverters.toPoint(source.getCoordinate());

				return new GeoResult<>(new GeoLocation<>(source.getMember(), point),
						new Distance(source.getDistance(), metric));
			}
		}
	}
}
