/*
 * Copyright 2013-2021 the original author or authors.
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
package org.springframework.data.redis.connection.convert;

import java.io.StringReader;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Metric;
import org.springframework.data.geo.Metrics;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisClusterNode.Flag;
import org.springframework.data.redis.connection.RedisClusterNode.LinkState;
import org.springframework.data.redis.connection.RedisClusterNode.RedisClusterNodeBuilder;
import org.springframework.data.redis.connection.RedisClusterNode.SlotRange;
import org.springframework.data.redis.connection.RedisGeoCommands.DistanceUnit;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation;
import org.springframework.data.redis.connection.RedisNode.NodeType;
import org.springframework.data.redis.connection.RedisZSetCommands.Tuple;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.util.ByteUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.NumberUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * Common type converters
 *
 * @author Jennifer Hickey
 * @author Thomas Darimont
 * @author Mark Paluch
 * @author Christoph Strobl
 */
abstract public class Converters {

	// private static final Log LOGGER = LogFactory.getLog(Converters.class);
	private static final Log LOGGER = LogFactory.getLog(Converters.class);

	private static final byte[] ONE = new byte[] { '1' };
	private static final byte[] ZERO = new byte[] { '0' };
	private static final String CLUSTER_NODES_LINE_SEPARATOR = "\n";

	/**
	 * Returns a {@link Converter} that always returns its input argument.
	 *
	 * @param <T> the type of the input and output objects to the function
	 * @return a function that always returns its input argument
	 * @since 2.5
	 */
	public static <T> Converter<T, T> identityConverter() {
		return t -> t;
	}

	public static Boolean stringToBoolean(String source) {
		return ObjectUtils.nullSafeEquals("OK", source);
	}

	public static Converter<String, Boolean> stringToBooleanConverter() {
		return Converters::stringToBoolean;
	}

	public static Converter<String, Properties> stringToProps() {
		return Converters::toProperties;
	}

	public static Converter<Long, Boolean> longToBoolean() {
		return Converters::toBoolean;
	}

	public static Converter<String, DataType> stringToDataType() {
		return Converters::toDataType;
	}

	public static Properties toProperties(String source) {
		Properties info = new Properties();
		try (StringReader stringReader = new StringReader(source)) {
			info.load(stringReader);
		} catch (Exception ex) {
			throw new RedisSystemException("Cannot read Redis info", ex);
		}
		return info;
	}

	public static Properties toProperties(Map<?, ?> source) {

		Properties target = new Properties();
		target.putAll(source);
		return target;
	}

	public static Boolean toBoolean(@Nullable Long source) {
		return source != null && source == 1L;
	}

	public static DataType toDataType(String source) {
		return DataType.fromCode(source);
	}

	public static byte[] toBit(Boolean source) {
		return (source ? ONE : ZERO);
	}

	/**
	 * Converts the result of a single line of {@code CLUSTER NODES} into a {@link RedisClusterNode}.
	 *
	 * @param clusterNodesLine
	 * @return
	 * @since 1.7
	 */
	protected static RedisClusterNode toClusterNode(String clusterNodesLine) {
		return ClusterNodesConverter.INSTANCE.convert(clusterNodesLine);
	}

	/**
	 * Converts lines from the result of {@code CLUSTER NODES} into {@link RedisClusterNode}s.
	 *
	 * @param lines
	 * @return
	 * @since 1.7
	 */
	public static Set<RedisClusterNode> toSetOfRedisClusterNodes(Collection<String> lines) {

		if (CollectionUtils.isEmpty(lines)) {
			return Collections.emptySet();
		}

		Set<RedisClusterNode> nodes = new LinkedHashSet<>(lines.size());

		for (String line : lines) {
			nodes.add(toClusterNode(line));
		}

		return nodes;
	}

	/**
	 * Converts the result of {@code CLUSTER NODES} into {@link RedisClusterNode}s.
	 *
	 * @param clusterNodes
	 * @return
	 * @since 1.7
	 */
	public static Set<RedisClusterNode> toSetOfRedisClusterNodes(String clusterNodes) {

		if (!StringUtils.hasText(clusterNodes)) {
			return Collections.emptySet();
		}

		String[] lines = clusterNodes.split(CLUSTER_NODES_LINE_SEPARATOR);
		return toSetOfRedisClusterNodes(Arrays.asList(lines));
	}

	public static List<Object> toObjects(Set<Tuple> tuples) {
		List<Object> tupleArgs = new ArrayList<>(tuples.size() * 2);
		for (Tuple tuple : tuples) {
			tupleArgs.add(tuple.getScore());
			tupleArgs.add(tuple.getValue());
		}
		return tupleArgs;
	}

	/**
	 * Returns the timestamp constructed from the given {@code seconds} and {@code microseconds}.
	 *
	 * @param seconds server time in seconds
	 * @param microseconds elapsed microseconds in current second
	 * @return
	 */
	public static Long toTimeMillis(String seconds, String microseconds) {
		return NumberUtils.parseNumber(seconds, Long.class) * 1000L
				+ NumberUtils.parseNumber(microseconds, Long.class) / 1000L;
	}

	/**
	 * Returns the timestamp constructed from the given {@code seconds} and {@code microseconds}.
	 *
	 * @param seconds server time in seconds.
	 * @param microseconds elapsed microseconds in current second.
	 * @param unit target unit.
	 * @return
	 * @since 2.5
	 */
	public static Long toTimeMillis(String seconds, String microseconds, TimeUnit unit) {

		long secondValue = TimeUnit.SECONDS.toMicros(NumberUtils.parseNumber(seconds, Long.class));
		long microValue = NumberUtils.parseNumber(microseconds, Long.class);

		return unit.convert(secondValue + microValue, TimeUnit.MICROSECONDS);
	}

	/**
	 * Converts {@code seconds} to the given {@link TimeUnit}.
	 *
	 * @param seconds
	 * @param targetUnit must not be {@literal null}.
	 * @return
	 * @since 1.8
	 */
	public static long secondsToTimeUnit(long seconds, TimeUnit targetUnit) {

		Assert.notNull(targetUnit, "TimeUnit must not be null!");

		if (seconds > 0) {
			return targetUnit.convert(seconds, TimeUnit.SECONDS);
		}

		return seconds;
	}

	/**
	 * Creates a new {@link Converter} to convert from seconds to the given {@link TimeUnit}.
	 *
	 * @param timeUnit muist not be {@literal null}.
	 * @return
	 * @since 1.8
	 */
	public static Converter<Long, Long> secondsToTimeUnit(TimeUnit timeUnit) {
		return seconds -> secondsToTimeUnit(seconds, timeUnit);
	}

	/**
	 * Converts {@code milliseconds} to the given {@link TimeUnit}.
	 *
	 * @param milliseconds
	 * @param targetUnit must not be {@literal null}.
	 * @return
	 * @since 1.8
	 */
	public static long millisecondsToTimeUnit(long milliseconds, TimeUnit targetUnit) {

		Assert.notNull(targetUnit, "TimeUnit must not be null!");

		if (milliseconds > 0) {
			return targetUnit.convert(milliseconds, TimeUnit.MILLISECONDS);
		}

		return milliseconds;
	}

	/**
	 * Creates a new {@link Converter} to convert from milliseconds to the given {@link TimeUnit}.
	 *
	 * @param timeUnit must not be {@literal null}.
	 * @return
	 * @since 1.8
	 */
	public static Converter<Long, Long> millisecondsToTimeUnit(TimeUnit timeUnit) {
		return seconds -> millisecondsToTimeUnit(seconds, timeUnit);
	}

	/**
	 * {@link Converter} capable of deserializing {@link GeoResults}.
	 *
	 * @param serializer
	 * @return
	 * @since 1.8
	 */
	public static <V> Converter<GeoResults<GeoLocation<byte[]>>, GeoResults<GeoLocation<V>>> deserializingGeoResultsConverter(
			RedisSerializer<V> serializer) {
		return new DeserializingGeoResultsConverter<>(serializer);
	}

	/**
	 * {@link Converter} capable of converting Double into {@link Distance} using given {@link Metric}.
	 *
	 * @param metric
	 * @return
	 * @since 1.8
	 */
	public static Converter<Double, Distance> distanceConverterForMetric(Metric metric) {
		return DistanceConverterFactory.INSTANCE.forMetric(metric);
	}

	/**
	 * Converts array outputs with key-value sequences (such as produced by {@code CONFIG GET}) from a {@link List} to
	 * {@link Properties}.
	 *
	 * @param input must not be {@literal null}.
	 * @return the mapped result.
	 * @since 2.0
	 */
	public static Properties toProperties(List<String> input) {

		Assert.notNull(input, "Input list must not be null!");
		Assert.isTrue(input.size() % 2 == 0, "Input list must contain an even number of entries!");

		Properties properties = new Properties();

		for (int i = 0; i < input.size(); i += 2) {

			properties.setProperty(input.get(i), input.get(i + 1));
		}

		return properties;
	}

	/**
	 * Returns a converter to convert array outputs with key-value sequences (such as produced by {@code CONFIG GET}) from
	 * a {@link List} to {@link Properties}.
	 *
	 * @return the converter.
	 * @since 2.0
	 */
	public static Converter<List<String>, Properties> listToPropertiesConverter() {
		return Converters::toProperties;
	}

	/**
	 * Returns a converter to convert from {@link Map} to {@link Properties}.
	 *
	 * @return the converter.
	 * @since 2.0
	 */
	@SuppressWarnings("unchecked")
	public static <K, V> Converter<Map<K, V>, Properties> mapToPropertiesConverter() {
		return (Converter) MapToPropertiesConverter.INSTANCE;
	}

	/**
	 * Convert the given {@literal nullable seconds} to a {@link Duration} or {@literal null}.
	 *
	 * @param seconds can be {@literal null}.
	 * @return given {@literal seconds} as {@link Duration} or {@literal null}.
	 * @since 2.1
	 */
	@Nullable
	public static Duration secondsToDuration(@Nullable Long seconds) {
		return seconds != null ? Duration.ofSeconds(seconds) : null;
	}

	/**
	 * Parse a rather generic Redis response, such as a list of something into a meaningful structure applying best effort
	 * conversion of {@code byte[]} and {@link ByteBuffer}.
	 *
	 * @param source the source to parse
	 * @param targetType eg. {@link Map}, {@link String},...
	 * @param <T>
	 * @return
	 * @since 2.3
	 */
	public static <T> T parse(Object source, Class<T> targetType) {
		return targetType.cast(parse(source, "root", Collections.singletonMap("root", targetType)));
	}

	/**
	 * Parse a rather generic Redis response, such as a list of something into a meaningful structure applying best effort
	 * conversion of {@code byte[]} and {@link ByteBuffer} based on the {@literal sourcePath} and a {@literal typeHintMap}
	 *
	 * @param source the source to parse
	 * @param sourcePath the current path (use "root", for level 0).
	 * @param typeHintMap source path to target type hints allowing wildcards ({@literal *}).
	 * @return
	 * @since 2.3
	 */
	public static Object parse(Object source, String sourcePath, Map<String, Class<?>> typeHintMap) {

		String path = sourcePath;
		Class<?> targetType = typeHintMap.get(path);

		if (targetType == null) {

			String alternatePath = sourcePath.contains(".") ? sourcePath.substring(0, sourcePath.lastIndexOf(".")) + ".*"
					: sourcePath;
			targetType = typeHintMap.get(alternatePath);
			if (targetType == null) {

				if (sourcePath.endsWith("[]")) {
					targetType = String.class;
				} else {
					targetType = source.getClass();
				}

			} else {
				if (targetType == Map.class && sourcePath.endsWith("[]")) {
					targetType = String.class;
				} else {
					path = alternatePath;
				}
			}
		}

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug(String.format("parsing %s (%s) as %s.", sourcePath, path, targetType));
		}

		if (targetType == Object.class) {
			return source;
		}

		if (ClassUtils.isAssignable(String.class, targetType)) {

			if (source instanceof String) {
				return source.toString();
			}
			if (source instanceof byte[]) {
				return new String((byte[]) source);
			}
			if (source instanceof ByteBuffer) {
				return new String(ByteUtils.getBytes((ByteBuffer) source));
			}
		}

		if (ClassUtils.isAssignable(List.class, targetType) && source instanceof List) {

			List<Object> sourceCollection = (List<Object>) source;
			List<Object> targetList = new ArrayList<>();
			for (int i = 0; i < sourceCollection.size(); i++) {
				targetList.add(parse(sourceCollection.get(i), sourcePath + ".[" + i + "]", typeHintMap));
			}
			return targetList;
		}

		if (ClassUtils.isAssignable(Map.class, targetType) && source instanceof List) {

			List<Object> sourceCollection = ((List<Object>) source);
			Map<String, Object> targetMap = new LinkedHashMap<>();
			for (int i = 0; i < sourceCollection.size(); i = i + 2) {

				String key = parse(sourceCollection.get(i), path + ".[]", typeHintMap).toString();
				targetMap.put(key, parse(sourceCollection.get(i + 1), path + "." + key, typeHintMap));
			}
			return targetMap;
		}

		return source;
	}

	/**
	 * Create an {@link Map.Entry} from {@code key} and {@code value}.
	 *
	 * @param key
	 * @param value
	 * @param <K>
	 * @param <V>
	 * @return
	 * @since 2.6
	 */
	public static <K, V> Map.Entry<K, V> entryOf(K key, V value) {
		return new SimpleEntry<>(key, value);
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.8
	 */
	enum DistanceConverterFactory {

		INSTANCE;

		/**
		 * @param metric can be {@literal null}. Defaults to {@link DistanceUnit#METERS}.
		 * @return never {@literal null}.
		 */
		DistanceConverter forMetric(@Nullable Metric metric) {
			return new DistanceConverter(ObjectUtils.nullSafeEquals(Metrics.NEUTRAL, metric) ? DistanceUnit.METERS : metric);
		}

		static class DistanceConverter implements Converter<Double, Distance> {

			private final Metric metric;

			/**
			 * @param metric can be {@literal null}. Defaults to {@link DistanceUnit#METERS}.
			 * @return never {@literal null}.
			 */
			DistanceConverter(Metric metric) {
				this.metric = ObjectUtils.nullSafeEquals(Metrics.NEUTRAL, metric) ? DistanceUnit.METERS : metric;
			}

			/*
			 * (non-Javadoc)
			 * @see org.springframework.core.convert.converter.Converter#convert(Object)
			 */
			@Override
			public Distance convert(Double source) {
				return new Distance(source, metric);
			}
		}
	}

	/**
	 * @author Christoph Strobl
	 * @param <V>
	 * @since 1.8
	 */
	static class DeserializingGeoResultsConverter<V>
			implements Converter<GeoResults<GeoLocation<byte[]>>, GeoResults<GeoLocation<V>>> {

		final RedisSerializer<V> serializer;

		public DeserializingGeoResultsConverter(RedisSerializer<V> serializer) {
			this.serializer = serializer;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(Object)
		 */
		@Override
		public GeoResults<GeoLocation<V>> convert(GeoResults<GeoLocation<byte[]>> source) {

			List<GeoResult<GeoLocation<V>>> values = new ArrayList<>(source.getContent().size());
			for (GeoResult<GeoLocation<byte[]>> value : source.getContent()) {

				values.add(new GeoResult<>(
						new GeoLocation<>(serializer.deserialize(value.getContent().getName()), value.getContent().getPoint()),
						value.getDistance()));
			}

			return new GeoResults<>(values, source.getAverageDistance().getMetric());
		}
	}

	enum ClusterNodesConverter implements Converter<String, RedisClusterNode> {

		INSTANCE;

		private static final Map<String, Flag> flagLookupMap;

		static {

			flagLookupMap = new LinkedHashMap<>(Flag.values().length, 1);
			for (Flag flag : Flag.values()) {
				flagLookupMap.put(flag.getRaw(), flag);
			}
		}

		static final int ID_INDEX = 0;
		static final int HOST_PORT_INDEX = 1;
		static final int FLAGS_INDEX = 2;
		static final int MASTER_ID_INDEX = 3;
		static final int LINK_STATE_INDEX = 7;
		static final int SLOTS_INDEX = 8;

		@Override
		public RedisClusterNode convert(String source) {

			String[] args = source.split(" ");
			String[] hostAndPort = StringUtils.split(args[HOST_PORT_INDEX], ":");

			Assert.notNull(hostAndPort, "ClusterNode information does not define host and port!");

			SlotRange range = parseSlotRange(args);
			Set<Flag> flags = parseFlags(args);

			String portPart = hostAndPort[1];
			if (portPart.contains("@")) {
				portPart = portPart.substring(0, portPart.indexOf('@'));
			}

			RedisClusterNodeBuilder nodeBuilder = RedisClusterNode.newRedisClusterNode()
					.listeningAt(hostAndPort[0], Integer.valueOf(portPart)) //
					.withId(args[ID_INDEX]) //
					.promotedAs(flags.contains(Flag.MASTER) ? NodeType.MASTER : NodeType.SLAVE) //
					.serving(range) //
					.withFlags(flags) //
					.linkState(parseLinkState(args));

			if (!args[MASTER_ID_INDEX].isEmpty() && !args[MASTER_ID_INDEX].startsWith("-")) {
				nodeBuilder.slaveOf(args[MASTER_ID_INDEX]);
			}

			return nodeBuilder.build();
		}

		private Set<Flag> parseFlags(String[] args) {

			String raw = args[FLAGS_INDEX];

			Set<Flag> flags = new LinkedHashSet<>(8, 1);
			if (StringUtils.hasText(raw)) {
				for (String flag : raw.split(",")) {
					flags.add(flagLookupMap.get(flag));
				}
			}
			return flags;
		}

		private LinkState parseLinkState(String[] args) {

			String raw = args[LINK_STATE_INDEX];

			if (StringUtils.hasText(raw)) {
				return LinkState.valueOf(raw.toUpperCase());
			}
			return LinkState.DISCONNECTED;
		}

		private SlotRange parseSlotRange(String[] args) {

			Set<Integer> slots = new LinkedHashSet<>();

			for (int i = SLOTS_INDEX; i < args.length; i++) {

				String raw = args[i];

				if (raw.startsWith("[")) {
					continue;
				}

				if (raw.contains("-")) {
					String[] slotRange = StringUtils.split(raw, "-");

					if (slotRange != null) {
						int from = Integer.valueOf(slotRange[0]);
						int to = Integer.valueOf(slotRange[1]);
						for (int slot = from; slot <= to; slot++) {
							slots.add(slot);
						}
					}
				} else {
					slots.add(Integer.valueOf(raw));
				}
			}

			return new SlotRange(slots);
		}

	}

	private static class SimpleEntry<K, V> implements Map.Entry<K, V> {

		private final K key;
		private final V value;

		public SimpleEntry(K key, V value) {
			this.key = key;
			this.value = value;
		}

		@Override
		public K getKey() {
			return key;
		}

		@Override
		public V getValue() {
			return value;
		}

		@Override
		public V setValue(V value) {
			throw new UnsupportedOperationException();
		}
	}
}
