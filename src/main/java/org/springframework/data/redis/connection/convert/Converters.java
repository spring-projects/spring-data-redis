/*
 * Copyright 2013-present the original author or authors.
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
import java.util.*;
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
import org.springframework.data.redis.connection.ClusterSlotHashUtil;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisClusterNode.Flag;
import org.springframework.data.redis.connection.RedisClusterNode.LinkState;
import org.springframework.data.redis.connection.RedisClusterNode.RedisClusterNodeBuilder;
import org.springframework.data.redis.connection.RedisClusterNode.SlotRange;
import org.springframework.data.redis.connection.RedisGeoCommands.DistanceUnit;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation;
import org.springframework.data.redis.connection.RedisNode.NodeType;
import org.springframework.data.redis.connection.zset.Tuple;
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
 * Common type converters.
 *
 * @author Jennifer Hickey
 * @author Thomas Darimont
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author daihuabin
 * @author John Blum
 * @author Sorokin Evgeniy
 * @author Marcin Grzejszczak
 */
public abstract class Converters {

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

		return StringUtils.hasText(clusterNodes)
				? toSetOfRedisClusterNodes(Arrays.asList(clusterNodes.split(CLUSTER_NODES_LINE_SEPARATOR)))
				: Collections.emptySet();
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

		Assert.notNull(targetUnit, "TimeUnit must not be null");

		return seconds > 0 ? targetUnit.convert(seconds, TimeUnit.SECONDS) : seconds;
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

		Assert.notNull(targetUnit, "TimeUnit must not be null");

		return milliseconds > 0 ? targetUnit.convert(milliseconds, TimeUnit.MILLISECONDS) : milliseconds;
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

		Assert.notNull(input, "Input list must not be null");
		Assert.isTrue(input.size() % 2 == 0, "Input list must contain an even number of entries");

		Properties properties = new Properties();

		for (int index = 0; index < input.size(); index += 2) {
			properties.setProperty(input.get(index), input.get(index + 1));
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
	@SuppressWarnings({ "rawtypes", "unchecked" })
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
			LOGGER.debug("parsing %s (%s) as %s".formatted(sourcePath, path, targetType));
		}

		if (targetType == Object.class) {
			return source;
		}

		if (ClassUtils.isAssignable(String.class, targetType)) {
			if (source instanceof String) {
				return source.toString();
			}
			if (source instanceof byte[] bytes) {
				return new String(bytes);
			}
			if (source instanceof ByteBuffer byteBuffer) {
				return new String(ByteUtils.getBytes(byteBuffer));
			}
		}

		if (ClassUtils.isAssignable(List.class, targetType) && source instanceof List<?> sourceCollection) {

			List<Object> targetList = new ArrayList<>();

			for (int i = 0; i < sourceCollection.size(); i++) {
				targetList.add(parse(sourceCollection.get(i), sourcePath + ".[" + i + "]", typeHintMap));
			}

			return targetList;
		}

		if (ClassUtils.isAssignable(Map.class, targetType) && source instanceof List<?> sourceCollection) {

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
	public static <K, V> Map.Entry<K, V> entryOf(@Nullable K key, @Nullable V value) {
		return new AbstractMap.SimpleImmutableEntry<>(key, value);
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

		/**
		 * Support following printf patterns:
		 * <ul>
		 * <li>{@code %s:%i} (Redis 3)</li>
		 * <li>{@code %s:%i@%i} (Redis 4, with bus port)</li>
		 * <li>{@code %s:%i@%i,%s} (Redis 7, with announced hostname)</li> The output of the {@code CLUSTER NODES } command
		 * is just a space-separated CSV string, where each line represents a node in the cluster. The following is an
		 * example of output on Redis 7.2.0. You can check the latest
		 * <a href="https://redis.io/docs/latest/commands/cluster-nodes/">here</a>.
		 * {@code <id> <ip:port@cport[,hostname]> <flags> <master> <ping-sent> <pong-recv> <config-epoch> <link-state> <slot> <slot> ... <slot>}
		 * </ul>
		 */
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

		/**
		 * Value object capturing Redis' representation of a cluster node network coordinate.
		 *
		 * @author Marcin Grzejszczak
		 * @author Mark Paluch
		 */
		record AddressPortHostname(String address, String port, @Nullable String hostname) {

			/**
			 * Parses Redis {@code CLUSTER NODES} host and port segment into {@link AddressPortHostname}.
			 */
			static AddressPortHostname parse(String hostAndPortPart) {

				String[] segments = hostAndPortPart.split(",");
				int portSeparator = segments[0].lastIndexOf(":");
				Assert.isTrue(portSeparator != -1, "ClusterNode information does not define host and port");

				String addressPart = getAddressPart(segments[0].substring(0, portSeparator));
				String portPart = getPortPart(segments[0].substring(portSeparator + 1));
				String hostnamePart = segments.length > 1 ? segments[1] : null;

				return new AddressPortHostname(addressPart, portPart, hostnamePart);
			}

			private static String getAddressPart(String address) {
				return address.startsWith("[") && address.endsWith("]") ? address.substring(1, address.length() - 1) : address;
			}

			private static String getPortPart(String segment) {

				if (segment.contains("@")) {
					return segment.substring(0, segment.indexOf('@'));
				}

				if (segment.contains(":")) {
					return segment.substring(0, segment.indexOf(':'));
				}

				return segment;
			}

			public int portAsInt() {
				return Integer.parseInt(port());
			}

			public boolean hasHostname() {
				return StringUtils.hasText(hostname());
			}

			public String getRequiredHostname() {

				if (StringUtils.hasText(hostname())) {
					return hostname();
				}

				throw new IllegalStateException("Hostname not available");
			}
		}

		@Override
		public RedisClusterNode convert(String source) {

			String[] args = source.split(" ");

			Assert.isTrue(args.length >= MASTER_ID_INDEX + 1,
					() -> "Invalid ClusterNode information, insufficient segments: %s".formatted(source));

			AddressPortHostname endpoint = AddressPortHostname.parse(args[HOST_PORT_INDEX]);

			SlotRange range = parseSlotRange(args);
			Set<Flag> flags = parseFlags(args[FLAGS_INDEX]);

			RedisClusterNodeBuilder nodeBuilder = RedisClusterNode.newRedisClusterNode()
					.listeningAt(endpoint.address(), endpoint.portAsInt()) //
					.withId(args[ID_INDEX]) //
					.promotedAs(flags.contains(Flag.MASTER) ? NodeType.MASTER : NodeType.REPLICA) //
					.serving(range) //
					.withFlags(flags) //
					.linkState(parseLinkState(args));

			if (endpoint.hasHostname()) {
				nodeBuilder.withName(endpoint.getRequiredHostname());
			}

			if (!args[MASTER_ID_INDEX].isEmpty() && !args[MASTER_ID_INDEX].startsWith("-")) {
				nodeBuilder.replicaOf(args[MASTER_ID_INDEX]);
			}

			return nodeBuilder.build();
		}

		private Set<Flag> parseFlags(String source) {

			Set<Flag> flags = new LinkedHashSet<>(8, 1);

			if (StringUtils.hasText(source)) {
				for (String flag : source.split(",")) {
					flags.add(flagLookupMap.get(flag));
				}
			}

			return flags;
		}

		private LinkState parseLinkState(String[] args) {

			String raw = args[LINK_STATE_INDEX];

			return StringUtils.hasText(raw) ? LinkState.valueOf(raw.toUpperCase()) : LinkState.DISCONNECTED;
		}

		private SlotRange parseSlotRange(String[] args) {

			BitSet slots = new BitSet(ClusterSlotHashUtil.SLOT_COUNT);

			for (int index = SLOTS_INDEX; index < args.length; index++) {

				String raw = args[index];

				if (raw.startsWith("[")) {
					continue;
				}

				if (raw.contains("-")) {

					String[] slotRange = StringUtils.split(raw, "-");

					if (slotRange != null) {
						int from = Integer.parseInt(slotRange[0]);
						int to = Integer.parseInt(slotRange[1]);
						for (int slot = from; slot <= to; slot++) {
							slots.set(slot);
						}
					}
				} else {
					slots.set(Integer.parseInt(raw));
				}
			}

			return new SlotRange(slots);
		}
	}
}
