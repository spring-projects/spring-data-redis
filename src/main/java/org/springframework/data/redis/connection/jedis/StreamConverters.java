/*
 * Copyright 2021-2022 the original author or authors.
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

import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XAddParams;
import redis.clients.jedis.params.XClaimParams;
import redis.clients.jedis.params.XPendingParams;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.params.XReadParams;
import redis.clients.jedis.resps.StreamEntry;
import redis.clients.jedis.resps.StreamPendingEntry;
import redis.clients.jedis.util.SafeEncoder;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.RedisStreamCommands;
import org.springframework.data.redis.connection.stream.ByteRecord;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.PendingMessagesSummary;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.connection.stream.StreamRecords;

/**
 * Converters for Redis Stream-specific types.
 * <p>
 * Converters typically convert between value objects/argument objects retaining the actual types of values (i.e. no
 * serialization/deserialization happens here).
 *
 * @author dengliming
 * @author Mark Paluch
 * @since 2.3
 */
class StreamConverters {

	static byte[][] entryIdsToBytes(List<RecordId> recordIds) {

		byte[][] target = new byte[recordIds.size()][];

		for (int i = 0; i < recordIds.size(); ++i) {
			RecordId id = recordIds.get(i);
			target[i] = JedisConverters.toBytes(id.getValue());
		}

		return target;
	}

	static String getLowerValue(Range<String> range) {
		return getValue(range.getLowerBound(), "-");
	}

	static String getUpperValue(Range<String> range) {
		return getValue(range.getUpperBound(), "+");
	}

	private static String getValue(Range.Bound<String> bound, String fallbackValue) {

		if (bound.equals(Range.Bound.unbounded())) {
			return fallbackValue;
		}

		return bound.getValue().map(it -> bound.isInclusive() ? it : "(" + it).orElse(fallbackValue);
	}

	static List<Object> mapToList(Map<String, Object> map) {

		List<Object> sources = new ArrayList<>(map.size() * 2);
		map.forEach((k, v) -> {
			sources.add(k);

			if (v instanceof StreamEntryID) {
				sources.add(v.toString());
			} else if (v instanceof StreamEntry) {
				List<Object> entries = new ArrayList<>(2);
				StreamEntry streamEntry = (StreamEntry) v;
				entries.add(streamEntry.getID().toString());
				entries.add(streamEntry.getFields());
				sources.add(entries);
			} else {
				sources.add(v);
			}
		});
		return sources;
	}

	static Map.Entry<byte[], byte[]>[] toStreamOffsets(StreamOffset<byte[]>[] streams) {
		return Arrays.stream(streams)
				.collect(Collectors.toMap(StreamOffset::getKey, v -> JedisConverters.toBytes(v.getOffset().getOffset())))
				.entrySet().toArray(new Map.Entry[0]);
	}

	static List<ByteRecord> convertToByteRecord(byte[] key, Object source) {

		List<List<Object>> objectList = (List<List<Object>>) source;
		List<ByteRecord> result = new ArrayList<>(objectList.size() / 2);

		if (objectList.isEmpty()) {
			return result;
		}

		for (List<Object> res : objectList) {

			if (res == null) {
				result.add(null);
				continue;
			}

			String entryIdString = SafeEncoder.encode((byte[]) res.get(0));
			List<byte[]> hash = (List<byte[]>) res.get(1);

			Iterator<byte[]> hashIterator = hash.iterator();
			Map<byte[], byte[]> fields = new HashMap<>(hash.size() / 2);
			while (hashIterator.hasNext()) {
				fields.put(hashIterator.next(), hashIterator.next());
			}

			result.add(StreamRecords.newRecord().in(key).withId(entryIdString).ofBytes(fields));
		}
		return result;
	}

	static List<ByteRecord> convertToByteRecords(List<?> sources) {

		List<ByteRecord> result = new ArrayList<>(sources.size() / 2);

		for (Object source : sources) {
			List<Object> stream = (List<Object>) source;
			result.addAll(convertToByteRecord((byte[]) stream.get(0), stream.get(1)));
		}

		return result;
	}

	static PendingMessagesSummary toPendingMessagesSummary(String groupName, Object source) {

		List<Object> objectList = (List<Object>) source;
		long total = BuilderFactory.LONG.build(objectList.get(0));
		Range.Bound<String> lower = objectList.get(1) != null
				? Range.Bound.inclusive(SafeEncoder.encode((byte[]) objectList.get(1)))
				: Range.Bound.unbounded();
		Range.Bound<String> upper = objectList.get(2) != null
				? Range.Bound.inclusive(SafeEncoder.encode((byte[]) objectList.get(2)))
				: Range.Bound.unbounded();
		List<List<Object>> consumerObjList = (List<List<Object>>) objectList.get(3);
		Map<String, Long> map;

		if (consumerObjList != null) {
			map = new HashMap<>(consumerObjList.size());
			for (List<Object> consumerObj : consumerObjList) {
				map.put(SafeEncoder.encode((byte[]) consumerObj.get(0)),
						Long.parseLong(SafeEncoder.encode((byte[]) consumerObj.get(1))));
			}
		} else {
			map = Collections.emptyMap();
		}

		return new PendingMessagesSummary(groupName, total, Range.of(lower, upper), map);
	}

	/**
	 * Convert the raw Jedis xpending result to {@link PendingMessages}.
	 *
	 * @param groupName the group name
	 * @param range the range of messages requested
	 * @param response the raw jedis response.
	 * @return
	 */
	static org.springframework.data.redis.connection.stream.PendingMessages toPendingMessages(String groupName,
			org.springframework.data.domain.Range<?> range, List<StreamPendingEntry> response) {

		List<PendingMessage> messages = response.stream()
				.map(streamPendingEntry -> new PendingMessage(RecordId.of(streamPendingEntry.getID().toString()),
						Consumer.from(groupName, streamPendingEntry.getConsumerName()),
						Duration.ofMillis(streamPendingEntry.getIdleTime()), streamPendingEntry.getDeliveredTimes()))
				.collect(Collectors.toList());

		return new PendingMessages(groupName, messages).withinRange(range);
	}

	public static XAddParams toXAddParams(RecordId recordId, RedisStreamCommands.XAddOptions options) {

		XAddParams params = new XAddParams();
		params.id(toStreamEntryId(recordId.getValue()));

		if (options.hasMaxlen()) {
			params.maxLen(options.getMaxlen());
		}

		if (options.hasMinId()) {
			params.minId(options.getMinId().getValue());
		}

		if (options.isNoMkStream()) {
			params.noMkStream();
		}

		if (options.isApproximateTrimming()) {
			params.approximateTrimming();
		}

		return params;
	}

	private static StreamEntryID toStreamEntryId(String value) {

		if ("*".equals(value)) {
			return StreamEntryID.NEW_ENTRY;
		}

		if ("$".equals(value)) {
			return StreamEntryID.LAST_ENTRY;
		}

		if (">".equals(value)) {
			return StreamEntryID.UNRECEIVED_ENTRY;
		}

		return new StreamEntryID(value);
	}

	public static XClaimParams toXClaimParams(RedisStreamCommands.XClaimOptions options) {

		XClaimParams params = XClaimParams.xClaimParams();

		if (options.isForce()) {
			params.force();
		}

		if (options.getRetryCount() != null) {
			params.retryCount(options.getRetryCount().intValue());
		}

		if (options.getUnixTime() != null) {
			params.time(options.getUnixTime().toEpochMilli());
		}

		return params;
	}

	public static XReadParams toXReadParams(StreamReadOptions readOptions) {

		XReadParams params = XReadParams.xReadParams();

		if (readOptions.isBlocking()) {
			params.block(readOptions.getBlock().intValue());
		}

		if (readOptions.getCount() != null) {
			params.count(readOptions.getCount().intValue());
		}

		return params;
	}

	public static XReadGroupParams toXReadGroupParams(StreamReadOptions readOptions) {

		XReadGroupParams params = XReadGroupParams.xReadGroupParams();

		if (readOptions.isBlocking()) {
			params.block(readOptions.getBlock().intValue());
		}

		if (readOptions.getCount() != null) {
			params.count(readOptions.getCount().intValue());
		}

		if (readOptions.isNoack()) {
			params.noAck();
		}

		return params;

	}

	public static XPendingParams toXPendingParams(RedisStreamCommands.XPendingOptions options) {

		Range<String> range = (Range<String>) options.getRange();
		XPendingParams xPendingParams = XPendingParams.xPendingParams(StreamConverters.getLowerValue(range),
				StreamConverters.getUpperValue(range), options.getCount().intValue());

		if (options.hasConsumer()) {
			xPendingParams.consumer(options.getConsumerName());
		}

		return xPendingParams;
	}

}
