/*
 * Copyright 2018-2021 the original author or authors.
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

import org.springframework.data.redis.connection.stream.ByteRecord;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.util.SafeEncoder;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * Converters for Redis Stream-specific types.
 * <p/>
 * Converters typically convert between value objects/argument objects retaining the actual types of values (i.e. no
 * serialization/deserialization happens here).
 *
 * @author dengliming
 * @since 2.3
 */
@SuppressWarnings({ "rawtypes" })
class StreamConverters {

	private static final BiFunction<Object, String, org.springframework.data.redis.connection.stream.PendingMessages> PENDING_MESSAGES_CONVERTER = (
			source, groupName) -> {

		if (null == source) {
			return null;
		}

		List<Object> streamsEntries = (List<Object>) source;
		List<PendingMessage> messages = new ArrayList<>(streamsEntries.size());
		for (Object streamObj : streamsEntries) {
			List<Object> stream = (List<Object>) streamObj;
			String id = SafeEncoder.encode((byte[]) stream.get(0));
			String consumerName = SafeEncoder.encode((byte[]) stream.get(1));
			long idleTime = BuilderFactory.LONG.build(stream.get(2));
			long deliveredTimes = BuilderFactory.LONG.build(stream.get(3));
			messages.add(new PendingMessage(RecordId.of(id), Consumer.from(groupName, consumerName),
					Duration.ofMillis(idleTime), deliveredTimes));
		}
		return new PendingMessages(groupName, messages);
	};

	private static final BiFunction<Object, byte[], List<ByteRecord>> BYTE_RECORD_CONVERTER = (source, key) -> {
		if (null == source) {
			return Collections.emptyList();
		}
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
	};

	static final List<ByteRecord> convertToByteRecord(byte[] key, Object source) {
		return BYTE_RECORD_CONVERTER.apply(source, key);
	}

	static final List<ByteRecord> convertToByteRecord(List<byte[]> sources) {
		if (sources == null) {
			return Collections.emptyList();
		}
		List<ByteRecord> result = new ArrayList<>();
		for (Object streamObj : sources) {
			List<Object> stream = (List<Object>) streamObj;
			result.addAll(convertToByteRecord((byte[]) stream.get(0), stream.get(1)));
		}
		return result;
	}

	/**
	 * Convert the raw Jedis xpending result to {@link PendingMessages}.
	 *
	 * @param groupName the group name
	 * @param range the range of messages requested
	 * @param source the raw jedis response.
	 * @return
	 */
	static org.springframework.data.redis.connection.stream.PendingMessages toPendingMessages(String groupName,
			org.springframework.data.domain.Range<?> range, Object source) {
		return PENDING_MESSAGES_CONVERTER.apply(source, groupName).withinRange(range);
	}
}
