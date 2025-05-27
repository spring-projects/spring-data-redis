/*
 * Copyright 2020-2025 the original author or authors.
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
package org.springframework.data.redis.connection.stream;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import org.jspecify.annotations.Nullable;
import org.springframework.data.redis.connection.convert.Converters;
import org.springframework.data.util.Streamable;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.3
 */
public class StreamInfo {

	public static class XInfoObject {

		protected static final Map<String, Class<?>> DEFAULT_TYPE_HINTS;

		static {

			Map<String, Class<?>> defaults = new HashMap<>(2);
			defaults.put("root", Map.class);
			defaults.put("root.*", String.class);

			DEFAULT_TYPE_HINTS = Collections.unmodifiableMap(defaults);
		}

		private Map<String, Object> raw;

		private XInfoObject(List<Object> raw, Map<String, Class<?>> typeHints) {
			this((Map<String, Object>) Converters.parse(raw, "root", typeHints));
		}

		private XInfoObject(Map<String, Object> raw) {
			this.raw = raw;
		}

		@Nullable
		<T> T get(String entry, Class<T> type) {

			Object value = raw.get(entry);

			return value == null ? null : type.cast(value);
		}

		<T> T getRequired(String entry, Class<T> type) {

			T value = get(entry, type);

			if (value == null) {
				throw new IllegalStateException("Value for entry '%s' is null".formatted(entry));
			}
			return value;
		}

		@Nullable
		<I, T> T getAndMap(String entry, Class<I> type, Function<I, T> f) {

			I value = get(entry, type);

			return value == null ? null : f.apply(value);
		}

		public Map<String, Object> getRaw() {
			return raw;
		}

		@Override
		public String toString() {
			return "XInfoStream" + raw;
		}
	}

	/**
	 * Value object holding general information about a {@literal Redis Stream}.
	 *
	 * @author Christoph Strobl
	 */
	public static class XInfoStream extends XInfoObject {

		private static final Map<String, Class<?>> typeHints;

		static {

			typeHints = new HashMap<>(DEFAULT_TYPE_HINTS);
			typeHints.put("root.first-entry", Map.class);
			typeHints.put("root.first-entry.*", Map.class);
			typeHints.put("root.first-entry.*.*", Object.class);
			typeHints.put("root.last-entry", Map.class);
			typeHints.put("root.last-entry.*", Map.class);
			typeHints.put("root.last-entry.*.*", Object.class);
		}

		private XInfoStream(List<Object> raw) {
			super(raw, typeHints);
		}

		/**
		 * Factory method to create a new instance of {@link XInfoStream}.
		 *
		 * @param source the raw value source.
		 * @return
		 */
		public static XInfoStream fromList(List<Object> source) {
			return new XInfoStream(source);
		}

		/**
		 * Total number of element in the stream. Corresponds to {@literal length}.
		 *
		 * @return
		 */
		public long streamLength() {
			return getRequired("length", Long.class);
		}

		/**
		 * The streams radix tree key size. Corresponds to {@literal radix-tree-keys}.
		 *
		 * @return
		 */
		public long radixTreeKeySize() {
			return getRequired("radix-tree-keys", Long.class);
		}

		/**
		 * Total number of element radix tree nodes. Corresponds to {@literal radix-tree-nodes}.
		 *
		 * @return
		 */
		public long radixTreeNodesSize() {
			return getRequired("radix-tree-nodes", Long.class);
		}

		/**
		 * The number of associated {@literal consumer groups}. Corresponds to {@literal groups}.
		 *
		 * @return
		 */
		public long groupCount() {
			return getRequired("groups", Long.class);
		}

		/**
		 * The last generated id. May not be the same as {@link #lastEntryId()}. Corresponds to
		 * {@literal last-generated-id}.
		 *
		 * @return
		 */
		public String lastGeneratedId() {
			return getRequired("last-generated-id", String.class);
		}

		/**
		 * The id of the streams first entry. Corresponds to {@literal first-entry 1)}.
		 *
		 * @return
		 */
		public @Nullable String firstEntryId() {
			return getAndMap("first-entry", Map.class, it -> it.keySet().iterator().next().toString());
		}

		/**
		 * The streams first entry. Corresponds to {@literal first-entry}.
		 *
		 * @return
		 */
		public @Nullable Map<Object, Object> getFirstEntry() {
			return getAndMap("first-entry", Map.class, Collections::unmodifiableMap);
		}

		/**
		 * The id of the streams last entry. Corresponds to {@literal last-entry 1)}.
		 *
		 * @return
		 */
		public @Nullable String lastEntryId() {
			return getAndMap("last-entry", Map.class, it -> it.keySet().iterator().next().toString());
		}

		/**
		 * The streams first entry. Corresponds to {@literal last-entry}.
		 *
		 * @return
		 */
		public @Nullable Map<Object, Object> getLastEntry() {
			return getAndMap("last-entry", Map.class, Collections::unmodifiableMap);
		}

	}

	/**
	 * Value object holding general information about {@literal consumer groups} associated with a
	 * {@literal Redis Stream}.
	 *
	 * @author Christoph Strobl
	 */
	public static class XInfoGroups implements Streamable<XInfoGroup> {

		private final List<XInfoGroup> groupInfoList;

		private XInfoGroups(List<Object> raw) {

			groupInfoList = new ArrayList<>();
			for (Object entry : raw) {
				groupInfoList.add(new XInfoGroup((List<Object>) entry));
			}
		}

		/**
		 * Factory method to create a new instance of {@link XInfoGroups}.
		 *
		 * @param source the raw value source.
		 * @return
		 */
		public static XInfoGroups fromList(List<Object> source) {
			return new XInfoGroups(source);
		}

		/**
		 * Total number of associated {@literal consumer groups}.
		 *
		 * @return zero if none available.
		 */
		public int groupCount() {
			return size();
		}

		/**
		 * Returns the number of {@link XInfoGroup} available.
		 *
		 * @return zero if none available.
		 * @see #groupCount()
		 */
		public int size() {
			return groupInfoList.size();
		}

		/**
		 * @return {@literal true} if no groups associated.
		 */
		@Override
		public boolean isEmpty() {
			return groupInfoList.isEmpty();
		}

		/**
		 * Returns an iterator over the {@link XInfoGroup} elements.
		 *
		 * @return
		 */
		@Override
		public Iterator<XInfoGroup> iterator() {
			return groupInfoList.iterator();
		}

		/**
		 * Returns the {@link XInfoGroup} element at the given {@literal index}.
		 *
		 * @return the element at the specified position.
		 * @throws IndexOutOfBoundsException if the index is out of range.
		 */
		public XInfoGroup get(int index) {
			return groupInfoList.get(index);
		}

		/**
		 * Returns a sequential {@code Stream} of {@link XInfoGroup}.
		 *
		 * @return
		 */
		@Override
		public Stream<XInfoGroup> stream() {
			return groupInfoList.stream();
		}

		/**
		 * Performs the given {@literal action} on every available {@link XInfoGroup} of this {@link XInfoGroups}.
		 *
		 * @param action
		 */
		@Override
		public void forEach(Consumer<? super XInfoGroup> action) {
			groupInfoList.forEach(action);
		}

		@Override
		public String toString() {
			return "XInfoGroups" + groupInfoList;
		}

	}

	public static class XInfoGroup extends XInfoObject {

		private XInfoGroup(List<Object> raw) {
			super(raw, DEFAULT_TYPE_HINTS);
		}

		public static XInfoGroup fromList(List<Object> raw) {
			return new XInfoGroup(raw);
		}

		/**
		 * The {@literal consumer group} name. Corresponds to {@literal name}.
		 *
		 * @return
		 */
		public String groupName() {
			return getRequired("name", String.class);
		}

		/**
		 * The total number of consumers in the {@literal consumer group}. Corresponds to {@literal consumers}.
		 *
		 * @return
		 */
		public Long consumerCount() {
			return getRequired("consumers", Long.class);
		}

		/**
		 * The total number of pending messages in the {@literal consumer group}. Corresponds to {@literal pending}.
		 *
		 * @return
		 */
		public Long pendingCount() {
			return getRequired("pending", Long.class);
		}

		/**
		 * The id of the last delivered message. Corresponds to {@literal last-delivered-id}.
		 *
		 * @return
		 */
		public String lastDeliveredId() {
			return getRequired("last-delivered-id", String.class);
		}
	}

	public static class XInfoConsumers implements Streamable<XInfoConsumer> {

		private final List<XInfoConsumer> consumerInfoList;

		public XInfoConsumers(String groupName, List<Object> raw) {

			consumerInfoList = new ArrayList<>();
			for (Object entry : raw) {
				consumerInfoList.add(new XInfoConsumer(groupName, (List<Object>) entry));
			}
		}

		public static XInfoConsumers fromList(String groupName, List<Object> source) {
			return new XInfoConsumers(groupName, source);
		}

		/**
		 * Total number of {@literal consumers} in the {@literal consumer group}.
		 *
		 * @return zero if none available.
		 */
		public int getConsumerCount() {
			return consumerInfoList.size();
		}

		/**
		 * Returns the number of {@link XInfoConsumer} available.
		 *
		 * @return zero if none available.
		 * @see #getConsumerCount()
		 */
		public int size() {
			return consumerInfoList.size();
		}

		/**
		 * @return {@literal true} if no groups associated.
		 */
		@Override
		public boolean isEmpty() {
			return consumerInfoList.isEmpty();
		}

		/**
		 * Returns an iterator over the {@link XInfoConsumer} elements.
		 *
		 * @return
		 */
		@Override
		public Iterator<XInfoConsumer> iterator() {
			return consumerInfoList.iterator();
		}

		/**
		 * Returns the {@link XInfoConsumer} element at the given {@literal index}.
		 *
		 * @return the element at the specified position.
		 * @throws IndexOutOfBoundsException if the index is out of range.
		 */
		public XInfoConsumer get(int index) {
			return consumerInfoList.get(index);
		}

		/**
		 * Returns a sequential {@code Stream} of {@link XInfoConsumer}.
		 *
		 * @return
		 */
		@Override
		public Stream<XInfoConsumer> stream() {
			return consumerInfoList.stream();
		}

		/**
		 * Performs the given {@literal action} on every available {@link XInfoConsumer} of this {@link XInfoConsumers}.
		 *
		 * @param action
		 */
		@Override
		public void forEach(Consumer<? super XInfoConsumer> action) {
			consumerInfoList.forEach(action);
		}

		@Override
		public String toString() {
			return "XInfoConsumers" + consumerInfoList;
		}
	}

	public static class XInfoConsumer extends XInfoObject {

		private final String groupName;

		public XInfoConsumer(String groupName, List<Object> raw) {

			super(raw, DEFAULT_TYPE_HINTS);
			this.groupName = groupName;
		}

		/**
		 * The {@literal consumer group} name.
		 *
		 * @return
		 */
		public String groupName() {
			return groupName;
		}

		/**
		 * The {@literal consumer} name. Corresponds to {@literal name}.
		 *
		 * @return
		 */
		public String consumerName() {
			return getRequired("name", String.class);
		}

		/**
		 * The idle time (in millis). Corresponds to {@literal idle}.
		 *
		 * @return
		 */
		public long idleTimeMs() {
			return getRequired("idle", Long.class);
		}

		/**
		 * The idle time. Corresponds to {@literal idle}.
		 *
		 * @return
		 */
		public Duration idleTime() {
			return Duration.ofMillis(idleTimeMs());
		}

		/**
		 * The number of pending messages. Corresponds to {@literal pending}.
		 *
		 * @return
		 */
		public long pendingCount() {
			return getRequired("pending", Long.class);
		}
	}
}
