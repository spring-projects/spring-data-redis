/*
 * Copyright 2018 the original author or authors.
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

import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;

import java.util.List;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.redis.connection.RedisStreamCommands;
import org.springframework.data.redis.connection.RedisStreamCommands.StreamReadOptions;
import org.springframework.data.redis.connection.convert.ListConverter;

/**
 * Converters for Redis Stream-specific types.
 * <p/>
 * Converters typically convert between value objects/argument objects retaining the actual types of values (i.e. no
 * serialization/deserialization happens here).
 *
 * @author Mark Paluch
 * @since 2.2
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
class StreamConverters {

	private static final ListConverter<StreamMessage<Object, Object>, RedisStreamCommands.StreamMessage<Object, Object>> STREAM_LIST_CONVERTER = new ListConverter<>(
			StreamMessageConverter.INSTANCE);

	/**
	 * Convert Lettuce's {@link StreamMessage} to {@link RedisStreamCommands.StreamMessage}.
	 *
	 * @param source must not be {@literal null}.
	 * @return the converted {@link RedisStreamCommands.StreamMessage}.
	 */
	static <K, V> RedisStreamCommands.StreamMessage<K, V> toStreamMessage(StreamMessage<K, V> source) {
		return StreamMessageConverter.INSTANCE.convert((StreamMessage) source);
	}

	/**
	 * Convert Lettuce's {@link List} of {@link StreamMessage} to {@link RedisStreamCommands.StreamMessage}s.
	 *
	 * @param source must not be {@literal null}.
	 * @return the converted {@link List}.
	 */
	static <K, V> List<RedisStreamCommands.StreamMessage<K, V>> toStreamMessages(List<StreamMessage<K, V>> source) {
		return (List) STREAM_LIST_CONVERTER.convert((List) source);
	}

	/**
	 * Convert {@link StreamReadOptions} to Lettuce's {@link XReadArgs}.
	 *
	 * @param readOptions must not be {@literal null}.
	 * @return the converted {@link XReadArgs}.
	 */
	static XReadArgs toReadArgs(StreamReadOptions readOptions) {
		return StreamReadOptionsToXReadArgsConverter.INSTANCE.convert(readOptions);
	}

	/**
	 * @return {@link Converter} to convert Lettuce {@link StreamMessage} into
	 *         {@link org.springframework.data.redis.connection.RedisStreamCommands.StreamMessage}.
	 */
	public static <K, V> Converter<StreamMessage<K, V>, RedisStreamCommands.StreamMessage<K, V>> streamMessageConverter() {
		return (Converter) StreamMessageConverter.INSTANCE;
	}

	/**
	 * @return {@link Converter} to convert a{@link List} of Lettuce {@link StreamMessage}s into a {@link List} of
	 *         {@link org.springframework.data.redis.connection.RedisStreamCommands.StreamMessage}.
	 */
	public static <K, V> Converter<List<StreamMessage<K, V>>, List<RedisStreamCommands.StreamMessage<K, V>>> streamMessageListConverter() {
		return (Converter) STREAM_LIST_CONVERTER;
	}

	/**
	 * {@link Converter} to convert Lettuce's {@link StreamMessage} to {@link RedisStreamCommands.StreamMessage}.
	 */
	enum StreamMessageConverter
			implements Converter<StreamMessage<Object, Object>, RedisStreamCommands.StreamMessage<Object, Object>> {
		INSTANCE;

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		public RedisStreamCommands.StreamMessage<Object, Object> convert(StreamMessage<Object, Object> source) {
			return new RedisStreamCommands.StreamMessage<>(source.getStream(), source.getId(), source.getBody());
		}
	}

	/**
	 * {@link Converter} to convert {@link StreamReadOptions} to Lettuce's {@link XReadArgs}.
	 */
	enum StreamReadOptionsToXReadArgsConverter implements Converter<StreamReadOptions, XReadArgs> {

		INSTANCE;

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		public XReadArgs convert(StreamReadOptions source) {

			XReadArgs args = new XReadArgs();

			if (source.isNoack()) {
				args.noack(true);
			}

			if (source.getBlock() != null) {
				args.block(source.getBlock());
			}

			if (source.getCount() != null) {
				args.count(source.getCount());
			}
			return args;
		}
	}
}
