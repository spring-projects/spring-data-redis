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
import org.springframework.data.redis.connection.RedisStreamCommands.ByteRecord;
import org.springframework.data.redis.connection.RedisStreamCommands.StreamReadOptions;
import org.springframework.data.redis.connection.StreamRecords;
import org.springframework.data.redis.connection.convert.ListConverter;

/**
 * Converters for Redis Stream-specific types.
 * <p/>
 * Converters typically convert between value objects/argument objects retaining the actual types of values (i.e. no
 * serialization/deserialization happens here).
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.2
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
class StreamConverters {

	/**
	 * Convert {@link StreamReadOptions} to Lettuce's {@link XReadArgs}.
	 *
	 * @param readOptions must not be {@literal null}.
	 * @return the converted {@link XReadArgs}.
	 */
	static XReadArgs toReadArgs(StreamReadOptions readOptions) {
		return StreamReadOptionsToXReadArgsConverter.INSTANCE.convert(readOptions);
	}

	public static Converter<StreamMessage<byte[], byte[]>, ByteRecord> byteRecordConverter() {
		return (it) -> StreamRecords.newRecord().in(it.getStream()).withId(it.getId()).ofBytes(it.getBody());
	}

	public static Converter<List<StreamMessage<byte[], byte[]>>, List<ByteRecord>> byteRecordListConverter() {
		return new ListConverter<>(byteRecordConverter());
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
