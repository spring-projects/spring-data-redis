/*
 * Copyright 2014-2018 the original author or authors.
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
package org.springframework.data.redis.connection.convert;

import java.util.ArrayList;
import java.util.List;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.redis.core.types.RedisClientInfo;
import org.springframework.data.redis.core.types.RedisClientInfo.RedisClientInfoBuilder;

/**
 * {@link Converter} implementation to create one {@link RedisClientInfo} per line entry in given {@link String} array.
 *
 * <pre>
 * ## sample of single line
 * addr=127.0.0.1:60311 fd=6 name= age=4059 idle=0 flags=N db=0 sub=0 psub=0 multi=-1 qbuf=0 qbuf-free=32768 obl=0 oll=0 omem=0 events=r cmd=client
 * </pre>
 *
 * @author Christoph Strobl
 * @since 1.3
 */
public class StringToRedisClientInfoConverter implements Converter<String[], List<RedisClientInfo>> {

	/*
	 * (non-Javadoc)
	 * @see org.springframework.core.convert.converter.Converter#convert(Object)
	 */
	@Override
	public List<RedisClientInfo> convert(String[] lines) {

		List<RedisClientInfo> clientInfoList = new ArrayList<>(lines.length);
		for (String line : lines) {
			clientInfoList.add(RedisClientInfoBuilder.fromString(line));
		}

		return clientInfoList;
	}

}
