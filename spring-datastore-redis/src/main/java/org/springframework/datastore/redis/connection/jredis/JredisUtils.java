/*
 * Copyright 2010 the original author or authors.
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

package org.springframework.datastore.redis.connection.jredis;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;

import org.jredis.RedisException;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.dao.InvalidDataAccessApiUsageException;

/**
 * Helper class featuring methods for JRedis connection handling, providing support for exception translation. 
 * 
 * @author Costin Leau
 */
public abstract class JredisUtils {

	public static DataAccessException convertJredisAccessException(RedisException ex) {
		return new InvalidDataAccessApiUsageException(ex.getMessage(), ex);
	}

	public static String convertToString(byte[] bytes, String encoding) {
		try {
			return new String(bytes, encoding);
		} catch (UnsupportedEncodingException ex) {
			throw new DataRetrievalFailureException("Unsupported encoding " + encoding, ex);
		}
	}

	static <T extends Collection<String>> T convertToStringCollection(List<byte[]> bytes, String encoding, Class<T> collectionType) {

		Collection<String> col = (List.class.isAssignableFrom(collectionType) ? new ArrayList<String>(bytes.size())
				: new LinkedHashSet<String>(bytes.size()));

		try {
			for (byte[] bs : bytes) {
				col.add(new String(bs, encoding));
			}
			return (T) col;
		} catch (UnsupportedEncodingException ex) {
			throw new DataRetrievalFailureException("Unsupported encoding " + encoding, ex);
		}
	}
}
