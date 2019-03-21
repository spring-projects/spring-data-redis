/*
 * Copyright 2011-2014 the original author or authors.
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
package org.springframework.data.redis.connection.srp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.springframework.data.redis.connection.DefaultSortParameters;
import org.springframework.data.redis.connection.SortParameters;

/**
 * Unit test of {@link SrpUtils}
 * 
 * @author Jennifer Hickey
 * @author Thomas Darimont Suppressed deprecation warnings since SrpUtils is deprecated.
 */
@SuppressWarnings("deprecation")
public class SrpUtilsTests {

	@Test
	public void testSortParamsWithAllParams() {
		SortParameters sortParams = new DefaultSortParameters().alpha().asc().by("weight_*".getBytes())
				.get("object_*".getBytes()).limit(0, 5);
		Object[] sort = SrpUtils.sortParams(sortParams, "foo".getBytes());
		assertArrayEquals(
				new String[] { "BY", "weight_*", "LIMIT 0 5", "GET", "object_*", "ASC", "ALPHA", "STORE", "foo" },
				convertByteArrays(sort));
	}

	@Test
	public void testSortParamsOnlyBy() {
		SortParameters sortParams = new DefaultSortParameters().numeric().by("weight_*".getBytes());
		Object[] sort = SrpUtils.sortParams(sortParams);
		assertThat(convertByteArrays(sort)).isEqualTo(new String[] { "BY", "weight_*" });
	}

	@Test
	public void testSortParamsOnlyLimit() {
		SortParameters sortParams = new DefaultSortParameters().numeric().limit(0, 5);
		Object[] sort = SrpUtils.sortParams(sortParams);
		assertThat(convertByteArrays(sort)).isEqualTo(new String[] { "LIMIT 0 5" });
	}

	@Test
	public void testSortParamsOnlyGetPatterns() {
		SortParameters sortParams = new DefaultSortParameters().numeric().get("foo".getBytes()).get("bar".getBytes());
		Object[] sort = SrpUtils.sortParams(sortParams);
		assertThat(convertByteArrays(sort)).isEqualTo(new String[] { "GET", "foo", "GET", "bar" });
	}

	@Test
	public void testSortParamsOnlyOrder() {
		SortParameters sortParams = new DefaultSortParameters().numeric().desc();
		Object[] sort = SrpUtils.sortParams(sortParams);
		assertThat(convertByteArrays(sort)).isEqualTo(new String[] { "DESC" });
	}

	@Test
	public void testSortParamsOnlyAlpha() {
		SortParameters sortParams = new DefaultSortParameters().alpha();
		Object[] sort = SrpUtils.sortParams(sortParams);
		assertThat(convertByteArrays(sort)).isEqualTo(new String[] { "ALPHA" });
	}

	@Test
	public void testSortParamsOnlyStore() {
		SortParameters sortParams = new DefaultSortParameters().numeric();
		Object[] sort = SrpUtils.sortParams(sortParams, "storelist".getBytes());
		assertThat(convertByteArrays(sort)).isEqualTo(new String[] { "STORE", "storelist" });
	}

	@Test
	public void testSortWithAllParams() {
		SortParameters sortParams = new DefaultSortParameters().alpha().asc().by("weight_*".getBytes())
				.get("object_*".getBytes()).limit(0, 5);
		byte[] sort = SrpUtils.sort(sortParams, "foo".getBytes());
		assertThat(new String(sort)).isEqualTo("BY weight_* LIMIT 0 5 GET object_* ASC ALPHA STORE foo");
	}

	@Test
	public void testSortOnlyBy() {
		SortParameters sortParams = new DefaultSortParameters().numeric().by("weight_*".getBytes());
		byte[] sort = SrpUtils.sort(sortParams);
		assertThat(new String(sort)).isEqualTo("BY weight_*");
	}

	@Test
	public void testSortOnlyLimit() {
		SortParameters sortParams = new DefaultSortParameters().numeric().limit(0, 5);
		byte[] sort = SrpUtils.sort(sortParams);
		assertThat(new String(sort)).isEqualTo("LIMIT 0 5");
	}

	@Test
	public void testSortOnlyGetPatterns() {
		SortParameters sortParams = new DefaultSortParameters().numeric().get("foo".getBytes()).get("bar".getBytes());
		byte[] sort = SrpUtils.sort(sortParams);
		assertThat(new String(sort)).isEqualTo("GET foo GET bar");
	}

	@Test
	public void testSortOnlyOrder() {
		SortParameters sortParams = new DefaultSortParameters().numeric().desc();
		byte[] sort = SrpUtils.sort(sortParams);
		assertThat(new String(sort)).isEqualTo("DESC");
	}

	@Test
	public void testSortOnlyAlpha() {
		SortParameters sortParams = new DefaultSortParameters().alpha();
		byte[] sort = SrpUtils.sort(sortParams);
		assertThat(new String(sort)).isEqualTo("ALPHA");
	}

	@Test
	public void testSortOnlyStore() {
		SortParameters sortParams = new DefaultSortParameters().numeric();
		byte[] sort = SrpUtils.sort(sortParams, "storelist".getBytes());
		assertThat(new String(sort)).isEqualTo("STORE storelist");
	}

	private String[] convertByteArrays(Object[] bytes) {
		List<String> convertedParams = new ArrayList<String>();
		for (Object b : bytes) {
			convertedParams.add(new String((byte[]) b));
		}
		return convertedParams.toArray(new String[0]);
	}
}
