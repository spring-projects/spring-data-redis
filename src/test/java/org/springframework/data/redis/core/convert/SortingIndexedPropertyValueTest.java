package org.springframework.data.redis.core.convert;

import static org.junit.Assert.*;

import org.junit.Test;
import org.springframework.data.redis.core.index.SortingIndexDefinition;

/**
 * DATAREDIS-814
 * 
 * @author Yan Ma
 */
public class SortingIndexedPropertyValueTest {

	@Test
	public void test() {
		
		SortingIndexDefinition sid = new SortingIndexDefinition("AccountTransaction", "createdTimestamp");
		SortingIndexedPropertyValue sipv = new SortingIndexedPropertyValue("AccountTransaction", "createdTimestamp",
				"createdTimestamp");
	}

}
