package org.springframework.data.redis.core.index;

import org.springframework.data.redis.core.index.Indexed;

public class TestAddress {

    public String street;
    @Indexed 
    public String city;
    public TestState state;
    public int streetNumber;
		public static String getStateCategory(TestState state) {
			String stateCategory;
			switch(state){
				case CA:
				case OR:
				case WA:
					stateCategory = "West";
					break;
				default:
					stateCategory =	"others";
			}
			return stateCategory;
		}
}
