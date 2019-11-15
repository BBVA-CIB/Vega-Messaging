package com.bbva.kyof.vega.testUtils;

import java.lang.reflect.Field;

public class ReflectionUtils
{
	/**
	 * Using reflection, take the object field from an origin
	 * @param origin the object that contains the searched attribute
	 * @param fieldName the name of the searched atribute
	 * @return the object searched
	 * @throws NoSuchFieldException
	 * @throws IllegalAccessException
	 */
	public static Object getObjectByReflection(Object origin, String fieldName)
			throws NoSuchFieldException, IllegalAccessException
	{
		Field field = origin.getClass().getDeclaredField(fieldName);
		field.setAccessible(true);
		return field.get(origin);
	}
}
