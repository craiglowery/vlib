package com.craiglowery.java.vlib.common;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface DefaultConfigurationValue {
	String value();
}
