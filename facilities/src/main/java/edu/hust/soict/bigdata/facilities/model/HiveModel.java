package edu.hust.soict.bigdata.facilities.model;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface HiveModel {
    String table() default "test";
    String schema() default "default";
}
