package com.tingendab.g4h.conf.model

import java.util.List;

import org.apache.hadoop.conf.Configuration;

class JobHelper {
	String name;
	String mapper;
	String combiner;
	String reducer;
	Configuration configuration;

	FileFormatHelper input; 
	FileFormatHelper output;
	
	Map<String, String> distribute = new HashMap();
	
	/* String toString(){
		println "[" + name
		println mapper
		println combiner
		println reducer
		println configuration
		println input
		println output + "]" 
	} */
}
