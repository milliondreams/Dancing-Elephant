package com.tingendab.g4h.groovy

import org.codehaus.groovy.control.CompilerConfiguration;
import org.codehaus.groovy.control.customizers.ImportCustomizer;
import groovy.lang.GroovyClassLoader

class DynamicSupportUtil {

	static getConfigClassLoader(ClassLoader loader){
		CompilerConfiguration configuration = new CompilerConfiguration();
		ImportCustomizer importCustomizer = new ImportCustomizer();
		importCustomizer.addStarImport("org.apache.hadoop.io")
		importCustomizer.addStarImport("org.apache.hadoop.fs")
		importCustomizer.addStarImport("org.apache.hadoop.mapreduce.lib.input")
		importCustomizer.addStarImport("org.apache.hadoop.mapreduce.lib.output")
		importCustomizer.addImport "org.apache.hadoop.mapreduce.lib.input.FileInputFormat"
		importCustomizer.addImport "org.apache.hadoop.mapreduce.lib.output.FileOutputFormat"
		importCustomizer.addImport "com.tingendab.g4h.conf.ConfigBuilder"
		configuration.addCompilationCustomizers(importCustomizer)
		
		return new GroovyClassLoader(loader,configuration);
	}
	
	static getHadoopedClassLoader(ClassLoader loader){
		CompilerConfiguration configuration = new CompilerConfiguration();
		ImportCustomizer importCustomizer = new ImportCustomizer();
		importCustomizer.addStarImport("org.apache.hadoop.io")
		configuration.addCompilationCustomizers(importCustomizer)
		
		return new GroovyClassLoader(loader,configuration);
	}
}
