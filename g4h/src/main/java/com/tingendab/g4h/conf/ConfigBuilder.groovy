package com.tingendab.g4h.conf

import java.awt.TextArea;

import com.tingendab.g4h.*

import groovy.util.BuilderSupport

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.*
import org.apache.hadoop.io.*
import org.apache.hadoop.mapreduce.*
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import com.tingendab.g4h.conf.model.FileFormatHelper
import com.tingendab.g4h.conf.model.JobHelper

class ConfigBuilder extends BuilderSupport {
	private static CONFNAME = "config"
	private static JOBHELPERNAME = "job"
	private static ATTRNAME = "attributes"
	private static INPUTNAME = "input"
	private static OUTPUTNAME = "output"
	private static DISTRIBUTENAME = "distribute"
	//private static PATHNAME = "paths"
	private static ROOTNAME = "jobs"
	private static DEFAULT_FILE_INPUT_FORMAT_PACKAGE = "org.apache.hadoop.mapreduce.lib.input."
	private static DEFAULT_FILE_OUTPUT_FORMAT_PACKAGE = "org.apache.hadoop.mapreduce.lib.output."
	private static DEFAULT_FILE_INPUT_FORMAT = FileInputFormat.class
	private static DEFAULT_FILE_OUTPUT_FORMAT = FileOutputFormat.class
	private static DEFAULT_OUTPUT_KEY_CLASS = Text.class
	private static DEFAULT_OUTPUT_VALUE_CLASS = Text.class

	List<JobHelper> jobHelpers = new ArrayList();
	Map attributes;

	@Override
	protected void setParent(Object parent, Object child) {
	}

	@Override
	protected Object createNode(Object name) {
		//TODO:: Implement support for DistributedCache and Classpath injection
		switch(name){
			case CONFNAME:
				return [name:CONFNAME,value:new Configuration()]
			case JOBHELPERNAME:
				return [name:JOBHELPERNAME,value:new JobHelper()]
			case ATTRNAME:
				return [name:ATTRNAME,value:new HashMap()]
			case INPUTNAME:
				return [name:INPUTNAME, value:new FileFormatHelper()]
			case OUTPUTNAME:
				return [name:OUTPUTNAME, value:new FileFormatHelper()]
			case DISTRIBUTENAME:
				return [name:DISTRIBUTENAME,value:new HashMap()]
			case ROOTNAME:
				return [name:"jobs", value:jobHelpers]
		}
		return name
	}

	@Override
	protected Object createNode(Object name, Object value) {
		switch(getCurrent().name){
			case ATTRNAME:
				getCurrent().value[name] = value
				return
			case JOBHELPERNAME:
				getCurrent().value.setProperty(name,value)
				return
			case DISTRIBUTENAME:
				getCurrent().value[name] = value
				return
			case INPUTNAME:
			case OUTPUTNAME:
				getCurrent().value.setProperty(name,value)
				return
		}

		return ["name":name, "value": value]
	}

	@Override
	protected Object createNode(Object name, Map attributes) {
		return null;
	}

	@Override
	protected Object createNode(Object name, Map attributes, Object value) {
		// TODO Auto-generated method stub
		return null;
	}

	protected void nodeCompleted(Object parent,Object node) {
		if(node!=null && parent instanceof java.util.Map){
			switch(parent.name){
				case CONFNAME:
					if(node.name == ATTRNAME){
						node.value.each{ k,v ->
							def type = (v.class =~ /.*\./).replaceAll ""
							switch(type){
								case "String":
									parent.value.set(k,v)
									break
								case "Integer":
									parent.value.setInt(k,v)
									break
								case "BigDecimal":
								case "Float":
									parent.value.setFloat(k,v)
									break;
								case "Boolean":
									parent.value.setBoolean(k,v)
									break
							}
						}
					}
					break
				case JOBHELPERNAME:
					if(node instanceof java.util.Map){
						switch(node.name){
							case CONFNAME:
								parent.value.configuration = node.value
								break
							case INPUTNAME:
								parent.value.input = node.value
								break
							case OUTPUTNAME:
								parent.value.output = node.value
								break
							case DISTRIBUTENAME:
								parent.value.distribute = node.value
						}
					}
					break
				default:
					
					if(node instanceof java.util.Map){
						if(node.name == INPUTNAME){
							if(node.value.format == null) node.value.format = DEFAULT_FILE_INPUT_FORMAT
						}else if(node.name == OUTPUTNAME){
							if(node.value.format == null) node.value.format = DEFAULT_FILE_OUTPUT_FORMAT
							if(node.value.keyClass == null) node.value.keyClass = DEFAULT_OUTPUT_KEY_CLASS
							if(node.value.valueClass == null) node.value.valueClass = DEFAULT_OUTPUT_VALUE_CLASS
						} else if(node.name == JOBHELPERNAME){
							parent.value.add node.value
						}
					}
			}
		}
	}

	public List<Job> prepareJobs(String appName){
		List<Job> jobs= new ArrayList();
		jobHelpers.each{jobHelper ->
			Job j;
			Long jobtime = new Date().time
			if(jobHelper.name == null){
				jobHelper.name = appName + jobtime
			}

			if(jobHelper.configuration == null){
				jobHelper.configuration = new Configuration()
			}

			j = new Job(jobHelper.configuration, jobHelper.name)

			setMapperClass(j,jobHelper.mapper)
			setCombinerClass(j,jobHelper.combiner)
			setReducerClass(j,jobHelper.reducer)

			if(jobHelper.input == null){
				j.setInputFormatClass(DEFAULT_FILE_INPUT_FORMAT)
				DEFAULT_FILE_INPUT_FORMAT.addInputPath(j, "./input/${appName}")
			}else{
				if(jobHelper.input.format == null){
					jobHelper.input.format = DEFAULT_FILE_INPUT_FORMAT
				}
				if(jobHelper.input.paths == null){
					jobHelper.input.paths="./input/${appName}"
				}
				//TODO: Build support for custom file input formats written in groovy
				j.setInputFormatClass jobHelper.input.format
				jobHelper.input.format.setInputPaths(j,jobHelper.input.paths)
			}

			if(jobHelper.input == null){
				j.setOutputFormatClass(DEFAULT_FILE_OUTPUT_FORMAT)
				DEFAULT_FILE_OUTPUT_FORMAT.setOutputPath(j, new Path("./output/${appName}"))
			}else{
				if(jobHelper.output.format == null){
					jobHelper.output.format = DEFAULT_FILE_OUTPUT_FORMAT
				}
				if(jobHelper.output.paths == null){
					jobHelper.output.paths="./output/${appName}"
				}
				if(jobHelper.output.keyClass == null) jobHelper.output.keyClass = DEFAULT_OUTPUT_KEY_CLASS
				if(jobHelper.output.valueClass == null) jobHelper.output.valueClass = DEFAULT_OUTPUT_VALUE_CLASS
				//TODO: Build support for custom file output formats written in groovy
				j.setOutputFormatClass jobHelper.output.format
				jobHelper.output.format.setOutputPath(j,new Path(jobHelper.output.paths))
				j.setOutputKeyClass jobHelper.output.keyClass
				j.setOutputValueClass jobHelper.output.valueClass
			}
			jobs.add j
		}
		return jobs;
	}

	private void  setMapperClass(Job j, String mapperName){
		//TODO: Once DistrbutedCache.addToClasspath works we can write full fledged mappers, i/p formats in groovy
		//TODO: Improve the setup logic
		try{
			Class clazz = Class.forName mapperName
			if(doesExtend(clazz,"org.apache.hadoop.mapreduce.Mapper")){
				j.setMapperClass(clazz)
			}else{
				j.getConfiguration().set("g4h.mapper", mapperName)
				j.setMapperClass GroovyMapper.class
			}
		}catch(ClassNotFoundException cnf){
			j.getConfiguration().set("g4h.mapper", mapperName)
			j.setMapperClass GroovyMapper.class
		}
	}

	private void  setCombinerClass(Job j, String combinerName){
		//TODO: Once DistrbutedCache.addToClasspath works we can write full fledged mappers, i/p formats in groovy
		//TODO: Improve the setup logic
		if(combinerName != null || combinerName == ""){
			try{
				Class clazz = Class.forName combinerName
				if(doesExtend(clazz,"org.apache.hadoop.mapreduce.Reducer")){
					j.setCombinerClass(clazz)
				}else{
					j.getConfiguration().set("g4h.combiner", combinerName)
					j.setCombinerClass GroovyCombiner.class
				}
			}catch(ClassNotFoundException cnf){
				j.getConfiguration().set("g4h.combiner", combinerName)
				j.setCombinerClass GroovyCombiner.class
			}
		}
	}

	private void  setReducerClass(Job j, String reducerName){
		if(reducerName != null || reducerName == ""){
			try{
				Class clazz = Class.forName reducerName
				if(doesExtend(clazz,"org.apache.hadoop.mapreduce.Reducer")){
					j.setReducerClass(clazz)
				}else{
					j.getConfiguration().set("g4h.reducer", reducerName)
					j.setReducerClass GroovyReducer.class
				}
			}catch(ClassNotFoundException cnf){
				j.getConfiguration().set("g4h.reducer", reducerName)
				j.setReducerClass GroovyReducer.class
			}
		}
	}

	private Boolean doesExtend(cls, parent) { if(cls.name == parent) return true; cls.superclass==null ? false : doesExtend(cls.superclass, parent) }
}
