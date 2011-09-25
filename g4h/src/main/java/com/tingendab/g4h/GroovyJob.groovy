package com.tingendab.g4h

import groovy.lang.GroovyClassLoader
import groovy.lang.GroovyObject

import java.io.File
import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.filecache.DistributedCache
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.codehaus.groovy.control.CompilationFailedException

import com.tingendab.g4h.conf.ConfigBuilder;
import com.tingendab.g4h.groovy.DynamicSupportUtil;

class GroovyJob {
	static main(String[] args) throws Exception {
		runApp(args[0], args)
	}

	static runApp(String appName, String[] args){

		// Initialize the configuration
		Configuration conf = new Configuration()

		// Get access to the Hadoop filesystem
		FileSystem hdfs = FileSystem.get(conf)

		// Groovy Configurator file path
		Path configPath = new Path("./g4happs/" + appName + "/conf/Config.groovy")

		println "Will read configuration from " + configPath.toUri()

		// Put the Application in distributed Cache
		Path p = new Path("./g4happs/" + appName);
		//System.out.println(p);
		//DistributedCache.addCacheFile(p.toUri(), conf)
		//DistributedCache.addFileToClassPath(new Path("./g4happs/" + appName), conf)

		if (hdfs.exists(configPath)) {
			try {

				FSDataInputStream din = hdfs.open(configPath)

				//GroovyClassLoader gcl = new GroovyClassLoader(GroovyJob.class.getClassLoader());
				GroovyClassLoader gcl = DynamicSupportUtil.getConfigClassLoader(GroovyJob.class.classLoader)
				Class configClass = gcl.parseClass(din)
				GroovyObject config = (GroovyObject) configClass.newInstance()

				Object[] argz = new Object[2]
				argz[0] = conf
				argz[1] = args

				ConfigBuilder create = new ConfigBuilder()
				config.metaClass.create = create
				config.metaClass.argz = args

				config.run()
				List<Job> jobs = create.prepareJobs appName
				
				jobs.each {
					DistributedCache.addCacheFile(p.toUri(), it.conf)
					it.setJarByClass(GroovyJob.class)
					it.waitForCompletion(true)	
				}

			} catch (CompilationFailedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace()
			} catch (IllegalStateException e) {
				// TODO Auto-generated catch block
				e.printStackTrace()
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace()
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace()
			}
		} else {
			Job job = new Job(conf, appName)

			// Set the defaults. These may be overridden by the configurator
			job.setOutputKeyClass(Text.class)
			job.setOutputValueClass(IntWritable.class)
			FileInputFormat.addInputPath(job, new Path("./input/" + appName))
			FileOutputFormat.setOutputPath(job, new Path("./input" + appName))
			System.exit(job.waitForCompletion(true) ? 0 : 1)
		}

	}
}
