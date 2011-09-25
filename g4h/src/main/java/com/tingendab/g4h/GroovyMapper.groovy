package com.tingendab.g4h

import groovy.lang.GroovyClassLoader
import groovy.lang.GroovyObject
import groovy.lang.MetaClass

import java.io.IOException

import org.apache.hadoop.filecache.DistributedCache
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.WritableComparable
import org.apache.hadoop.mapreduce.Mapper

import com.tingendab.g4h.groovy.*

/**
 * 
 * @author rohit
 */
class GroovyMapper
extends
Mapper<WritableComparable, WritableComparable, WritableComparable, WritableComparable> {
	private GroovyObject mapper

	@Override
	protected void setup(Mapper.Context context) throws IOException,
	InterruptedException {
		try {
			Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration())

			System.err.println("<p>------------------ Printing path files -------------<br/>");
			for(Path path: paths){
				System.err.println(path.toUri().toString() + "  <br/>");
			}
			System.err.println("------------------ Done Printing path files -------------</p>");
	
			GroovyClassLoader gcl = DynamicSupportUtil.getHadoopedClassLoader(GroovyMapper.class.classLoader)
			paths.each {gcl.addClasspath(it.toString())}
			Class mapperClass = gcl.loadClass(context.getConfiguration().get("g4h.mapper"))
			mapper = (GroovyObject) mapperClass.newInstance()
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace()
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace()
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace()
		}
	}

	@Override
	protected void map(WritableComparable key, WritableComparable value,
	Mapper.Context context) throws IOException, InterruptedException {
		/* Object[] argz = new Object[3]
		 argz[0] = key
		 argz[1] = value
		 argz[2] = context */

		mapper.metaClass.key = key
		mapper.metaClass.value = value
		mapper.metaClass.context = context

		if(mapper instanceof Script){
			mapper.run()
		}else{
			mapper.map()
		}
	}
}
