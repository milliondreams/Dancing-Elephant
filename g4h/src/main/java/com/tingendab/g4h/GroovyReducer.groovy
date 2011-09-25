/*
 * GroovyReducer.java
 *
 * Created on May 14, 2011, 2:46:00 AM
 */

package com.tingendab.g4h

import java.io.IOException

import org.apache.hadoop.filecache.DistributedCache
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.WritableComparable
import org.apache.hadoop.mapreduce.Reducer

import com.tingendab.g4h.groovy.*

/**
 * 
 * @author rohit
 */
public class GroovyReducer
		extends
		Reducer<WritableComparable, WritableComparable, WritableComparable, WritableComparable> {

	private GroovyObject reducer;

	@Override
	protected void setup(Reducer.Context context) throws IOException,
			InterruptedException {
		try {
			Path[] paths = DistributedCache.getLocalCacheFiles(context
					.getConfiguration());

			GroovyClassLoader gcl = DynamicSupportUtil.getHadoopedClassLoader(GroovyReducer.class.classLoader)
			for (Path path : paths) {
				gcl.addClasspath(path.toString());
			}
			Class reducerClass = gcl.loadClass(context.getConfiguration().get(
					"g4h.reducer"));
			reducer = (GroovyObject) reducerClass.newInstance();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	};

	@Override
	protected void reduce(WritableComparable key,
			Iterable<WritableComparable> values, Reducer.Context context)
			throws IOException, InterruptedException {

		reducer.metaClass.key = key
		reducer.metaClass.values = values
		reducer.metaClass.context = context

		if(reducer instanceof Script){
			reducer.run()
		}else{
			reducer.reduce()
		}
	}
}
