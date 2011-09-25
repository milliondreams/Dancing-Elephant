package com.tingendab.g4h;

import groovy.lang.GroovyClassLoader;
import groovy.lang.GroovyObject;

import java.io.IOException;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

/**
*
* @author rohit
*/
public class GroovyCombiner
		extends
		Reducer<WritableComparable, WritableComparable, WritableComparable, WritableComparable> {
	private GroovyObject combiner;

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
			String combinerClassName = context.getConfiguration().get(
			"g4h.combiner");
			Class reducerClass = gcl.loadClass(combinerClassName);
			combiner = (GroovyObject) reducerClass.newInstance();
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
		combiner.metaClass.key = key
		combiner.metaClass.values = values
		combiner.metaClass.context = context

		if(combiner instanceof Script){
			combiner.run()
		}else{
			combiner.reduce()
		}
	}
}
