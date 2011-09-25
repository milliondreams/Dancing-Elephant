class Config{
	
	def config = {config, cargs ->
		config.set("g4h.mapper", "com.tingendab.hadoop.groovy.scripts.Mapper")
		config.set("g4h.reducer", "com.tingendab.hadoop.groovy.scripts.Reducer")
		/* config.set("g4h.combiner", "com.tingendab.hadoop.groovy.scripts.Reducer") */
	}
	
	def job = {job, cargs ->
		FileInputFormat.addInputPath(job, new Path("./wcinput"));
		FileOutputFormat.setOutputPath(job, new Path("./wcoutput"));
	}
}
