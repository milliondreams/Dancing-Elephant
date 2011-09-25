create jobs {
	job{
		name "wordcount"        
		mapper "com.tingendab.hadoop.groovy.Mapper"    
		reducer "com.tingendab.hadoop.groovy.Reducer" 
	
		input {
			format TextInputFormat.class
			paths  "./wcinput"
		}
		output {
			format TextOutputFormat.class
			paths "./wcoutput"
			keyClass org.apache.hadoop.io.Text.class
			valueClass org.apache.hadoop.io.IntWritable.class
		}
	}
}
