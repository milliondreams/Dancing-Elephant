class Reducer{
	void reduce(){
		int sum = 0;
		values.each {
			sum += it.get();
		}
		context.write(key, new IntWritable(sum));
	}
}
