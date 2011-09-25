class Mapper{
	def IntWritable one = new IntWritable(1)
	def Text word = new Text()

	def map(){
		value.toString().split().each {
			word.set(it)
			context.write(word, one)
		}
	}
}