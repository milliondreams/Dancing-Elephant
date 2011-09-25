def IntWritable one = new IntWritable(1)
def Text word = new Text()

value.toString().split().each {
	word.set(it)
	context.write(word, one)
}
