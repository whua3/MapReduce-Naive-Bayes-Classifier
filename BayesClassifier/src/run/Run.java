package run;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import mrJob.DocCount;

public class Run {
	static String[] otherArgs;

	public static void main(String[] args) throws Exception {
		// 加载hadoop配置
		Configuration conf = new Configuration();
		otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		// 校验命令行输入参数
		if (otherArgs.length != 6) {
			System.err.println("Usage: args error！");
			System.exit(6);
		}

		// 构造一个Job实例job1，并命名为"DocCount"
		Job DocCount = new Job(conf, "DocCount");
		// 设置jar
		DocCount.setJarByClass(DocCount.class);

		// 设置Mapper
		DocCount.setMapperClass(mrJob.DocCount.DocCountMap.class);
		// 设置Reducer
		DocCount.setReducerClass(mrJob.DocCount.DocCountReduce.class);
		DocCount.setMapOutputKeyClass(Text.class);
		DocCount.setMapOutputValueClass(IntWritable.class);
		// 设置OutputKey
		DocCount.setOutputKeyClass(Text.class);
		// 设置OutputValue
		DocCount.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(DocCount, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(DocCount, new Path(otherArgs[1]));

		// 构造一个Job实例job1，并命名为"fileContainWord"
		Job FileContainWord = new Job(conf, "FileContainWord");
		// 设置jar
		FileContainWord.setJarByClass(mrJob.FileContainWord.class);
		// 设置Mapper
		FileContainWord.setMapperClass(mrJob.FileContainWord.FileContainWordMap.class);
		// 设置Reducer
		FileContainWord.setReducerClass(mrJob.FileContainWord.FileContainWordReduce.class);
		FileContainWord.setMapOutputKeyClass(Text.class);
		FileContainWord.setMapOutputValueClass(IntWritable.class);
		// 设置OutputKey
		FileContainWord.setOutputKeyClass(Text.class);
		// 设置OutputValue
		FileContainWord.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(FileContainWord, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(FileContainWord, new Path(otherArgs[2]));

		// 构造一个Job实例job3，并命名为"SumWordInClassification"
		Job SumWordInClassification = new Job(conf, "SumWordInClassification");
		// 设置jar
		SumWordInClassification.setJarByClass(mrJob.SumWordInClassification.class);

		// 设置Mapper
		SumWordInClassification.setMapperClass(mrJob.SumWordInClassification.SumWordInClassificationMap.class);
		// 设置Reducer
		SumWordInClassification.setReducerClass(mrJob.SumWordInClassification.SumWordInClassificationReduce.class);
		SumWordInClassification.setMapOutputKeyClass(Text.class);
		SumWordInClassification.setMapOutputValueClass(IntWritable.class);
		// 设置OutputKey
		SumWordInClassification.setOutputKeyClass(Text.class);
		// 设置OutputValue
		SumWordInClassification.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(SumWordInClassification, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(SumWordInClassification, new Path(otherArgs[3]));

		// -------------------------------------------------------------------

		DocCount.waitForCompletion(true);
		FileContainWord.waitForCompletion(true);
		SumWordInClassification.waitForCompletion(true);

		// -------------------------------------------------------------------

		// 构造一个Job实例job4，并命名为"Test"
		Job Test = new Job(conf, "Test");
		// 设置jar
		Test.setJarByClass(mrJob.Test.class);

		// 设置Mapper
		Test.setMapperClass(mrJob.Test.TestMap.class);
		// 设置Reducer
		Test.setReducerClass(mrJob.Test.TestReduce.class);
		Test.setMapOutputKeyClass(Text.class);
		Test.setMapOutputValueClass(Text.class);
		// 设置OutputKey
		Test.setOutputKeyClass(Text.class);
		// 设置OutputValue
		Test.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(Test, new Path(otherArgs[4]));
		FileOutputFormat.setOutputPath(Test, new Path(otherArgs[5]));

		Test.waitForCompletion(true);
		staticFunction.Evaluation.evaluation();
		System.exit(0);
	}

}
