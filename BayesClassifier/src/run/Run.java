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
		// ����hadoop����
		Configuration conf = new Configuration();
		otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		// У���������������
		if (otherArgs.length != 6) {
			System.err.println("Usage: args error��");
			System.exit(6);
		}

		// ����һ��Jobʵ��job1��������Ϊ"DocCount"
		Job DocCount = new Job(conf, "DocCount");
		// ����jar
		DocCount.setJarByClass(DocCount.class);

		// ����Mapper
		DocCount.setMapperClass(mrJob.DocCount.DocCountMap.class);
		// ����Reducer
		DocCount.setReducerClass(mrJob.DocCount.DocCountReduce.class);
		DocCount.setMapOutputKeyClass(Text.class);
		DocCount.setMapOutputValueClass(IntWritable.class);
		// ����OutputKey
		DocCount.setOutputKeyClass(Text.class);
		// ����OutputValue
		DocCount.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(DocCount, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(DocCount, new Path(otherArgs[1]));

		// ����һ��Jobʵ��job1��������Ϊ"fileContainWord"
		Job FileContainWord = new Job(conf, "FileContainWord");
		// ����jar
		FileContainWord.setJarByClass(mrJob.FileContainWord.class);
		// ����Mapper
		FileContainWord.setMapperClass(mrJob.FileContainWord.FileContainWordMap.class);
		// ����Reducer
		FileContainWord.setReducerClass(mrJob.FileContainWord.FileContainWordReduce.class);
		FileContainWord.setMapOutputKeyClass(Text.class);
		FileContainWord.setMapOutputValueClass(IntWritable.class);
		// ����OutputKey
		FileContainWord.setOutputKeyClass(Text.class);
		// ����OutputValue
		FileContainWord.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(FileContainWord, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(FileContainWord, new Path(otherArgs[2]));

		// ����һ��Jobʵ��job3��������Ϊ"SumWordInClassification"
		Job SumWordInClassification = new Job(conf, "SumWordInClassification");
		// ����jar
		SumWordInClassification.setJarByClass(mrJob.SumWordInClassification.class);

		// ����Mapper
		SumWordInClassification.setMapperClass(mrJob.SumWordInClassification.SumWordInClassificationMap.class);
		// ����Reducer
		SumWordInClassification.setReducerClass(mrJob.SumWordInClassification.SumWordInClassificationReduce.class);
		SumWordInClassification.setMapOutputKeyClass(Text.class);
		SumWordInClassification.setMapOutputValueClass(IntWritable.class);
		// ����OutputKey
		SumWordInClassification.setOutputKeyClass(Text.class);
		// ����OutputValue
		SumWordInClassification.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(SumWordInClassification, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(SumWordInClassification, new Path(otherArgs[3]));

		// -------------------------------------------------------------------

		DocCount.waitForCompletion(true);
		FileContainWord.waitForCompletion(true);
		SumWordInClassification.waitForCompletion(true);

		// -------------------------------------------------------------------

		// ����һ��Jobʵ��job4��������Ϊ"Test"
		Job Test = new Job(conf, "Test");
		// ����jar
		Test.setJarByClass(mrJob.Test.class);

		// ����Mapper
		Test.setMapperClass(mrJob.Test.TestMap.class);
		// ����Reducer
		Test.setReducerClass(mrJob.Test.TestReduce.class);
		Test.setMapOutputKeyClass(Text.class);
		Test.setMapOutputValueClass(Text.class);
		// ����OutputKey
		Test.setOutputKeyClass(Text.class);
		// ����OutputValue
		Test.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(Test, new Path(otherArgs[4]));
		FileOutputFormat.setOutputPath(Test, new Path(otherArgs[5]));

		Test.waitForCompletion(true);
		staticFunction.Evaluation.evaluation();
		System.exit(0);
	}

}
