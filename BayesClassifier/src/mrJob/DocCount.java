package mrJob;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class DocCount {
	/*
	 * ͳ��ÿ��classification�е�doc����Ŀ 
	 * map����<�����ļ�������IntWritable����ÿһ�����ݣ�Text��>
	 * map���<classification��Text����one��IntWritable��>
	 * reduce���<classification��Text����number��IntWritable��>
	 */
	public static class DocCountMap extends Mapper<Object, Text, Text, IntWritable> {
		private final IntWritable one = new IntWritable(1);
		private Text classification = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			// ��StringTokenizer��Ϊ�ִ�������value���зִ�
			StringTokenizer itr = new StringTokenizer(value.toString());
			classification.set(itr.nextToken());

			context.write(classification, one);
		}
	}

	public static class DocCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
}
