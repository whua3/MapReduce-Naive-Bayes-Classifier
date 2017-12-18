package mrJob;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class SumWordInClassification {
	/*
	 * ͳ��ÿ��classification��word������ 
	 * map����<�����ļ�������IntWritable����ÿһ�����ݣ�Text��>
	 * map���<<classification>��Text��,number��IntWritable����>
	 * reduce���<<classification>��Text��,sum��IntWritable��>
	 */
	public static class SumWordInClassificationMap extends Mapper<Object, Text, Text, IntWritable> {

		private Text classification = new Text();
		private IntWritable number = new IntWritable();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			// ��StringTokenizer��Ϊ�ִ�������value���зִ�
			StringTokenizer itr = new StringTokenizer(value.toString());
			classification.set(itr.nextToken());
			itr.nextToken();
			int sum = 0;
			// �����ִʺ���
			while (itr.hasMoreTokens()) {
				itr.nextToken();
				sum++;
			}
			number.set(sum);
			context.write(classification, number);
		}
	}

	public static class SumWordInClassificationReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
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
