package mrJob;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class FileContainWord {
	/*
	 * ͳ��ÿ��word��ÿ��classification�г��ֵĴ��� 
	 * map����<�����ļ�������IntWritable����ÿһ�����ݣ�Text��>
	 * map���<<classification:word>��Text��,one��Intwritable����>
	 * reduce���<<classification��word>��Text��,number��IntWritable��>
	 */
	public static class FileContainWordMap extends Mapper<Object, Text, Text, IntWritable> {
		private final IntWritable one = new IntWritable(1);
		private Text classification = new Text();
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			// ��StringTokenizer��Ϊ�ִ�������value���зִ�
			StringTokenizer itr = new StringTokenizer(value.toString());
			classification.set(itr.nextToken());
			itr.nextToken();
			// �����ִʺ���
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				String str = classification.toString() + ":" + word.toString();
				word.set(str);
				context.write(word, one);
			}
		}
	}

	public static class FileContainWordReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
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
