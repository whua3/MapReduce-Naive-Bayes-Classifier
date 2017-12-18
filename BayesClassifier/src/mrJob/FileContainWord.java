package mrJob;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class FileContainWord {
	/*
	 * 统计每个word在每个classification中出现的次数 
	 * map输入<输入文件行数（IntWritable），每一行内容（Text）>
	 * map输出<<classification:word>（Text）,one（Intwritable））>
	 * reduce输出<<classification：word>（Text）,number（IntWritable）>
	 */
	public static class FileContainWordMap extends Mapper<Object, Text, Text, IntWritable> {
		private final IntWritable one = new IntWritable(1);
		private Text classification = new Text();
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			// 用StringTokenizer作为分词器，对value进行分词
			StringTokenizer itr = new StringTokenizer(value.toString());
			classification.set(itr.nextToken());
			itr.nextToken();
			// 遍历分词后结果
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
