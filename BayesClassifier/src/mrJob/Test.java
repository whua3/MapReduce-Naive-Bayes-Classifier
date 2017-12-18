package mrJob;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Test {
	/*
	 * 预测每个测试文档所属的类别 map输入：经过处理的测试文档<输入文件行数（IntWritable），每一行内容（Text）>
	 * map输出：<<classification:doc>（Text）,word（Text）>
	 * 这样，每篇测试文档的每个word都发送到了同一个reducer
	 * 在reduce阶段计算每篇文档属于每个classification的概率，最后选出概率最大的，作为这篇文档预测的classification
	 * reduce输出：<<classification：doc>（Text），预测的classification（Text）>
	 */
	public static class TestMap extends Mapper<Object, Text, Text, Text> {

		private Text text = new Text();
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// 用StringTokenizer作为分词器，对value进行分词
			StringTokenizer itr = new StringTokenizer(value.toString());
			String classification;
			String doc;

			classification = itr.nextToken();
			doc = itr.nextToken();
			text.set(classification + ":" + doc);
			// 遍历分词后结果
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(text, word);
			}
		}
	}

	public static class TestReduce extends Reducer<Text, Text, Text, Text> {
		public void setup(Context context) throws IOException {
			staticFunction.CalculateProbability.GetPriorProbability();
			staticFunction.CalculateProbability.GetConditionProbability();
		}

		private Text coutValue = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			HashMap<String, Integer> testWordMap = new HashMap<String, Integer>();
			testWordMap.clear();
			String word;
			double conditionProbability = 0;
			double wordPriorProbability = 0;
			double max = (-1) * Double.MAX_VALUE;
			HashMap<String, Double> probabilityOfClassification = new HashMap<String, Double>();
			//将一篇测试文档中的单词保存在testWordMap中
			for (Text val : values) {
				word = val.toString();
				if (testWordMap.containsKey(word)) {
					testWordMap.put(word, testWordMap.get(word) + 1);
				} else {
					testWordMap.put(word, 1);
				}
			}

			//遍历一篇测试文档中的每个单词
			for (Map.Entry<String, Integer> entry : testWordMap.entrySet()) {
				word = entry.getKey();
				int num = entry.getValue();

				//遍历每个类别，计算这个单词在这个类别中的概率和
				for (Map.Entry<String, Integer> entry2 : staticFunction.CalculateProbability.classificationArray.entrySet()) {
					String tmpClassification = entry2.getKey();
					if (staticFunction.CalculateProbability.wordsProbability.containsKey(tmpClassification + ":" + word)) {
						conditionProbability = Math.log(staticFunction.CalculateProbability.wordsProbability.get(tmpClassification + ":" + word));
					} else {
						conditionProbability = Math.log(staticFunction.CalculateProbability.wordsProbability.get(tmpClassification));
					}
					double probability = 0;
					probability = conditionProbability * num;
					if (probabilityOfClassification.containsKey(tmpClassification)) {
						probabilityOfClassification.put(tmpClassification,
								probabilityOfClassification.get(tmpClassification) + probability);
					} else {
						probabilityOfClassification.put(tmpClassification, probability);
					}
				}
			}

			// 加上先验概率
			for (Map.Entry<String, Integer> entry2 : staticFunction.CalculateProbability.classificationArray.entrySet()) {
				String tmpClassification = entry2.getKey();
				wordPriorProbability = Math.log(staticFunction.CalculateProbability.priorProbability.get(tmpClassification));
				probabilityOfClassification.put(tmpClassification,
						probabilityOfClassification.get(tmpClassification) + wordPriorProbability);
			}

			String preClassification;
			double relevantPro = 0;
			String resultClassification = "wanghua";
			for (Map.Entry<String, Double> entry : probabilityOfClassification.entrySet()) {
				preClassification = entry.getKey();
				relevantPro = entry.getValue();
				if (relevantPro > max) {
					resultClassification = preClassification;
					max = relevantPro;
				}
			}
			coutValue.set(resultClassification);
			context.write(key, coutValue);
		}
	}
}
