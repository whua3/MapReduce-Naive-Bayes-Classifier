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
	 * Ԥ��ÿ�������ĵ���������� map���룺��������Ĳ����ĵ�<�����ļ�������IntWritable����ÿһ�����ݣ�Text��>
	 * map�����<<classification:doc>��Text��,word��Text��>
	 * ������ÿƪ�����ĵ���ÿ��word�����͵���ͬһ��reducer
	 * ��reduce�׶μ���ÿƪ�ĵ�����ÿ��classification�ĸ��ʣ����ѡ���������ģ���Ϊ��ƪ�ĵ�Ԥ���classification
	 * reduce�����<<classification��doc>��Text����Ԥ���classification��Text��>
	 */
	public static class TestMap extends Mapper<Object, Text, Text, Text> {

		private Text text = new Text();
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// ��StringTokenizer��Ϊ�ִ�������value���зִ�
			StringTokenizer itr = new StringTokenizer(value.toString());
			String classification;
			String doc;

			classification = itr.nextToken();
			doc = itr.nextToken();
			text.set(classification + ":" + doc);
			// �����ִʺ���
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
			//��һƪ�����ĵ��еĵ��ʱ�����testWordMap��
			for (Text val : values) {
				word = val.toString();
				if (testWordMap.containsKey(word)) {
					testWordMap.put(word, testWordMap.get(word) + 1);
				} else {
					testWordMap.put(word, 1);
				}
			}

			//����һƪ�����ĵ��е�ÿ������
			for (Map.Entry<String, Integer> entry : testWordMap.entrySet()) {
				word = entry.getKey();
				int num = entry.getValue();

				//����ÿ����𣬼�������������������еĸ��ʺ�
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

			// �����������
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
