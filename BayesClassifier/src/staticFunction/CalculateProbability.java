package staticFunction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CalculateProbability {
	/*
	 * ��ѵ�����еĴ������B
	 */
	public static int B;

	public static int getB() {
		Configuration conf = new Configuration();
		String filePath = "/Bayes/output/out2/part-r-00000";

		FSDataInputStream fsr = null;
		BufferedReader bufferedReader = null;
		String lineTxt = null;
		String array[];
		String array2[];
		HashMap<String, Integer> wordB = new HashMap<String, Integer>();
		try {
			FileSystem fs = FileSystem.get(URI.create(filePath), conf);
			fsr = fs.open(new Path(filePath));
			bufferedReader = new BufferedReader(new InputStreamReader(fsr));
			while ((lineTxt = bufferedReader.readLine()) != null) {
				array = lineTxt.split("	");
				array2 = array[0].split(":");
				String word = array2[1];
				int num = Integer.parseInt(array[1]);
				if (wordB.containsKey(word)) {
					wordB.put(word, wordB.get(word) + num);
				} else {
					wordB.put(word, num);
				}
			}
			B = wordB.size();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (bufferedReader != null) {
				try {
					bufferedReader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		return B;
	}

	/*
	 * ����job1�õ��Ľ��������������� �������P(c)=��c���ļ�����/����ѵ���������ļ�����
	 * ����:��Ӧ��һ��MapReduce�����args[1] 
	 * ����:�õ�HashMap<String,Double>��ŵ���<����,����>
	 */
	public static HashMap<String, Double> priorProbability = new HashMap<String, Double>();
	public static HashMap<String, Integer> classificationArray = new HashMap<String, Integer>();
	// static List<String> classificationArray = new ArrayList<String>();

	public static HashMap<String, Double> GetPriorProbability() throws IOException {

		Configuration conf = new Configuration();
		String filePath = "/Bayes/output/out1/part-r-00000";

		double totalDocs = 0;
		FSDataInputStream fsr = null;
		BufferedReader bufferedReader1 = null;
		BufferedReader bufferedReader2 = null;
		String lineTxt = null;
		String array[];
		try {
			FileSystem fs1 = FileSystem.get(URI.create(filePath), conf);
			fsr = fs1.open(new Path(filePath));
			bufferedReader1 = new BufferedReader(new InputStreamReader(fsr));
			while ((lineTxt = bufferedReader1.readLine()) != null) {
				array = lineTxt.split("	");
				totalDocs += Integer.parseInt(array[1]);
				if (!classificationArray.containsKey(array[0])) {
					classificationArray.put(array[0], 1);
				}
			}

			FileSystem fs2 = FileSystem.get(URI.create(filePath), conf);
			fsr = fs2.open(new Path(filePath));
			bufferedReader2 = new BufferedReader(new InputStreamReader(fsr));
			while ((lineTxt = bufferedReader2.readLine()) != null) {
				array = lineTxt.split("	");
				priorProbability.put(array[0], Integer.parseInt(array[1]) * 1.0 / totalDocs);// �����ĵ��ĸ���
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (bufferedReader1 != null) {
				try {
					bufferedReader1.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (bufferedReader2 != null) {
				try {
					bufferedReader2.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		// ��֤�Ƿ�õ��������
		// for (Map.Entry<String, Double> entry : priorProbability.entrySet()) {
		// String mykey = entry.getKey().toString();
		// double myvalue = Double.parseDouble(entry.getValue().toString());
		// System.out.println(mykey + "\t" + myvalue + "\t");
		// }

		return priorProbability;
	}

	/*
	 * ������������ 
	 * �þ�̬������������������ڸ��������ռ�ĸ���,������������P(word|c)=����c�°�������word������+1��/(��c�µ�������+B) 
	 * ����:�ڶ���MapReduce�����[2](��C�и��������ڳ��ֵ��ĵ�����)
	 * ���:�õ�HashMap<String,Double>��ŵ���<<����:����>,����>
	 */
	public static HashMap<String, Double> wordsProbability = new HashMap<String, Double>();

	public static HashMap<String, Double> GetConditionProbability() throws IOException {
		int BB = getB();

		HashMap<String, Integer> classificationTotalWord = new HashMap<String, Integer>();
		Configuration conf = new Configuration();
		String classificationTotalWordPath = "/Bayes/output/out3/part-r-00000";
		String WordInclassificationPath = "/Bayes/output/out2/part-r-00000";

		FSDataInputStream fsr1 = null;
		BufferedReader bufferedReader1 = null;
		String lineTxt = null;
		String array[];
		try {
			FileSystem fs1 = FileSystem.get(URI.create(classificationTotalWordPath), conf);
			fsr1 = fs1.open(new Path(classificationTotalWordPath));
			bufferedReader1 = new BufferedReader(new InputStreamReader(fsr1));
			while ((lineTxt = bufferedReader1.readLine()) != null) {
				array = lineTxt.split("	");
				classificationTotalWord.put(array[0], Integer.parseInt(array[1]));
				// System.out.println(lineTxt);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (bufferedReader1 != null) {
				try {
					bufferedReader1.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		// ��֤�Ƿ�õ��༰�൥������
		// for (Map.Entry<String, Integer> entry :
		// classificationTotalWord.entrySet()) {
		// String mykey = entry.getKey();
		// int myvalue = entry.getValue();
		// System.out.println(mykey + "\t" + myvalue);
		// }

		FSDataInputStream fsr2 = null;
		BufferedReader bufferedReader2 = null;
		String array2[];
		try {
			FileSystem fs2 = FileSystem.get(URI.create(WordInclassificationPath), conf);
			fsr2 = fs2.open(new Path(WordInclassificationPath));
			bufferedReader2 = new BufferedReader(new InputStreamReader(fsr2));
			while ((lineTxt = bufferedReader2.readLine()) != null) {
				array = lineTxt.split("	");// <<classification:word>,number>
				array2 = array[0].split(":");// <classification:word>
				wordsProbability.put(array[0],
						(Integer.parseInt(array[1]) + 1) * 1.0 / (classificationTotalWord.get(array2[0]) + BB));
			}

			// ����ͬһ�����û�г��ֹ��ĵ��ʵĸ���һ����1/(classificationTotalDocNums.get(newKey.toString())+2)
			// �����࣬ÿ��������ټ�һ��û�г��ֵ��ʵĸ��ʣ����ʽΪ<classification,probability>
			for (Map.Entry<String, Integer> entry : classificationTotalWord.entrySet()) {
				wordsProbability.put(entry.getKey().toString(),
						1.0 / (classificationTotalWord.get(entry.getKey().toString()) + BB));
				// System.out.println(entry.getKey().toString() + "\t" +
				// 1.0/(classificationTotalWord.get(entry.getKey().toString())+BB));
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (bufferedReader2 != null) {
				try {
					bufferedReader2.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		// ��֤�Ƿ�õ���������
		// for (Map.Entry<String, Double> entry : wordsProbability.entrySet()) {
		// String mykey = entry.getKey();
		// double myvalue = entry.getValue();
		// System.out.println(mykey + "\t" + myvalue);
		// }

		return wordsProbability;
	}
}
