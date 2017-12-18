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

public class Evaluation {
	// 对test作业产生的结果进行评估，读取test作业的output
	public static void evaluation() {
		HashMap<String, Integer> realNum = new HashMap<String, Integer>();// 真实A类样本个数
		HashMap<String, Integer> predictNum = new HashMap<String, Integer>();// 预测为A类的样本数
		HashMap<String, Integer> correctNum = new HashMap<String, Integer>();// 正确预测为A类的样本个数
		// 准确率=correctNum/predictNum 召回率=correctNum/realNum；

		Configuration conf = new Configuration();
		String filePath = "/Bayes/output/final/part-r-00000";

		FSDataInputStream fsr = null;
		BufferedReader bufferedReader = null;
		String lineTxt = null;
		String array[];
		String array2[];
		String realClassification;
		String predictClassification;
		try {
			FileSystem fs = FileSystem.get(URI.create(filePath), conf);
			fsr = fs.open(new Path(filePath));
			bufferedReader = new BufferedReader(new InputStreamReader(fsr));
			while ((lineTxt = bufferedReader.readLine()) != null) {
				array = lineTxt.split("	");
				array2 = array[0].split(":");
				predictClassification = array[1];
				realClassification = array2[0];
				if (realNum.containsKey(realClassification)) {
					realNum.put(realClassification, realNum.get(realClassification) + 1);
				} else {
					realNum.put(predictClassification, 1);
				}
				if (predictNum.containsKey(predictClassification)) {
					predictNum.put(predictClassification, predictNum.get(predictClassification) + 1);
				} else {
					predictNum.put(predictClassification, 1);
				}
				if (predictClassification.equals(realClassification)) {
					if (correctNum.containsKey(realClassification)) {
						correctNum.put(realClassification, correctNum.get(realClassification) + 1);
					} else {
						correctNum.put(realClassification, 1);
					}
				}
			}

			System.out.println("----------------------------------");
			int sumDoc = 0;
			for (Map.Entry<String, Integer> entry : realNum.entrySet()) {
				String classification = entry.getKey();
				int realNumber = entry.getValue();
				sumDoc += realNumber;

				int correctNunber = correctNum.get(classification);
				int predictNumber = predictNum.get(classification);

				double P = correctNunber * 1.0 / predictNumber;
				double R = correctNunber * 1.0 / realNumber;
				double F = 2*P*R/(P+R);
				System.out.println(classification + "的准确率 = " + P);
				System.out.println(classification + "的召回率 = " + R);
				System.out.println(classification + "的F = " + F);
				System.out.println();
			}
			int correctTotalNunber = 0;
			for (Map.Entry<String, Integer> entry : correctNum.entrySet()) {
				correctTotalNunber += entry.getValue();
			}
			System.out.println("Micro-average = " + correctTotalNunber * 1.0 / sumDoc);
			System.out.println("----------------------------------");
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
	}
}
