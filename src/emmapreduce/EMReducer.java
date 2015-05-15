package emmapreduce;

import java.io.IOException;
import java.util.Iterator;

import em.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class EMReducer extends MapReduceBase implements Reducer<IntWritable, Text, Text, Text>{
	
	@Override
	public void reduce(IntWritable key, Iterator<Text> values,
			OutputCollector<Text, Text> out, Reporter reporter) throws IOException {
		
		double[] mean = null;
		double[][] sigma = null;
		double rk = 0.0;
		int N = 0;
		int dimensions = -1;
		
		while (values.hasNext()) {
			Text line = values.next();
			ReduceParams params = new ReduceParams(line.toString());
			
			if (dimensions == -1) {
				dimensions = params.getDimensions();
				mean = new double[dimensions];
				sigma = new double[dimensions][dimensions];
			}
			
			rk += params.getR();
			N += params.getN();
			double[] m = params.getMeasurements();
			double[][] s = params.getSigma();
			for (int i = 0; i < dimensions; i++) {
				mean[i] += m[i];
				for (int j = 0; j< dimensions; j++) {
					sigma[i][j] += s[i][j];
				}
			}
		}
				
		double pi = rk / N;
		if (pi > 1) {
			System.out.println("PROBLEM: pi = " + pi);
		}
		for (int i = 0; i < dimensions; i++) {
			mean[i] /= rk;
			for (int j = 0; j< dimensions; j++) {
				if (sigma[i][j] / rk == 0.0) {
					System.out.println("PROBLEM: " + sigma[i][j] + " / " + rk + " = " + sigma[i][j] / rk);
				}
				sigma[i][j] /= rk;
			}
		}
		
		Gaussian g = new Gaussian(mean, sigma, pi, dimensions);
		out.collect(new Text("cluster" + key.get()), new Text(g.toString()));
	}
}
