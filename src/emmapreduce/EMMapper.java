package emmapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import em.*;
import seaflow.*;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;

public class EMMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>{
	EMparams params;
	
	
	@Override
	public void configure(JobConf job) {
		super.configure(job);
		ArrayList<String> lines = new ArrayList<String>();
		try {
			Path[] clusterFiles = DistributedCache.getLocalCacheFiles(job);
			for (Path clusterFile : clusterFiles) {
				loadLines(clusterFile, lines);
			}
			params = new EMparams(lines);
		} catch (IOException e) {
			e.printStackTrace();
			System.err.println("Caught exception while getting cached files: " + StringUtils.stringifyException(e));
		}
	}
	
	private void loadLines(Path path, ArrayList<String> lines) {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(path.toString()));
			String line = null;
			while ((line = reader.readLine()) != null) {
				lines.add(line);
			}
			
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
			System.err.println("Caught exception while parsing the cached file '" + path + "' : " + StringUtils.stringifyException(e));
		}
	}
	
	private double logsumexp(double[] d) {
	    double maxterm = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < d.length; i++) {
            if (d[i] > maxterm) {
                maxterm = d[i];
            }
        }
        
        System.out.println("maxterm: " + maxterm);
        
        double sum = 0.0;
        for (int i = 0; i < d.length; i++) {
            sum += Math.exp(d[i] - maxterm);
        }
        
        return maxterm + Math.log(sum);
	}
	
	@Override
	public void map(LongWritable id, Text line,
			OutputCollector<IntWritable, Text> out, Reporter reporter)
			throws IOException {
		
	    // easiest thing is for astronomy data to be transformed into opp format:
	    // file no, line no, num dimensions, measurements....
		OPP opp = new OPP(line.toString());
		
		int k = params.getK();
		Gaussian[] gaussians = params.getGaussians();
		double[] responsibilities = new double[k];
		int dimensions = gaussians[0].getDimensions();
		
		
		for (int i = 0; i < k; i++) {
		    responsibilities[i] = gaussians[i].log_pdf(opp.getMeasurements()) + Math.log(gaussians[i].getPi());
		}
		
		double denominator = logsumexp(responsibilities);
		
		//nope
		//for (int i = 0; i < k; i++) {
		//	responsibilities[i] = (gaussians[i].pdf(opp.getMeasurements()) * gaussians[i].getPi());
		//	denominator += responsibilities[i];
		//}
		
		for (int i = 0; i < k; i++) {
			responsibilities[i] = Math.exp(responsibilities[i] - denominator);
			double[][] partialSig = gaussians[i].partialSigma(opp.getMeasurements(), responsibilities[i]);
			double[] partialMean = new double[dimensions];
			double[] m = opp.getMeasurements();
			
			for (int j = 0; j < dimensions; j++) {
				partialMean[j] = m[j] * responsibilities[i];
			}
			ReduceParams rp = new ReduceParams(responsibilities[i], 1, partialMean, partialSig);
			out.collect(new IntWritable(i), new Text(rp.toString()));
		}
		
		for (int i = 0; i < k; i++) {
			if (responsibilities[i] > 1) {
				System.out.println("PROBLEM: responsibility of k = " + i + " is " + responsibilities[i]);
			}
		}
		
	}
	
	/*
	private static double lse(double[] d) {
        double maxterm = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < d.length; i++) {
            if (d[i] > maxterm) {
                maxterm = d[i];
            }
        }
        
        System.out.println("maxterm: " + maxterm);
        
        double sum = 0.0;
        for (int i = 0; i < d.length; i++) {
            sum += Math.exp(d[i] - maxterm);
        }
        
        return maxterm + Math.log(sum);
    }
	
	public static void main(String[] args) {
	    double[] mean = {26528,12637,1824,28493};
        double[][] sigma = new double[4][4];
        sigma[0][0] = 1; sigma[0][1] = 0; sigma[0][2] = 0; sigma[0][3] = 0;
        sigma[1][0] = 0; sigma[1][1] = 1; sigma[1][2] = 0; sigma[1][3] = 0;
        sigma[2][0] = 0; sigma[2][1] = 0; sigma[2][2] = 1; sigma[2][3] = 0;
        sigma[3][0] = 0; sigma[3][1] = 0; sigma[3][2] = 0; sigma[3][3] = 1;
        
        double pi = 1.0 / 2;
        
        double[] mean2 = {42528,27171,8469,48677};
        
        Gaussian g = new Gaussian(mean, sigma, pi);
        Gaussian g2 = new Gaussian(mean2, sigma, pi);
        double[] measurements = {30755,11771,25587,17613};
        double r1 = g.log_pdf(measurements) + Math.log(g.getPi());
        double r2 = g2.log_pdf(measurements)+ Math.log(g2.getPi());
        double[] rs = {r1, r2};
        double lse = lse(rs);
        double p1 = Math.exp(r1 - lse);
        double p2 = Math.exp(r2 - lse);
        System.out.println(p1);
        System.out.println(p2);
	}*/
}
