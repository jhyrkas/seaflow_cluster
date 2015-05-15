package mrresults;

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

public class ResultsMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>{
    EMparams params;

    private static int CHL_INDEX = 3;
    private static double CHL_THRESH  = 10000.0;
    private static double PROB_THRESH = 0.7;
    
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
    // send OPP file and line num info to cluster with highest responsibility
    public void map(LongWritable id, Text line,
            OutputCollector<IntWritable, Text> out, Reporter reporter)
            throws IOException {
        
        OPP opp = new OPP(line.toString());
        
        if (opp.getMeasurements()[CHL_INDEX] < CHL_THRESH) {
            out.collect(new IntWritable(-1), new Text(opp.getFile() + "," + opp.getLineNum()));
            return;
        }
        
        int k = params.getK();
        Gaussian[] gaussians = params.getGaussians();
        double[] responsibilities = new double[k];
        
        
        for (int i = 0; i < k; i++) {
            responsibilities[i] = gaussians[i].log_pdf(opp.getMeasurements()) + Math.log(gaussians[i].getPi());
        }
        
        double denominator = logsumexp(responsibilities);
        
        double max_prob = Double.NEGATIVE_INFINITY;
        int cluster = -1;
        for (int i = 0; i < k; i++) {
            responsibilities[i] = Math.exp(responsibilities[i] - denominator);
            if (responsibilities[i] > max_prob) {
                max_prob = responsibilities[i];
                cluster = i;
            }
        }
        
        if (max_prob < PROB_THRESH) {
            out.collect(new IntWritable(-1), new Text(opp.getFile() + "," + opp.getLineNum()));
        } else {        
            out.collect(new IntWritable(cluster), new Text(opp.getFile() + "," + opp.getLineNum()));
        }
    }
}
