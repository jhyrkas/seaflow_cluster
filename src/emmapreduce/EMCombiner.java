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

public class EMCombiner extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text>{
    @Override
    public void reduce(IntWritable key, Iterator<Text> values,
            OutputCollector<IntWritable, Text> out, Reporter reporter) throws IOException {
        
        ReduceParams params = new ReduceParams(values.next().toString());

        while (values.hasNext()) {
            ReduceParams next = new ReduceParams(values.next().toString());
            
            params.incrementR(next.getR());
            params.incrementN(next.getN());
            params.incrementMeasurements(next.getMeasurements());
            params.incrementSigma(next.getSigma());
        }
        
        if (params.getN() < params.getR()) {
        	System.out.println("PROBLEM: N < R in Combiner");
        	System.out.println("N = " + params.getN() + ", R = " + params.getR());
        }
        
        out.collect(key, new Text(params.toString()));
    }
}
