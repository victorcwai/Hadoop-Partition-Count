package polyu.bigdata;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class PartitionCounting {
	

    public static class PartitionMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
    	
        private final static IntWritable one = new IntWritable(1);
        private IntWritable part = new IntWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 
	        //insert your codes here
            Configuration conf = context.getConfiguration();
            int min = Integer.parseInt(conf.get("min_value"));
            int max = Integer.parseInt(conf.get("max_value"));
            int num_part = Integer.parseInt(conf.get("num_partitions"));
        	int rangePerSet = (max-min)/num_part;
            int score;
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
            	score = Integer.parseInt(itr.nextToken());
            	for(int i=0; i<num_part; i++){
            		if(score > min+i*rangePerSet && score <= min+(i+1)*rangePerSet){
                    	//part: 0->0 to 20; 1->20 to 40; ..... 4->80 to 100
            			part.set(i);
            			System.out.println("score outputed:" + score + " part:" + part);
            			//always output one
                        context.write(part,one);
            		}
            	}
            }
        }
    }
    public static class PartitionReducer extends Reducer<IntWritable, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        private Text range = new Text();
        
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        	//insert your codes here
            Configuration conf = context.getConfiguration();
            int min = Integer.parseInt(conf.get("min_value"));
            int max = Integer.parseInt(conf.get("max_value"));
            int num_part = Integer.parseInt(conf.get("num_partitions"));
        	int rangePerSet = (max-min)/num_part;
        	
            int sum = 0;
            for (IntWritable val : values) {
              sum += val.get();
            }
            result.set(sum);
           	range.set(Integer.toString(min+key.get()*rangePerSet) + "," + Integer.toString(min+(key.get()+1)*rangePerSet));
           	
            context.write(range, result);
        }
    }
}
