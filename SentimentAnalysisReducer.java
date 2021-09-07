import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SentimentAnalysisReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
    	int possum = 0;
    	int negsum = 0;
        for (IntWritable val : values) {
        	if(val.get()==0) {
        		negsum += 1;								//count number of neg comments
        	}
        	else if(val.get()==1) {
        		possum += 1;								//count number of positive comments
        	}
        }
        if (possum >= negsum){								//if majority pos, keyword is pos and vice versa
        	key.set("KEY WORD IS POSITIVE -> ");
        	result.set(1);
        }
        else{
        	key.set("KEY WORD IS NEGATIVE -> ");
        	result.set(0);
        }
        context.write(key, result);
    }
}