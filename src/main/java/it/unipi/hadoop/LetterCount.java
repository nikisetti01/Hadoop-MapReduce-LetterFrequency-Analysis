
package it.unipi.hadoop;
import it.unipi.hadoop.CharacterProcessor;
import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;
 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configuration;
public class LetterCount {
    public static class LetterCounterMapper extends Mapper<Object, Text, Text , LongWritable>
    {
        private Text reducerKey= new Text("total_letters");
        private LongWritable reducerValue;
        public void setup(Context context){
            reducerValue = new LongWritable(0);
           
        }
        @Override
        public void map(Object key, Text value, Context context){
            String line=value.toString();
            for(char c: line.toCharArray()){
                char carattere=CharacterProcessor.processCharacter(c);
                Boolean check = carattere != 0 ? true : false;
                if(check)
                    reducerValue.set(reducerValue.get()+1);
                
            }
 
           
        }
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException
        {
            // Emit the final key-value pair
            context.write(reducerKey, reducerValue);
        }
 
   
       
    }
    public static class CounterReducer extends Reducer<Text, LongWritable, Text, LongWritable>
    {
 
        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
        {
            // Initialize the reducer value
            LongWritable reducerValue = new LongWritable();
            reducerValue.set(0);
 
            // Iterate over the values
            for (LongWritable value : values) {
                reducerValue.set(reducerValue.get() + value.get());
            }
 
            // Write the output
            context.write(key, reducerValue);
        }
    }
  
}
 
 
 
 
 