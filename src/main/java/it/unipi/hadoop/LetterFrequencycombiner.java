package it.unipi.hadoop;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class LetterFrequencycombiner {
    public static class FrequencyLetterMapper extends Mapper <Object, Text, Text, LongWritable> 
    {
        private Text reducerKey= new Text();
        private final static LongWritable reducerValue= new LongWritable(1);
    
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException   {
            String line=value.toString();
            for(char c: line.toCharArray()){
                char carattere=CharacterProcessor.processCharacter(c);
                Boolean check = carattere != 0 ? true : false;
                if(check){
                    reducerKey.set(String.valueOf(carattere));
                    context.write(reducerKey, reducerValue);
                }
                
            }
        }
    
    
        }
        public static class FrequencyLetterCombiner extends Reducer<Text, LongWritable, Text, LongWritable>
        {
            @Override
            public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{
                long sum = 0;
                for(LongWritable value: values){
                    sum += value.get();
                }
                context.write(key, new LongWritable(sum));
            }
        }
    


// creami il reducer che calcoli la frequenza di ogni lettera utilizzando una variabile globale totalLetter già calcolata in LetterCount
public static class FrequencyLetterReducer extends Reducer<Text, LongWritable, Text, DoubleWritable>
{
    private long totalLetter;
    @Override
    public void setup(Context context){
        totalLetter = context.getConfiguration().getLong("totalLetters", 2);
    }
    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{
        long sum = 0;
        for(LongWritable value: values){
            sum += value.get();
        }
        double frequency = (double)sum/totalLetter;
        context.write(key, new DoubleWritable(frequency));
    }

}
}
