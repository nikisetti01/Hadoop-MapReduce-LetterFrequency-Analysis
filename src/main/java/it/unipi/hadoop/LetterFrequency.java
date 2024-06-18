package it.unipi.hadoop;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class LetterFrequency {
    public static class FrequencyLetterMapper extends Mapper <Object, Text, Text, LongWritable>
    {
        private Map<Text,LongWritable> mapletter;
    
        @Override
        public void setup(Context context)
        {
            // Initialize the map
            mapletter = new HashMap<Text, LongWritable>();
        }
        @Override
        public void map(Object key, Text value, Context context){
            String line=value.toString();
            for(char c: line.toCharArray()){
                char carattere=CharacterProcessor.processCharacter(c);
                Boolean check = carattere != 0 ? true : false;
                if(check){
                  if(mapletter.containsKey(new Text(String.valueOf(carattere)))){
                    LongWritable count = mapletter.get(new Text(String.valueOf(carattere)));
                    count.set(count.get()+1);
                    mapletter.put(new Text(String.valueOf(carattere)), count);

                  }else{
                    mapletter.put(new Text(String.valueOf(carattere)), new LongWritable(1));
                  }
                
            }
        }
    
    
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException{
            for(Map.Entry<Text, LongWritable> entry :mapletter.entrySet()){
                context.write(entry.getKey(), entry.getValue());
            }
        }


}
// creami un reducer che calcoli la frequenza di ogni lettera utilizzando una variabile globale totalLetter già calcolata in LetterCount
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
        double frequency = (double)sum/(double)totalLetter;
        context.write(key, new DoubleWritable(frequency));
    }


}


}
