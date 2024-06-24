package it.unipi.hadoop.InMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import it.unipi.hadoop.CharacterProcessor;

public class LetterFrequency {

    public static class FrequencyLetterMapper extends Mapper<Object, Text, Text, LongWritable> {
        
        // Define a map to store letter counts
        private Map<Text, LongWritable> mapLetter;

        // Override the setup method to initialize the map
        @Override
        protected void setup(Context context) {
            mapLetter = new HashMap<>();
        }

        // Override the map method
        @Override
        protected void map(Object key, Text value, Context context) {
            // Convert the input line to a string
            String line = value.toString();

            // Iterate over each character in the line
            for (char c : line.toCharArray()) {
                // Process the character using CharacterProcessor
                char carattere = CharacterProcessor.processCharacter(c);

                // Check if the character is valid
                boolean check = carattere != 0;

                // Increment the count in the map if the character is valid
                if (check) {
                    Text letter = new Text(String.valueOf(carattere));
                    if (mapLetter.containsKey(letter)) {
                        LongWritable count = mapLetter.get(letter);
                        count.set(count.get() + 1);
                    } else {
                        mapLetter.put(letter, new LongWritable(1));
                    }
                }
            }
        }

        // Override the cleanup method to emit the final key-value pairs
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Text, LongWritable> entry : mapLetter.entrySet()) {
                context.write(entry.getKey(), entry.getValue());
            }
        }
    }

    public static class FrequencyLetterReducer extends Reducer<Text, LongWritable, Text, DoubleWritable> {

        // Define a variable to hold the total count of letters
        private long totalLetter;

        // Override the setup method to retrieve the total letter count from the configuration
        @Override
        protected void setup(Context context) {
            totalLetter = context.getConfiguration().getLong("totalLetters", 1);
        }

        // Override the reduce method
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;

            // Sum up all the values for the given key
            for (LongWritable value : values) {
                sum += value.get();
            }

            // Calculate the frequency of the current letter
            double frequency = (double) sum / totalLetter;

            // Write the frequency to the context
            context.write(key, new DoubleWritable(frequency));
        }
    }
}
