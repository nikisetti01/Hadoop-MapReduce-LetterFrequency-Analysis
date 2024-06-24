package it.unipi.hadoop.Combiner;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import it.unipi.hadoop.CharacterProcessor;

public class LetterFrequency {

    public static class FrequencyLetterMapper extends Mapper<Object, Text, Text, LongWritable> {
        
        // Define the key and value to be used in the reducer
        private Text reducerKey = new Text();
        private static final LongWritable reducerValue = new LongWritable(1);

        // Override the map method
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Convert the input line to a string
            String line = value.toString();

            // Iterate over each character in the line
            for (char c : line.toCharArray()) {
                // Process the character using CharacterProcessor
                char carattere = CharacterProcessor.processCharacter(c);

                // Check if the character is valid
                boolean check = carattere != 0;

                // Write the key-value pair to the context if the character is valid
                if (check) {
                    reducerKey.set(String.valueOf(carattere));
                    context.write(reducerKey, reducerValue);
                }
            }
        }
    }

    // Combiner class for summing up letter counts
    public static class FrequencyLetterCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {

        // Override the reduce method
        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;

            // Sum up all the values for the given key
            for (LongWritable value : values) {
                sum += value.get();
            }

            // Write the intermediate sum to the context
            context.write(key, new LongWritable(sum));
        }
    }

    public static class FrequencyLetterReducer extends Reducer<Text, LongWritable, Text, DoubleWritable> {

        // Define a variable to hold the total count of letters
        private long totalLetter;

        // Override the setup method to retrieve the total letter count from the configuration
        @Override
        public void setup(Context context) {
            totalLetter = context.getConfiguration().getLong("totalLetters", 1);
        }

        // Override the reduce method
        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
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
