package it.unipi.hadoop.InMapper;

import it.unipi.hadoop.CharacterProcessor;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configuration;

public class LetterCount {

    public static class LetterCounterMapper extends Mapper<Object, Text, Text, LongWritable> {

        // Define the key and value to be used in the reducer
        private Text reducerKey = new Text("total_letters");
        private LongWritable reducerValue;

        // Override the setup method to initialize the reducer value
        @Override
        protected void setup(Context context) {
            reducerValue = new LongWritable(0);
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

                // Increment the count if the character is valid
                if (check) {
                    reducerValue.set(reducerValue.get() + 1);
                }
            }
        }

        // Override the cleanup method to emit the final key-value pair
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(reducerKey, reducerValue);
        }
    }

    public static class LetterCounterReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        // Override the reduce method
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            // Initialize the reducer value
            long sum = 0;

            // Sum up all the values for the given key
            for (LongWritable value : values) {
                sum += value.get();
            }

            // Write the final aggregated count to the context
            context.write(key, new LongWritable(sum));
        }
    }
}
