package com.weather;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WeatherMaxPrecipReducer extends Reducer<Text, Text, Text, Text> {

    // Global maximum (for all months)
    private double maxTotal = Double.NEGATIVE_INFINITY;
    private String maxMonthYear = "";

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        double sum = 0.0;

        // Sum precipitation for this year-month
        for (Text v : values) {
            String s = v.toString();
            try {
                sum += Double.parseDouble(s);
            } catch (NumberFormatException e) {
                // bad value → skip
            }
        }

        // Update global max if this month is bigger
        if (sum > maxTotal) {
            maxTotal = sum;
            maxMonthYear = key.toString();
        }
    }

    @Override
    public void cleanup(Context context)
            throws IOException, InterruptedException {

        // After all keys are processed → output only ONE result
        if (!maxMonthYear.isEmpty() && maxTotal > Double.NEGATIVE_INFINITY) {
            context.write(
                    new Text("Max month-year: " + maxMonthYear),
                    new Text("Total precipitation: " + maxTotal));
        }
    }
}
