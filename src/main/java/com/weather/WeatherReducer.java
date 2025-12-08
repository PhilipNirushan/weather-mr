package com.weather;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WeatherReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        double totalPrecip = 0;
        double totalTemp = 0;
        int count = 0;

        for (Text val : values) {
            String[] parts = val.toString().split(",");
            double precip = Double.parseDouble(parts[0]);
            double temp = Double.parseDouble(parts[1]);

            totalPrecip += precip;
            totalTemp += temp;
            count++;
        }

        double meanTemp = totalTemp / count;
        String meanTempFormatted = String.format("%.2f", meanTemp);
        String result = "Total Precipitation =" + totalPrecip + "hours, Mean Temperature=" + meanTempFormatted;

        context.write(key, new Text(result));
    }
}
