package com.weather;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WeatherMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        // Skip header line
        if (line.contains("date")) {
            return;
        }

        // Split CSV line
        String[] fields = line.split(",");

        if (fields.length < 8) {
            return;
        }
            
        String district = fields[0].trim();
        String date = fields[1].trim();
        String month = fields[2].trim();
        String temp = fields[3].trim();
        String precip = fields[5].trim();

        // Year Filtering
        String yearStr = date.split("-")[0];
        int year = Integer.parseInt(yearStr);

        // Only keep records from past decade (>=2014)
        if (year < 2014) {
            return;  // skip old data
        }

        String outKey = district + "," + month;
        String outValue = precip + "," + temp;

        context.write(new Text(outKey), new Text(outValue));
    }
}
