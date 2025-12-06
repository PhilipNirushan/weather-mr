package com.weather;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.Text;

import java.io.IOException;

import javax.naming.Context;

public class WeatherMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        // Skip header row
        if (line.startsWith("district")) {
            return;
        }

        String[] parts = line.split(",");

        if (parts.length < 5)
            return;

        String district = parts[0].trim();
        String month = parts[2].trim();
        String temp = parts[3].trim();
        String precip = parts[4].trim();

        String outKey = district + "," + month;
        String outValue = precip + "," + temp;

        context.write(new Text(outKey), new Text(outValue));
    }
}
