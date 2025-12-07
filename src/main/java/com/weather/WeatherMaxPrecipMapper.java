package com.weather;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WeatherMaxPrecipMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        // Skip header row
        if (line.startsWith("city_name")) {
            return;
        }

        // Split CSV line
        String[] fields = line.split(",");

        // Safety: need at least 5 columns
        if (fields.length < 5) {
            return;
        }

        String date = fields[1].trim(); // yyyy-mm-dd
        String precipStr = fields[4].trim(); // precipitation_sum

        if (precipStr.isEmpty()) {
            return; // skip empty precipitation
        }

        // Extract year and month from date
        String[] dateParts = date.split("-");
        if (dateParts.length < 2) {
            return; // bad date
        }

        String year = dateParts[0];
        String month = dateParts[1]; // "01", "02", ...

        // Build key: "year-month" e.g. "2019-02"
        String keyOut = year + "-" + month;

        // Send to reducer
        context.write(new Text(keyOut), new Text(precipStr));
    }
}
