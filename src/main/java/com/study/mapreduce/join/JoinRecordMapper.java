package com.study.mapreduce.join;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class JoinRecordMapper extends Mapper<LongWritable, Text, TextPair, Text> {
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] arr = line.split("\\s+", 2);
        context.write(new TextPair(arr[0], "1"), new Text(arr[1]));
    }
}

