package com.study.mapreduce.join.SemiJoin;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

public class JoinRecordMapper extends Mapper<LongWritable, Text, TextPair, Text> {

    private Set<String> userIds = new HashSet<>();

    @Override
    protected void setup(Context context) throws IOException {
        URI[] cacheFiles = context.getCacheFiles();
        URI pdUri = cacheFiles[0];
        String path = pdUri.getPath();

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
        String line;
        while (null != (line = bufferedReader.readLine())) {
            userIds.add(line);
        }
    }

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] arr = line.split("\\s+", 2);
        if (userIds.contains(arr[0])) {
            context.write(new TextPair(arr[0], "1"), new Text(arr[1]));
        }
    }
}

