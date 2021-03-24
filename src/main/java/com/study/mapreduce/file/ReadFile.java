package com.study.mapreduce.file;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

public class ReadFile {

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(args[0]), conf);
        try (FSDataInputStream in = fs.open(new Path(args[0]))) {
            IOUtils.copy(in, System.out);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
