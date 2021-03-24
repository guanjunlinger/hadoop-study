package com.study.mapreduce.file;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

public class WriteFile {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(args[0]), conf);
        FSDataOutputStream fsDataOutputStream = fs.create(new Path(args[0]), () -> {
            System.out.println("....");
        });
        fsDataOutputStream.writeChars("we are friends");
        fsDataOutputStream.close();
    }
}
