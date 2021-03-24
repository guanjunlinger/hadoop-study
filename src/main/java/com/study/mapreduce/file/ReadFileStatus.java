package com.study.mapreduce.file;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

public class ReadFileStatus {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(args[0]), conf);
        FileStatus fileStatus = fs.getFileStatus(new Path(args[0]));
        if (fileStatus.isDirectory()) {
            FileStatus[] fileStatuses = fs.listStatus(fileStatus.getPath());
            System.out.println(fileStatuses.length);
        } else {
            System.out.println(fileStatus.getBlockSize());
        }
    }

}
