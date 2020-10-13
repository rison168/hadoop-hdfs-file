package com.hdfs.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Array;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

public class HDFSClient {

    /**
     * 文件上传
     */
    @Test
     public void uploadFile() throws IOException, URISyntaxException, InterruptedException {
         //获取对象
         Configuration configuration = new Configuration();
         FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.0.101:9000"), configuration, "root");

         //执行更名操作
         fileSystem.copyFromLocalFile(new Path("file:///home/rison/data/hadoop-test.txt"), new Path("/hadoop-test.txt"));

         //关闭资源
         fileSystem.close();

         System.out.println("upload file finish!");
     }

    /**
     * 文件下载
     */
    public void downLoadFile() throws IOException, URISyntaxException, InterruptedException {
        //获取对象
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.0.101:9000"), configuration, "root");

        //执行更名操作
        fileSystem.copyToLocalFile(new Path("/test-newName.gz"), new Path("/test-newName.gz"));

        //关闭资源
        fileSystem.close();

        System.out.println("upload file finish!");
    }

    /**
     * 文件删除
     */
    public void deleteFile(){

    }

    /**
     * 文件修改
     */
    @Test
    public void updateFile() throws URISyntaxException, IOException, InterruptedException {
        //获取对象
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.0.101:9000"), configuration, "root");

        //执行更名操作
        fileSystem.rename(new Path("/test.gz"), new Path("/test-newName.gz"));

        //关闭资源
        fileSystem.close();

        System.out.println("update name finish!");
    }

    /**
     * 查看文件信息
     */
    @Test
    public void getFileListInfo() throws URISyntaxException, IOException, InterruptedException {
        //获取对象
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.0.101:9000"), configuration, "root");

        RemoteIterator<LocatedFileStatus> filelist = fileSystem.listFiles(new Path("/"), true);

        while (filelist.hasNext()){
            LocatedFileStatus locatedFileStatus = filelist.next();
            //查看文件名称、长度、块、权限
            // 文件名称git
            System.out.println("文件名：" + locatedFileStatus.getPath().getName());
            //文件长度
            System.out.println("文件大小：" + locatedFileStatus.getLen());
            //文件权限
            System.out.println("文件权限：" + locatedFileStatus.getPermission());
            //文件块
            for (BlockLocation blockLocation : locatedFileStatus.getBlockLocations()){
                System.out.println("block*-> " + blockLocation.getHosts());
            }
            System.out.println("-----*file* ----------------\n");
        }
    }
}
