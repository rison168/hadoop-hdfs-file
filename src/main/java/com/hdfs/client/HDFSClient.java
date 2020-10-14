package com.hdfs.client;

import com.sun.scenario.effect.impl.sw.sse.SSEBlend_SRC_OUTPeer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSClient {

    /**
     * 文件上传
     */
    @Test
     public void uploadFile() throws IOException, URISyntaxException, InterruptedException {
         //获取对象
         Configuration configuration = new Configuration();
         FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.1.101:9000"), configuration, "root");

         //执行更名操作
         fileSystem.copyFromLocalFile(new Path("/home/rison/Desktop/jdk-8u251-linux-x64.tar.gz"), new Path("/tst.gz"));

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
    @Test
    public void deleteFile() throws URISyntaxException, IOException, InterruptedException {
        //获取对象
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.1.101:9000"), configuration, "root");
        //如果删除文件夹则循环为true
        fileSystem.delete(new Path("/Ton8PE_V4.0.iso"), false);

        fileSystem.close();

        System.out.println("delete file finish!");
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
        fileSystem.close();
    }

    /**
     * IO 下载文件
     */
    @Test
    public void downloadFileForIO() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.1.101:9000"), configuration, "root");
        //获取输入流
        FSDataInputStream fis = fileSystem.open(new Path("/io-new-file.txt"));
        //获取输出流
        FileOutputStream fio = new FileOutputStream(new File("/home/rison/test-file.txt"));
        //流的对拷
        IOUtils.copyBytes(fis, fio, configuration);
        //关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fio);
        System.out.println("file IO download finish");

    }

    /**
     * IO 上传文件
     */
    @Test
    public void uploadFileForIO() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.1.101:9000"), configuration, "root");
        //获取输入流
        FileInputStream fis = new FileInputStream(new File("/home/rison/data/io-file.txt"));
        //获取输出流
        FSDataOutputStream fio = fileSystem.create(new Path("/io-new-file.txt"));
        //流的对拷
        IOUtils.copyBytes(fis, fio, configuration);
        //关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fio);
        System.out.println("file IO upload finish");

    }
}
