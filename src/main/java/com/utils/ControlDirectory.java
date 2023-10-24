package com.utils;

import java.io.File;

public class ControlDirectory {


    public static void deleteDirectory(File directory) {
        File[] filesAndDirs = directory.listFiles();
        if (filesAndDirs != null) {
            for (File fileOrDir : filesAndDirs) {
                if (fileOrDir.isDirectory()) {
                    // 递归删除子目录
                    deleteDirectory(fileOrDir);
                } else {
                    // 删除文件
                    fileOrDir.delete();
                }
            }
        }
        // 删除目录
        directory.delete();
    }


    //删除父子目录所有文件 保留父目录
    public static void DeleteFilesAndSubdirectories (File directory) {
        if (directory.exists() && directory.isDirectory()) {
            // 获取目录中的所有文件和子目录
            File[] filesAndDirs = directory.listFiles();

            if (filesAndDirs != null) {
                for (File fileOrDir : filesAndDirs) {
                    if (fileOrDir.isDirectory()) {
                        // 递归删除子目录
                        deleteDirectory(fileOrDir);
                    } else {
                        // 删除文件
                        fileOrDir.delete();
                    }
                }
            }
            System.out.println("目录中的所有文件和子目录已删除。");
        } else {
            System.out.println("目录不存在或不是一个目录。");
        }
    }
    public static void createDirectory(String directoryPath){
        File directory = new File(directoryPath);
        // 删除目录中的所有文件和子目录
        DeleteFilesAndSubdirectories(directory);
        // 检查目录是否存在，如果不存在则创建
        if (!directory.exists()) {
            boolean created = directory.mkdirs(); // 创建目录及其父目录
            if (created) {
                System.out.println("目录已成功创建。");
            } else {
                System.out.println("无法创建目录。");
            }
        } else {
            System.out.println("目录已经存在。");
        }
    }
}
