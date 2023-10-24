package com.utils;

import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.io.IOException;
import java.io.InputStream;

public class MybatisUtils {

    private static SqlSessionFactory sqlSessionFactory = null;
    static
    {
        try {
            String resource = "mybatis-config.xml";
            InputStream  inputStream = Resources.getResourceAsStream(resource);
            sqlSessionFactory = new SqlSessionFactoryBuilder().build(inputStream);
            System.out.println("sqlSessionFactory successful");
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("sqlSessionFactory is errer");
        }
    }
    public static SqlSession getSqlSession(){
        return sqlSessionFactory.openSession();

    }
}
