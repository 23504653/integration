package houseData;

import com.mapper.FidCompareMapper;
import com.utils.ControlDirectory;
import com.utils.MyConnection;
import com.utils.MybatisUtils;
import org.apache.ibatis.session.SqlSession;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;



public class seaweedMain {




    private static final String BEGIN_DATE = "2023-10-12";
    //临时
    private static final String directoryPath = "E:\\weedfsPicture";
    private static final String SEAWEED_ERROR_FILE="/SeaweedError.sql";
    private static BufferedWriter SeaweedWriterError;
    private static File SeaweedFileError;
    private static Statement seaweedStatement;
    private static ResultSet seaweedResultSet;

    public static void main(String agr[]) throws SQLException {

        SeaweedFileError = new File(SEAWEED_ERROR_FILE);
        if (SeaweedFileError.exists()){
            SeaweedFileError.delete();
        }
        try{
            SeaweedFileError.createNewFile();
            FileWriter fw = new FileWriter(SeaweedFileError.getAbsoluteFile());
            SeaweedWriterError = new BufferedWriter(fw);
            SeaweedWriterError.write("错误记录");
            SeaweedWriterError.newLine();
            SeaweedWriterError.flush();
        }catch (IOException e){
            System.out.println("sqlWriter 文件创建失败");
            e.printStackTrace();
            return;
        }


        seaweedStatement = MyConnection.getStatement();
        SqlSession sqlSession = MybatisUtils.getSqlSession();
        FidCompareMapper fidCompareMapper =  sqlSession.getMapper(FidCompareMapper.class);

        try {
            seaweedResultSet = seaweedStatement.executeQuery("SELECT ID"
                    +" FROM UPLOAD_FILE"
                    +" ORDER BY UPLOAD_TIME");
            seaweedResultSet.last();
            int sumCount = seaweedResultSet.getRow(),i=0;
            System.out.println("记录总数-"+sumCount);
            seaweedResultSet.beforeFirst();
            while (seaweedResultSet.next()){
//             查询表已有操作记录，没有的话才操作
               if(fidCompareMapper.selectOne(seaweedResultSet.getString("ID")) == null) {
                   //每次建立图片保存目录，并清空目录
                   ControlDirectory.createDirectory(directoryPath);




               }
               i++;
               System.out.println(i+"/"+String.valueOf(sumCount)+"--ID:"+seaweedResultSet.getString("ID"));
            }

        }catch (Exception e){
            System.out.println("id is errer-----id:"+seaweedResultSet.getString("id"));
            e.printStackTrace();
            return;
        }finally {
            sqlSession.close();
        }



    }
}
