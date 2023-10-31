package houseData.beforeStartup;

import com.bean.JointCorpDevelop;
import com.mapper.FidCompareMapper;
import com.mapper.JointCorpDevelopMapper;
import com.utils.MyConnection;
import com.utils.MybatisUtils;
import org.apache.ibatis.session.SqlSession;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class createCorpId1  {
    private static final String BEGIN_DATE = "2023-03-09";
    private static String DB_URL = "jdbc:mysql://127.0.0.1:3306/HOUSE_INFO?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
    private static Statement cropStatement;
    private static ResultSet cropResultSet;

    public static void main(String agr[]) throws SQLException {

        cropStatement = MyConnection.getStatement(DB_URL,"root","dgsoft");
        SqlSession sqlSession = MybatisUtils.getSqlSession();
        JointCorpDevelopMapper jointCorpDevelopMapper =  sqlSession.getMapper(JointCorpDevelopMapper.class);
        try {
            cropResultSet = cropStatement.executeQuery("select * from DEVELOPER order by name");

             Map<String,Object> map = new HashMap<>();
             cropResultSet.last();


            int sumCount = cropResultSet.getRow(),i=0;
            Integer j = jointCorpDevelopMapper.findMaxId();
            if (j!=null){
                j = j.intValue()+1;
            }else {
                j = 0;
            }

            System.out.println("记录总数-"+sumCount);
            cropResultSet.beforeFirst();
            while(cropResultSet.next()){
                map.clear();
                if(jointCorpDevelopMapper.selectByDevelopId(cropResultSet.getString("ID")) == null) {
                    System.out.println("j-"+j);

                    map.put("corpid",j.intValue());
                    map.put("developerId",cropResultSet.getString("ID"));
                    map.put("name",cropResultSet.getString("name"));
                    jointCorpDevelopMapper.addJointCorpDevelopMapper(map);
                    sqlSession.commit();

                }
                i++;
                j++;
                System.out.println(i+"/"+String.valueOf(sumCount));
            }
        }catch (Exception e){
            System.out.println("id is errer-----id:"+cropResultSet.getString("ID"));
            e.printStackTrace();
            return;
        }finally {
            if (cropResultSet!=null){
                cropResultSet.close();
            }
            if(cropStatement!=null){
                cropStatement.close();
            }
            sqlSession.close();
            MyConnection.closeConnection();

        }

    }

}
