package houseData.beforeStartup;

import com.mapper.BuildIdMapper;
import com.utils.MyConnection;
import com.utils.MybatisUtils;
import org.apache.ibatis.session.SqlSession;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class createBuildId3 {
    private static final String BEGIN_DATE = "2023-03-09";
    private static String DB_URL = "jdbc:mysql://127.0.0.1:3306/HOUSE_INFO?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
    private static Statement buildStatement;
    private static ResultSet buildResultSet;


    public static void main(String agr[]) throws SQLException {
        buildStatement = MyConnection.getStatement(DB_URL,"root","dgsoft");
        SqlSession sqlSession = MybatisUtils.getSqlSession();
        BuildIdMapper buildIdMapper =  sqlSession.getMapper(BuildIdMapper.class);

        try {
            buildResultSet = buildStatement.executeQuery("select * from BUILD order by name");

            Map<String,Object> map = new HashMap<>();
            buildResultSet.last();


            int sumCount = buildResultSet.getRow(),i=0;
            Integer j = buildIdMapper.findMaxId();
            if (j!=null){
                j = j.intValue()+1;
            }else {
                j = 5000;//最大13158 下一起始 15000
            }

            System.out.println("记录总数-"+sumCount);
            buildResultSet.beforeFirst();
            while(buildResultSet.next()){
                map.clear();
                if(buildIdMapper.selectByOldBuildId(buildResultSet.getString("ID")) == null) {
                    System.out.println("j-"+j);

                    map.put("id",j.intValue());
                    map.put("oid",buildResultSet.getString("ID"));
                    map.put("name",buildResultSet.getString("name"));
                    buildIdMapper.addBuildId(map);
                    sqlSession.commit();
                    j++;
                }
                i++;

                System.out.println(i+"/"+String.valueOf(sumCount));
            }
        }catch (Exception e){
            System.out.println("id is errer-----id:"+buildResultSet.getString("ID"));
            e.printStackTrace();
            return;
        }finally {
            if (buildResultSet!=null){
                buildResultSet.close();
            }
            if(buildStatement!=null){
                buildStatement.close();
            }
            sqlSession.close();
            MyConnection.closeConnection();

        }

    }
}
