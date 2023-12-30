package houseData.beforeStartup.build;

import com.mapper.build.RBBuildIdMapper;
import com.utils.BuildbatisUtils;
import com.utils.MyConnection;
import org.apache.ibatis.session.SqlSession;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class createRBBuildId {
    private static final String BEGIN_DATE = "2023-03-09";
    private static String DB_URL = "jdbc:mysql://127.0.0.1:3306/HOUSE_INFO?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
    private static Statement statement;
    private static ResultSet resultSet;

    public static void main(String agr[]) throws SQLException {
        statement = MyConnection.getStatement(DB_URL, "root", "dgsoft");
        SqlSession sqlSession = BuildbatisUtils.getSqlSession();
        RBBuildIdMapper rbBuildIdMapper = sqlSession.getMapper(RBBuildIdMapper.class);


        try {
            resultSet = statement.executeQuery("select * from INTEGRATION.build_id");

            Map<String,Object> map = new HashMap<>();
            resultSet.last();
            int sumCount = resultSet.getRow(),i=0;



            System.out.println("记录总数-"+sumCount);
            resultSet.beforeFirst();
            while(resultSet.next()){
                map.clear();
                if(rbBuildIdMapper.selectByOldBuildId(resultSet.getString("ID")) == null) {
                    map.put("id",resultSet.getInt("id"));
                    map.put("oid",resultSet.getString("oid"));
                    rbBuildIdMapper.addBuildId(map);
                    sqlSession.commit();

                }
                i++;

                System.out.println(i+"/"+String.valueOf(sumCount));
            }
        } catch (Exception e) {
            System.out.println("id is errer-----id:" + resultSet.getString("ID"));
            e.printStackTrace();
            return;
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
            if (statement != null) {
                statement.close();
            }
            sqlSession.close();
            MyConnection.closeConnection();

        }

    }
}
