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
            resultSet = statement.executeQuery("SELECT A.*,REPLACE(A.NBNAME , A.SNAME, '') AS newB FROM " +
                    "(SELECT B.ID,B.NAME AS BNAME,P.NAME AS PNAME,S.NAME AS SNAME,D.NAME AS DNAME, REPLACE(B.NAME , D.NAME, '') AS NBNAME " +
                    "FROM BUILD AS B LEFT JOIN PROJECT AS P ON B.PROJECT_ID=P.ID " +
                    "                LEFT JOIN SECTION AS S ON P.SECTIONID = S.ID " +
                    "                LEFT JOIN DISTRICT AS D ON S.DISTRICT = D.ID) AS A order by A.BNAME;");

            Map<String,Object> map = new HashMap<>();
            resultSet.last();

            int sumCount = resultSet.getRow(),i=0;
            Integer j = rbBuildIdMapper.findMaxId();
            if (j!=null){
                j = j.intValue()+1;
            }else {
                j = 5000;//最大13158 下一起始 15000
            }

            System.out.println("记录总数-"+sumCount);
            resultSet.beforeFirst();
            while(resultSet.next()){
                map.clear();
                if(rbBuildIdMapper.selectByOldBuildId(resultSet.getString("ID")) == null) {
                    System.out.println("j-"+j);

                    map.put("id",j.intValue());
                    map.put("oid",resultSet.getString("ID"));
                    rbBuildIdMapper.addBuildId(map);
                    sqlSession.commit();
                    j++;
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
