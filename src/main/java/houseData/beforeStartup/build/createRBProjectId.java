package houseData.beforeStartup.build;

import com.mapper.build.RBProjectIdMapper;
import com.utils.BuildbatisUtils;
import com.mapper.build.RBBuildIdMapper;
import com.utils.MyConnection;
import org.apache.ibatis.session.SqlSession;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
public class createRBProjectId {

    private static final String BEGIN_DATE = "2023-03-09";
    private static String DB_URL = "jdbc:mysql://127.0.0.1:3306/HOUSE_INFO?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
    private static Statement projectStatement;
    private static ResultSet projectResultSet;

    public static void main(String agr[]) throws SQLException {
        projectStatement = MyConnection.getStatement(DB_URL,"root","dgsoft");
        SqlSession sqlSession = BuildbatisUtils.getSqlSession();
        RBProjectIdMapper rbProjectIdMapper =  sqlSession.getMapper(RBProjectIdMapper.class);

        try {
            projectResultSet = projectStatement.executeQuery("select * from INTEGRATION.project_Id ");

            Map<String,Object> map = new HashMap<>();
            projectResultSet.last();


            int sumCount = projectResultSet.getRow(),i=0;

            System.out.println("记录总数-"+sumCount);
            projectResultSet.beforeFirst();
            while(projectResultSet.next()){
                map.clear();
                if(rbProjectIdMapper.selectByOldProjectId(projectResultSet.getString("ID")) == null) {

                    map.put("id",projectResultSet.getInt("id"));
                    map.put("oid",projectResultSet.getString("oid"));
                    rbProjectIdMapper.addProjectId(map);
                    sqlSession.commit();

                }
                i++;

                System.out.println(i+"/"+String.valueOf(sumCount));
            }
        }catch (Exception e){
            System.out.println("id is errer-----id:"+projectResultSet.getString("ID"));
            e.printStackTrace();
            return;
        }finally {
            if (projectResultSet!=null){
                projectResultSet.close();
            }
            if(projectStatement!=null){
                projectStatement.close();
            }
            sqlSession.close();
            MyConnection.closeConnection();

        }

    }
}
