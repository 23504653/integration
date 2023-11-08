package houseData.beforeStartup;

import com.mapper.JointCorpDevelopMapper;
import com.mapper.ProjectIdMapper;
import com.utils.MyConnection;
import com.utils.MybatisUtils;
import org.apache.ibatis.session.SqlSession;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class createProjectId2 {
    private static final String BEGIN_DATE = "2023-03-09";
    private static String DB_URL = "jdbc:mysql://127.0.0.1:3306/HOUSE_INFO?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
    private static Statement projectStatement;
    private static ResultSet projectResultSet;

    public static void main(String agr[]) throws SQLException {
        projectStatement = MyConnection.getStatement(DB_URL,"root","dgsoft");
        SqlSession sqlSession = MybatisUtils.getSqlSession();
        ProjectIdMapper projectIdMapper =  sqlSession.getMapper(ProjectIdMapper.class);

        try {
            projectResultSet = projectStatement.executeQuery("select * from PROJECT order by name");

            Map<String,Object> map = new HashMap<>();
            projectResultSet.last();


            int sumCount = projectResultSet.getRow(),i=0;
            Integer j = projectIdMapper.findMaxId();
            if (j!=null){
                j = j.intValue()+1;
            }else {
                j = 300;
            }

            System.out.println("记录总数-"+sumCount);
            projectResultSet.beforeFirst();
            while(projectResultSet.next()){
                map.clear();
                if(projectIdMapper.selectByOldProjectId(projectResultSet.getString("ID")) == null) {
                    System.out.println("j-"+j);

                    map.put("id",j.intValue());
                    map.put("oid",projectResultSet.getString("ID"));
                    map.put("name",projectResultSet.getString("name"));
                    projectIdMapper.addProjectId(map);
                    sqlSession.commit();

                }
                i++;
                j++;
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
