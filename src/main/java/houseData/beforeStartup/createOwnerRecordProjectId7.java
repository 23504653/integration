package houseData.beforeStartup;

import com.mapper.BuildIdMapper;
import com.mapper.OwnerRecordProjectIdMapper;
import com.utils.MyConnection;
import com.utils.MybatisUtils;
import org.apache.ibatis.session.SqlSession;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class createOwnerRecordProjectId7 {
    private static final String BEGIN_DATE = "2023-03-09";
    private static String DB_URL = "jdbc:mysql://127.0.0.1:3306/HOUSE_OWNER_RECORD?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
    private static Statement statement;
    private static ResultSet resultSet;

    public static void main(String agr[]) throws SQLException {
        statement = MyConnection.getStatement(DB_URL,"root","dgsoft");
        SqlSession sqlSession = MybatisUtils.getSqlSession();
        OwnerRecordProjectIdMapper ownerRecordProjectIdMapper =  sqlSession.getMapper(OwnerRecordProjectIdMapper.class);

        try {
            resultSet = statement.executeQuery("select * from PROJECT order by name");

            Map<String,Object> map = new HashMap<>();
            resultSet.last();


            int sumCount = resultSet.getRow(),i=0;
            Integer j = ownerRecordProjectIdMapper.findMaxId();
            if (j!=null){
                j = j.intValue()+1;
            }else {
                j = 690000;//共 2998 下一起始 695000
            }

            System.out.println("记录总数-"+sumCount);
            resultSet.beforeFirst();
            while(resultSet.next()){
                map.clear();
                if(ownerRecordProjectIdMapper.selectByOldId(resultSet.getString("ID")) == null) {
                    System.out.println("j-"+j);

                    map.put("id",j.intValue());
                    map.put("oid",resultSet.getString("ID"));
                    ownerRecordProjectIdMapper.addProjectId(map);
                    sqlSession.commit();

                }
                i++;
                j++;
                System.out.println(i+"/"+String.valueOf(sumCount));
            }
        }catch (Exception e){
            System.out.println("id is errer-----id:"+resultSet.getString("ID"));
            e.printStackTrace();
            return;
        }finally {
            if (resultSet!=null){
                resultSet.close();
            }
            if(statement!=null){
                statement.close();
            }
            sqlSession.close();
            MyConnection.closeConnection();

        }

    }

}
