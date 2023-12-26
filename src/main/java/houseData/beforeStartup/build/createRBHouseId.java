package houseData.beforeStartup.build;

import com.mapper.build.RBHouseIdMapper;
import com.utils.BuildbatisUtils;
import com.utils.MyConnection;
import org.apache.ibatis.session.SqlSession;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class createRBHouseId {
    private static final String BEGIN_DATE = "2023-03-09";
    private static String DB_URL = "jdbc:mysql://127.0.0.1:3306/HOUSE_INFO?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
    private static Statement statement;
    private static ResultSet resultSet;
    public static void main(String agr[]) throws SQLException {
        statement = MyConnection.getStatement(DB_URL,"root","dgsoft");
        SqlSession sqlSession = BuildbatisUtils.getSqlSession();
        RBHouseIdMapper rbHouseIdMapper =  sqlSession.getMapper(RBHouseIdMapper.class);


        try {
            resultSet = statement.executeQuery("select * from HOUSE_INFO.HOUSE order by HOUSE_INFO.HOUSE.BUILDID,HOUSE_INFO.HOUSE.IN_FLOOR_NAME,HOUSE_ORDER,HOUSE.ID");

            Map<String,Object> map = new HashMap<>();
            resultSet.last();


            int sumCount = resultSet.getRow(),i=0;
            Integer j = rbHouseIdMapper.findMaxId();
            if (j!=null){
                j = j.intValue()+1;
            }else {
                j = 412000; //最大588680 下一 600000
            }

            System.out.println("记录总数-"+sumCount);
            resultSet.beforeFirst();
            while(resultSet.next()){
                map.clear();
                if(rbHouseIdMapper.selectByOldHouseId(resultSet.getString("ID")) == null) {
                    System.out.println("j-"+j);

                    map.put("id",j.intValue());
                    map.put("oid",resultSet.getString("ID"));
                    rbHouseIdMapper.addHouseId(map);
                    sqlSession.commit();
                    j++;
                }
                i++;

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
