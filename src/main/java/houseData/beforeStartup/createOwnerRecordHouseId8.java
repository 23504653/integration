package houseData.beforeStartup;

import com.bean.OwnerRecordHouseId;
import com.mapper.OwnerRecordHouseIdMapper;
import com.mapper.OwnerRecordProjectIdMapper;
import com.utils.MyConnection;
import com.utils.MybatisUtils;
import org.apache.ibatis.session.SqlSession;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class createOwnerRecordHouseId8 {
    private static final String BEGIN_DATE = "2023-03-09";
    private static String DB_URL = "jdbc:mysql://127.0.0.1:3306/HOUSE_OWNER_RECORD?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
    private static Statement statement;
    private static ResultSet resultSet;

    public static void main(String agr[]) throws SQLException {
        statement = MyConnection.getStatement(DB_URL,"root","dgsoft");
        SqlSession sqlSession = MybatisUtils.getSqlSession();
        OwnerRecordHouseIdMapper ownerRecordHouseIdMapper =  sqlSession.getMapper(OwnerRecordHouseIdMapper.class);

        try {
            resultSet = statement.executeQuery("select * from HOUSE order by ID");

            Map<String,Object> map = new HashMap<>();
            resultSet.last();


            int sumCount = resultSet.getRow(),i=0;
            Integer j = ownerRecordHouseIdMapper.findMaxId();
            if (j!=null){
                j = j.intValue()+1;
            }else {
                j = 608000;//最大 1167608 下一起始 1200000
            }

            System.out.println("记录总数-"+sumCount);
            resultSet.beforeFirst();
            while(resultSet.next()){
                map.clear();
                if(ownerRecordHouseIdMapper.selectByOldId(resultSet.getString("ID")) == null) {
                    System.out.println("j-"+j);

                    map.put("id",j.intValue());
                    map.put("oid",resultSet.getString("ID"));
                    ownerRecordHouseIdMapper.addHouseId(map);
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
