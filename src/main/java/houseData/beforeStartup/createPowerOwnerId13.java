package houseData.beforeStartup;

import com.mapper.PowerOwnerIdMapper;
import com.utils.MyConnection;
import com.utils.MybatisUtils;
import org.apache.ibatis.session.SqlSession;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class createPowerOwnerId13 {
    private static final String BEGIN_DATE = "2023-03-09";
    private static String DB_URL = "jdbc:mysql://127.0.0.1:3306/HOUSE_OWNER_RECORD?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
    private static Statement businessStatement;
    private static ResultSet businessResultSet;

    public static void main(String agr[]) throws SQLException {
        businessStatement = MyConnection.getStatement(DB_URL, "root", "dgsoft");
        SqlSession sqlSession = MybatisUtils.getSqlSession();
        PowerOwnerIdMapper powerOwnerIdMapper = sqlSession.getMapper(PowerOwnerIdMapper.class);

        try {
            businessResultSet = businessStatement.executeQuery("select * from HOUSE_OWNER_RECORD.POWER_OWNER order by ID");

            Map<String, Object> map = new HashMap<>();
            businessResultSet.last();


            int sumCount = businessResultSet.getRow(), i = 0;
            Integer j = powerOwnerIdMapper.findMaxId();
            if (j != null) {
                j = j.intValue() + 1;
            } else {
                j = 1780000; //共计 578707     下一个2,400,000
            }

            System.out.println("记录总数-" + sumCount);
            businessResultSet.beforeFirst();
            while (businessResultSet.next()) {
                map.clear();
                if (powerOwnerIdMapper.selectByOldId(businessResultSet.getString("ID")) == null) {
                    System.out.println("j-" + j);

                    map.put("id", j.intValue());
                    map.put("oid", businessResultSet.getString("ID"));

                    powerOwnerIdMapper.addPowerOwnerId(map);
                    sqlSession.commit();

                }
                i++;
                j++;
                System.out.println(i + "/" + String.valueOf(sumCount));
            }
        } catch (Exception e) {
            System.out.println("id is errer-----id:" + businessResultSet.getString("ID"));
            e.printStackTrace();
            return;
        } finally {
            if (businessResultSet != null) {
                businessResultSet.close();
            }
            if (businessStatement != null) {
                businessStatement.close();
            }
            sqlSession.close();
            MyConnection.closeConnection();

        }
    }

}
