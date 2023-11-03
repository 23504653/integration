package houseData.beforeStartup;

import com.mapper.WorkBookMapper;
import com.utils.MybatisUtils;
import org.apache.ibatis.session.SqlSession;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class createWorkBook0 {

    /**
     除了houseType useType外的字典处理， 通过调用workbookMapper 通过就字典的ID 取出新字典的ID
     use INTEGRATION;
     -- 'layer_type 户型结构','layout 户型居室 为空不导入'，orientation	房屋朝向 么有
     INSERT dictionary.dictionary_word (category, value, label, seq, report_value, _group, enabled) value
     ('land_takeType',3,'集体建设用地',3,'03',null,true);
     INSERT dictionary.dictionary_word (category, value, label, seq, report_value, _group, enabled) value
     ('land_takeType',4,'出租',4,'04',null,true);
     INSERT dictionary.dictionary_word (category, value, label, seq, report_value, _group, enabled) value
     ('land_takeType',5,'宅基地使用权',5,'05',null,true);
     INSERT dictionary.dictionary_word (category, value, label, seq, report_value, _group, enabled) value
     ('land_takeType',6,'其它',6,'06',null,true);
     INSERT dictionary.dictionary_word (category, value, label, seq, report_value, _group, enabled) value
     ('land_property',3,'其它',3,'03',null,true);
     INSERT dictionary.dictionary_word (category, value, label, seq, report_value, _group, enabled) value
     ('land_licenseType',4,'其它',4,'04',null,true);

     INSERT dictionary.dictionary_word (category, value, label, seq, report_value, _group, enabled) value
     ('structure',8,'剪力结构',8,'08',null,true);
     INSERT dictionary.dictionary_word (category, value, label, seq, report_value, _group, enabled) value
     ('structure',9,'土木结构',9,'09',null,true);
     INSERT dictionary.dictionary_word (category, value, label, seq, report_value, _group, enabled) value
     ('structure',10,'花岗石结构',10,'10',null,true);
     INSERT dictionary.dictionary_word (category, value, label, seq, report_value, _group, enabled) value
     ('structure',11,'砖混结构',11,'11',null,true);
     INSERT dictionary.dictionary_word (category, value, label, seq, report_value, _group, enabled) value
     ('structure',11,'砖混结构',11,'11',null,true);


     INSERT INTEGRATION.workBook(id, oid, value) value (3,'landCardType.collective','集体土地使用证');
     INSERT INTEGRATION.workBook(id, oid, value) value (2,'landCardType.stateOwned','国有土地使用证');
     INSERT INTEGRATION.workBook(id, oid, value) value (1,'870','国有');
     INSERT INTEGRATION.workBook(id, oid, value) value (2,'100','集体');
     INSERT INTEGRATION.workBook(id, oid, value) value (1,'180','出让');
     INSERT INTEGRATION.workBook(id, oid, value) value (2,'181','划拨');
     INSERT INTEGRATION.workBook(id, oid, value) value (3,'182','集体建设用地');
     INSERT INTEGRATION.workBook(id, oid, value) value (4,'183','出租');
     INSERT INTEGRATION.workBook(id, oid, value) value (5,'184','宅基地使用权');

     INSERT INTEGRATION.workBook(id, oid, value) value (0,'215','夫妻');
     INSERT INTEGRATION.workBook(id, oid, value) value (1,'3852','父子（女）');
     INSERT INTEGRATION.workBook(id, oid, value) value (1,'3853','父子（女）');
     INSERT INTEGRATION.workBook(id, oid, value) value (2,'3850','母子（女）');
     INSERT INTEGRATION.workBook(id, oid, value) value (2,'3851','母子（女）');
     INSERT INTEGRATION.workBook(id, oid, value) value (4,'216','兄弟');
     INSERT INTEGRATION.workBook(id, oid, value) value (5,'217','姐妹');
     INSERT INTEGRATION.workBook(id, oid, value) value (6,'3856','其它');

     INSERT INTEGRATION.workBook(id, oid, value) value (0,'827','其他结构');
     INSERT INTEGRATION.workBook(id, oid, value) value (1,'88','钢结构');
     INSERT INTEGRATION.workBook(id, oid, value) value (2,'822','钢、钢筋混凝土结构');
     INSERT INTEGRATION.workBook(id, oid, value) value (2,'89','钢、钢筋混凝土结构');
     INSERT INTEGRATION.workBook(id, oid, value) value (3,'823','钢筋混凝土结构');
     INSERT INTEGRATION.workBook(id, oid, value) value (3,'915','钢筋混凝土结构');
     INSERT INTEGRATION.workBook(id, oid, value) value (4,'824','混合结构');
     INSERT INTEGRATION.workBook(id, oid, value) value (5,'825','砖木结构');
     INSERT INTEGRATION.workBook(id, oid, value) value (8,'4113','剪力结构');
     INSERT INTEGRATION.workBook(id, oid, value) value (9,'826','土木结构');
     INSERT INTEGRATION.workBook(id, oid, value) value (10,'90','花岗石结构');
     INSERT INTEGRATION.workBook(id, oid, value) value (11,'1559','砖混结构');

     INSERT INTEGRATION.workBook(id, oid, value) value ('SOLDIER','SOLDIER_CARD','士兵证');
     INSERT INTEGRATION.workBook(id, oid, value) value ('RESIDENT_ID','MASTER_ID','身份证');
     INSERT INTEGRATION.workBook(id, oid, value) value ('COMPANY','COMPANY_CODE','营业执照');
     INSERT INTEGRATION.workBook(id, oid, value) value ('ORGANIZATION','CORP_CODE','机构代码证');
     INSERT INTEGRATION.workBook(id, oid, value) value ('PASSPORT','PASSPORT','护照');
     INSERT INTEGRATION.workBook(id, oid, value) value ('TAIWAN','TW_ID','台湾通行证');
     INSERT INTEGRATION.workBook(id, oid, value) value ('OTHER','OTHER','无证件');
     INSERT INTEGRATION.workBook(id, oid, value) value ('OFFICER','OFFICER_CARD','军官证');
     INSERT INTEGRATION.workBook(id, oid, value) value ('SPECIAL','GA_ID','港澳通行证');

     */

    public static void main(String agr[]) throws SQLException {
        SqlSession sqlSession = MybatisUtils.getSqlSession();
        WorkBookMapper workBookMapper = sqlSession.getMapper(WorkBookMapper.class);
        Map<String,Object> map = new HashMap<>();
        System.out.println("保存除了houseType useType外的字典处理sql");
    }

}
