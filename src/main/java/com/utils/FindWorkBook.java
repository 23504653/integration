package com.utils;

import com.bean.WorkBook;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 字典转换
 *
 */

public class FindWorkBook {

    public static String landUseType(String id){
        if(id ==null || id.equals("") || id.equals("2") || id.equals("住宅") || id.equals("住宅用地")
                || id.equals("公寓") || id.equals("公寓") ){
            return "DWELLING";

        }else if(id.equals("商业") || id.equals("商业/车库") || id.equals("商业用地") || id.equals("商服用地") || id.equals("网点") || id.equals("车库、库房")){
            return "SHOP";
        }else {
            return "DWELLING";
        }
    }



    public static WorkBook changeLandSnapshot(String id){
        // 没有默认是住宅
        WorkBook workBook = new WorkBook();
        workBook.setId("DWELLING");
        workBook.setValue("住宅");
        if(id == null || id.equals("") || id.isBlank()){
            return workBook;
        }
        if(id.equals("DWELLING_KEY")){
            workBook.setId("DWELLING");
            workBook.setValue("住宅");
            return workBook;
        }else if(id.equals("SHOP_HOUSE_KEY")){
            workBook.setId("SHOP");
            workBook.setValue("商业用房");
            return workBook;
        }else if(id.equals("CAR_STORE")) {
            workBook.setId("GARAGE");
            workBook.setValue("车库");
            return workBook;
        }else {
            return workBook;
        }
    }

    /**
     * @param id
     * @return 转换正价类型，个人和企业的证件类型转换
     */

    public static WorkBook changeIdType(String id,String cardType){
        //cardType 1,机构，2个人
        WorkBook workBook = new WorkBook();
        if(cardType.equals(1)){
            workBook.setId("RESIDENT_ID");
            workBook.setValue("身份证");
        }else {
            workBook.setId("COMPANY");
            workBook.setValue("营业执照");
        }


        if(id == null || id.equals("") || id.isBlank()){
            return workBook;
        }
        if(id.equals("SOLDIER_CARD")) { //士兵证
            workBook.setId("SOLDIER");
            workBook.setValue("士兵证");
            return workBook;
        }else if(id.equals("MASTER_ID")){//身份证
            workBook.setId("RESIDENT_ID");
            workBook.setValue("身份证");
            return workBook;
        }else if(id.equals("COMPANY_CODE")){//营业执照
            workBook.setId("COMPANY");
            workBook.setValue("营业执照");
            return workBook;
        }else if(id.equals("CORP_CODE")){//机构代码证
            workBook.setId("ORGANIZATION");
            workBook.setValue("机构代码证");
            return workBook;
        }else if(id.equals("PASSPORT")){//护照
            workBook.setId("PASSPORT");
            workBook.setValue("护照");
            return workBook;
        }else if(id.equals("TW_ID")){//台湾通行证
            workBook.setId("TAIWAN");
            workBook.setValue("台湾通行证");
            return workBook;
        }else if(id.equals("OFFICER_CARD")){//军官证
            workBook.setId("OFFICER");
            workBook.setValue("军官证");
            return workBook;
        }else if(id.equals("GA_ID")){//港澳通行证
            workBook.setId("SPECIAL");
            workBook.setValue("港澳通行证");
            return workBook;
        }else {
            return workBook;
        }
    }

    /**
     * @param id
     * @return 土地权证类型转换
     */
    public static WorkBook changeLandCardType(String id){
        WorkBook workBook = new WorkBook();
        workBook.setId(null);
        workBook.setValue(null);
        if(id.equals("landCardType.collective")){
            workBook.setId("3");
            workBook.setValue("集体土地使用证");
            return workBook;
        }else {
            workBook.setId("2");
            workBook.setValue("国有土地使用证");
            return workBook;
        }

    }
    /**
     * @param id
     * @return 土地性质转换 允许为空
     */
    public static WorkBook changeLandProperty(String id){
        WorkBook workBook = new WorkBook();
        workBook.setId(null);
        workBook.setValue(null);
        if(id == null || id.equals("") || id.isBlank()){
            return workBook;
        }
        if(id.equals("100")){
            workBook.setId("2");
            workBook.setValue("集体");
            return workBook;
        }else if(id.equals("870")){
            workBook.setId("1");
            workBook.setValue("国有");
            return workBook;
        }else {
            return workBook;
        }
    }
    /**
     * @param id
     * @return 土地取得方式 不能为空 字典
     */
    public static WorkBook changeLandTakeType(String id){
        WorkBook workBook = new WorkBook();
        workBook.setId("6");
        workBook.setValue("其它");
        if(id == null || id.equals("") || id.isBlank()){
            return workBook;
        }
        if(id.equals("180")) {
            workBook.setId("1");
            workBook.setValue("出让");
            return workBook;
        }else if(id.equals("181")) {
            workBook.setId("2");
            workBook.setValue("划拨");
            return workBook;
        }else if(id.equals("182")) {
            workBook.setId("3");
            workBook.setValue("集体建设用地");
            return workBook;
        }else if(id.equals("183")) {
            workBook.setId("4");
            workBook.setValue("出租");
            return workBook;
        }else if(id.equals("184")) {
            workBook.setId("5");
            workBook.setValue("宅基地使用权");
            return workBook;
        }else {
            return workBook;
        }
    }


    public static WorkBook projectSizeType(String id){
        WorkBook workBook = new WorkBook();
        if(id.equals("buildSize.small")){
            workBook.setId("SMALL");
            workBook.setValue("小");
            return workBook;
        }else if(id.equals("buildSize.big")){
            workBook.setId("BIG");
            workBook.setValue("大");
            return workBook;
        }else {
            workBook.setId("MIDDLE");
            workBook.setValue("中等");
            return workBook;
        }
    }
    public static WorkBook structure(String id){
        WorkBook workBook = new WorkBook();
        workBook.setId("0");
        workBook.setValue("其他结构");

        if(id.equals("88")){
            workBook.setId("1");
            workBook.setValue("钢结构");
            return workBook;
        }else if(id.equals("822") || id.equals("89")){
            workBook.setId("2");
            workBook.setValue("钢、钢筋混凝土结构");
            return workBook;
        }else if(id.equals("823") || id.equals("915")){
            workBook.setId("3");
            workBook.setValue("钢筋混凝土结构");
            return workBook;
        }else if(id.equals("824")){
            workBook.setId("4");
            workBook.setValue("混合结构");
            return workBook;
        }else if(id.equals("825")){
            workBook.setId("5");
            workBook.setValue("砖木结构");
            return workBook;
        }else if(id.equals("4113")){
            workBook.setId("8");
            workBook.setValue("剪力结构");
            return workBook;
        }else if(id.equals("826")){
            workBook.setId("9");
            workBook.setValue("土木结构");
            return workBook;
        }else if(id.equals("90")){
            workBook.setId("10");
            workBook.setValue("花岗石结构");
            return workBook;
        }else if(id.equals("1559")){
            workBook.setId("11");
            workBook.setValue("砖混结构");
            return workBook;
        }else if(id.equals("框剪")){
            workBook.setId("12");
            workBook.setValue("框剪");
            return workBook;
        }else if(id.equals("框架")){
            workBook.setId("13");
            workBook.setValue("框架");
            return workBook;
        }else{
            return workBook;
        }
    }

    public static WorkBook buildType(String id){
        WorkBook workBook = new WorkBook();
        workBook.setId("MULTI");
        workBook.setValue("多层");
        if (id.equals("202")){
            workBook.setId("MULTI");
            workBook.setValue("多层");
            return workBook;
        }else if(id.equals("203")){
            workBook.setId("HIGH");
            workBook.setValue("高层");
            return workBook;
        }else if(id.equals("204")){
            workBook.setId("VILLA");
            workBook.setValue("别墅");
            return workBook;
        }else if(id.equals("2772")){
            workBook.setId("BUNGALOW");
            workBook.setValue("平房");
            return workBook;
        }else{
            return workBook;
        }
    }

    public static WorkBook getMappingCorpId(String id){
        WorkBook workBook = new WorkBook();
        workBook.setId("3");
        workBook.setValue("未知测绘机构");
        if(id==null || id.equals("")){
            return workBook;
        }
        if(id.equals("1")){
            workBook.setId("1");
            workBook.setValue("东港市房产测绘中心");
            return workBook;
        }else if(id.equals("2")){
            workBook.setId("2");
            workBook.setValue("东港市村镇建设管理处测绘队");
            return workBook;
        }else {
            return workBook;
        }

    }
}
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
 ('structure',8,'剪力结构',8,'08',null,true);
 INSERT dictionary.dictionary_word (category, value, label, seq, report_value, _group, enabled) value
 ('structure',9,'土木结构',9,'09',null,true);
 INSERT dictionary.dictionary_word (category, value, label, seq, report_value, _group, enabled) value
 ('structure',10,'花岗石结构',10,'10',null,true);
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
