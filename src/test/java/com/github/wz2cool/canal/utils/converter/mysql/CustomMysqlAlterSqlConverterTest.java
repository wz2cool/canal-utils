package com.github.wz2cool.canal.utils.converter.mysql;

import com.github.wz2cool.canal.utils.converter.mysql.decorator.AlterSqlConverterDecorator;
import com.github.wz2cool.canal.utils.converter.mysql.decorator.DefaultValueDecorator;
import com.github.wz2cool.canal.utils.converter.mysql.decorator.ReplaceNameDecorator;
import com.github.wz2cool.canal.utils.generator.AbstractSqlTemplateGenerator;
import com.github.wz2cool.canal.utils.generator.MysqlSqlTemplateGenerator;
import com.github.wz2cool.canal.utils.model.CanalRowChange;
import com.github.wz2cool.canal.utils.model.SqlTemplate;
import com.google.common.collect.Lists;
import net.sf.jsqlparser.JSQLParserException;
import org.junit.Before;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author penghai
 */
public class CustomMysqlAlterSqlConverterTest {
    private AbstractSqlTemplateGenerator sqlTemplateGenerator;
    private CanalRowChange canalRowChange;
    private MysqlAlterSqlConverter mysqlAlterSqlConverter;

    @Before
    public void init() {
        List<AlterSqlConverterDecorator> decorators = Lists.newArrayList();
        decorators.add(new DefaultValueDecorator());
        decorators.add(new ReplaceNameDecorator("qa_penghai", "t_com_info"));
        mysqlAlterSqlConverter = new MysqlAlterSqlConverter(decorators);
        sqlTemplateGenerator = new MysqlSqlTemplateGenerator(mysqlAlterSqlConverter);
        canalRowChange = new CanalRowChange();
        canalRowChange.setId(1L);
        canalRowChange.setDatabase("dmdc");
        canalRowChange.setTable("t_com_info");
        canalRowChange.setType("ALTER");
        canalRowChange.setRowDataList(Lists.newArrayList());
        canalRowChange.setEs(System.currentTimeMillis());
        canalRowChange.setDdl(true);
    }

    @org.junit.Test
    public void testRenameColumn() throws JSQLParserException {
        canalRowChange.setSql("-- 对卖数据的表操作已二次确认\n" +
                "ALTER TABLE `dmdc`.`t_com_info`\n" +
                "ADD COLUMN `paid_in_capital` decimal(20, 4) NULL COMMENT '实缴资本(万元)' AFTER `all_dm_udic_status`,\n" +
                "ADD COLUMN `registration_organization` varchar(600) NULL COMMENT '注册机构' AFTER `paid_in_capital`,\n" +
                "ADD COLUMN `approval_date` date NULL COMMENT '核准日期' AFTER `registration_organization`,\n" +
                "ADD COLUMN `main_business` varchar(4000) NULL COMMENT '主营业务' AFTER `approval_date`,\n" +
                "MODIFY COLUMN `com_eng_short_name` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '公司英文简称' AFTER `com_eng_name`");
        List<SqlTemplate> sqlTemplates = sqlTemplateGenerator.listDDLSqlTemplates(canalRowChange);
        assertEquals(5, sqlTemplates.size());
    }

    @org.junit.Test
    public void testDropKey() throws JSQLParserException {
        String testSql = "alter table com_related_party\n" +
                "    drop key uk_business";
        List<String> result = mysqlAlterSqlConverter.convert(testSql);
        assertEquals("ALTER TABLE qa_penghai.`t_com_info` DROP INDEX `uk_business`;", result.get(0));
        canalRowChange.setSql(testSql);
        List<SqlTemplate> sqlTemplates = sqlTemplateGenerator.listDDLSqlTemplates(canalRowChange);
        assertEquals(1, sqlTemplates.size());
    }

    @org.junit.Test
    public void testCreateIndexSimple() throws JSQLParserException {
        String testSql = "create index idx_com_uni_code on com_related_party (com_uni_code)";
        List<String> result = mysqlAlterSqlConverter.convert(testSql);
        assertEquals("CREATE INDEX `idx_com_uni_code` ON qa_penghai.`t_com_info` (com_uni_code);", result.get(0));
        canalRowChange.setSql(testSql);
        List<SqlTemplate> sqlTemplates = sqlTemplateGenerator.listDDLSqlTemplates(canalRowChange);
        assertEquals(1, sqlTemplates.size());
    }

    @org.junit.Test
    public void testCreateIndexWithComment() throws JSQLParserException {
        String testSql = "/* ApplicationName=DataGrip 2024.3.5 */ create index idx_business_info on com_related_party (com_uni_code, report_date)";
        List<String> result = mysqlAlterSqlConverter.convert(testSql);
        assertEquals("CREATE INDEX `idx_business_info` ON qa_penghai.`t_com_info` (com_uni_code, report_date);", result.get(0));
        canalRowChange.setSql(testSql);
        List<SqlTemplate> sqlTemplates = sqlTemplateGenerator.listDDLSqlTemplates(canalRowChange);
        assertEquals(1, sqlTemplates.size());
    }

    @org.junit.Test
    public void testCreateIndexMultipleColumns() throws JSQLParserException {
        String testSql = "CREATE INDEX idx_multiple_cols ON com_related_party (com_uni_code, report_date, relation_type)";
        List<String> result = mysqlAlterSqlConverter.convert(testSql);
        assertEquals("CREATE INDEX `idx_multiple_cols` ON qa_penghai.`t_com_info` (com_uni_code, report_date, relation_type);", result.get(0));
        canalRowChange.setSql(testSql);
        List<SqlTemplate> sqlTemplates = sqlTemplateGenerator.listDDLSqlTemplates(canalRowChange);
        assertEquals(1, sqlTemplates.size());
    }

    @org.junit.Test
    public void testCreateIndexWithBackticks() throws JSQLParserException {
        String testSql = "create index `idx_party_info` on `com_related_party` (`com_uni_code`, `related_party_code`)";
        List<String> result = mysqlAlterSqlConverter.convert(testSql);
        assertEquals("CREATE INDEX `idx_party_info` ON qa_penghai.`t_com_info` (`com_uni_code`, `related_party_code`);", result.get(0));
        canalRowChange.setSql(testSql);
        List<SqlTemplate> sqlTemplates = sqlTemplateGenerator.listDDLSqlTemplates(canalRowChange);
        assertEquals(1, sqlTemplates.size());
    }

    @org.junit.Test
    public void testCreateIndexCaseInsensitive() throws JSQLParserException {
        String testSql = "CREATE INDEX IDX_UPPER_CASE ON COM_RELATED_PARTY (COM_UNI_CODE)";
        List<String> result = mysqlAlterSqlConverter.convert(testSql);
        assertEquals("CREATE INDEX `IDX_UPPER_CASE` ON qa_penghai.`t_com_info` (COM_UNI_CODE);", result.get(0));
        canalRowChange.setSql(testSql);
        List<SqlTemplate> sqlTemplates = sqlTemplateGenerator.listDDLSqlTemplates(canalRowChange);
        assertEquals(1, sqlTemplates.size());
    }

}
