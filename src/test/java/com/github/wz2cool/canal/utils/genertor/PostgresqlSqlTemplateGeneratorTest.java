package com.github.wz2cool.canal.utils.genertor;

import com.github.wz2cool.canal.utils.converter.mysql.decorator.AlterSqlConverterDecorator;
import com.github.wz2cool.canal.utils.converter.mysql.decorator.ReplaceNameDecorator;
import com.github.wz2cool.canal.utils.converter.postgresql.PostgresqlAlterSqlConverter;
import com.github.wz2cool.canal.utils.generator.PostgresqlSqlTemplateGenerator;
import com.github.wz2cool.canal.utils.model.CanalRowChange;
import com.github.wz2cool.canal.utils.model.SqlTemplate;
import com.google.common.collect.Lists;
import net.sf.jsqlparser.JSQLParserException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class PostgresqlSqlTemplateGeneratorTest {
    private PostgresqlSqlTemplateGenerator postgresqlSqlTemplateGenerator;
    private CanalRowChange canalRowChange;

    @Before
    public void init() {
        List<AlterSqlConverterDecorator> decorators = Lists.newArrayList();
        decorators.add(new ReplaceNameDecorator("hermes_test", "test_bond_quote_best_price_broker"));
        PostgresqlAlterSqlConverter postgresqlAlterSqlConverter = new PostgresqlAlterSqlConverter(decorators);
        postgresqlSqlTemplateGenerator = new PostgresqlSqlTemplateGenerator(postgresqlAlterSqlConverter);
        canalRowChange = new CanalRowChange();
        canalRowChange.setId(1L);
        canalRowChange.setDatabase("dmdc");
        canalRowChange.setTable("t_com_info");
        canalRowChange.setType("ALTER");
        canalRowChange.setRowDataList(Lists.newArrayList());
        canalRowChange.setEs(System.currentTimeMillis());
        canalRowChange.setDdl(true);
    }


    @Test
    public void testAddColumn() throws JSQLParserException {
        canalRowChange.setSql("ALTER TABLE `penghai_test`.`bank_product_site_config` ADD COLUMN `site_url_code_2` varchar(255) DEFAULT " +
                "NULL COMMENT 'test' AFTER `site_url_code`");
        final List<SqlTemplate> sqlTemplates = postgresqlSqlTemplateGenerator.listDDLSqlTemplates(canalRowChange);
        SqlTemplate sqlTemplate = sqlTemplates.get(0);
        String expectedSql = "ALTER TABLE hermes_test.\"test_bond_quote_best_price_broker\" ADD site_url_code_2 VARCHAR (255)";
        Assert.assertEquals(expectedSql, sqlTemplate.getExpression());
    }


}
