package com.github.wz2cool.canal.utils.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class FlatMessageTest {

    @Test
    public void testJsonToObject() throws IOException {
        String json = "{\"data\":[{\"id\":\"2451\",\"text_id\":\"42005\",\"rec_id\":\"42005\",\"sec_code\":\"jysh0000002\",\"sec_name\":\"上海证券交易所\",\"f001d\":\"2021-09-06 00:00:00\",\"f002v\":\"关于15恒大03（122393）盘中临时停牌的公告\",\"f003v\":\"http://dataclouds.cninfo.com.cn/sjother/regulatory_announcement/sse/b7da90e20ed511ec9e78fa163e26e5de.html\",\"f004v\":\"HTML\",\"f005n\":\"2.0\",\"f006v\":\"上交所一般公告\",\"memo\":\"正文\",\"object_id\":\"-1403210153\",\"report_date\":\"2021-09-06\",\"create_time\":\"2021-09-06 13:54:01\",\"update_time\":\"2021-09-06 13:54:01\",\"row_key\":null,\"change_code\":null}],\"database\":\"juchao\",\"es\":1630907640000,\"id\":996119,\"isDdl\":false,\"mysqlType\":{\"id\":\"bigint(20) unsigned\",\"text_id\":\"bigint(20) unsigned\",\"rec_id\":\"varchar(32)\",\"sec_code\":\"varchar(32)\",\"sec_name\":\"varchar(64)\",\"f001d\":\"datetime\",\"f002v\":\"varchar(256)\",\"f003v\":\"varchar(256)\",\"f004v\":\"varchar(32)\",\"f005n\":\"decimal(12,4)\",\"f006v\":\"varchar(512)\",\"memo\":\"varchar(256)\",\"object_id\":\"bigint(20)\",\"report_date\":\"date\",\"create_time\":\"datetime\",\"update_time\":\"datetime\",\"row_key\":\"varchar(36)\",\"change_code\":\"tinyint\"},\"old\":null,\"pkNames\":[\"id\"],\"sql\":\"\",\"sqlType\":{\"id\":-5,\"text_id\":-5,\"rec_id\":12,\"sec_code\":12,\"sec_name\":12,\"f001d\":93,\"f002v\":12,\"f003v\":12,\"f004v\":12,\"f005n\":3,\"f006v\":12,\"memo\":12,\"object_id\":-5,\"report_date\":91,\"create_time\":93,\"update_time\":93,\"row_key\":12,\"change_code\":-6},\"table\":\"supervision_org_bulletin_info\",\"ts\":1630907640733,\"type\":\"INSERT\"}";
        ObjectMapper objectMapper = new ObjectMapper();
        final FlatMessage flatMessage = objectMapper.readValue(json, FlatMessage.class);
        Assert.assertFalse(flatMessage.getIsDdl());
    }
}
