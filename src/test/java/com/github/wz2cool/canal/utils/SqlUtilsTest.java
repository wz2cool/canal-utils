package com.github.wz2cool.canal.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.wz2cool.canal.utils.model.CanalRowChange;
import com.github.wz2cool.canal.utils.model.FlatMessage;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class SqlUtilsTest {

    private final String json = "{\"data\":[{\"id\":\"2451\",\"text_id\":\"42005\",\"rec_id\":\"42005\",\"sec_code\":\"jysh0000002\",\"sec_name\":\"xxxx\",\"f001d\":\"2021-09-06 00:00:00\",\"f002v\":\"xxxxx\",\"f003v\":\"xxxxx\",\"f004v\":\"HTML\",\"f005n\":\"2.0\",\"f006v\":\"xxxx\",\"memo\":\"正文\",\"object_id\":\"-1403210153\",\"report_date\":\"2021-09-06\",\"create_time\":\"2021-09-06 13:54:01\",\"update_time\":\"2021-09-06 13:54:01\",\"row_key\":null,\"change_code\":null}],\"database\":\"juchao\",\"es\":1630907640000,\"id\":996119,\"isDdl\":false,\"mysqlType\":{\"id\":\"bigint(20) unsigned\",\"text_id\":\"bigint(20) unsigned\",\"rec_id\":\"varchar(32)\",\"sec_code\":\"varchar(32)\",\"sec_name\":\"varchar(64)\",\"f001d\":\"datetime\",\"f002v\":\"varchar(256)\",\"f003v\":\"varchar(256)\",\"f004v\":\"varchar(32)\",\"f005n\":\"decimal(12,4)\",\"f006v\":\"varchar(512)\",\"memo\":\"varchar(256)\",\"object_id\":\"bigint(20)\",\"report_date\":\"date\",\"create_time\":\"datetime\",\"update_time\":\"datetime\",\"row_key\":\"varchar(36)\",\"change_code\":\"tinyint\"},\"old\":null,\"pkNames\":[\"id\"],\"sql\":\"\",\"sqlType\":{\"id\":-5,\"text_id\":-5,\"rec_id\":12,\"sec_code\":12,\"sec_name\":12,\"f001d\":93,\"f002v\":12,\"f003v\":12,\"f004v\":12,\"f005n\":3,\"f006v\":12,\"memo\":12,\"object_id\":-5,\"report_date\":91,\"create_time\":93,\"update_time\":93,\"row_key\":12,\"change_code\":-6},\"table\":\"supervision_org_bulletin_info\",\"ts\":1630907640733,\"type\":\"INSERT\"}";

    @Test
    public void testGetRowChange() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        final FlatMessage flatMessage = objectMapper.readValue(json, FlatMessage.class);
        final CanalRowChange rowChange = SqlUtils.getRowChange(flatMessage);
        assertFalse(rowChange.isDdl());
        assertEquals("supervision_org_bulletin_info", rowChange.getTable());
        assertEquals("juchao", rowChange.getDatabase());
    }
}
