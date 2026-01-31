package io.github.collin.cdc.mysql.cdc.ods.clickhouse;

import io.github.collin.cdc.common.enums.OpType;
import io.github.collin.cdc.common.util.JacksonUtil;
import io.github.collin.cdc.mysql.cdc.common.constants.FieldConstants;
import io.github.collin.cdc.common.constants.SchemaConstants;
import io.github.collin.cdc.mysql.cdc.common.dto.RowJson;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * 将 RowJson 转为写入 ClickHouse 的 JSON 行（含 f_ts、is_del）
 *
 * @author collin
 */
public class ClickHouseRowToJsonProcess extends ProcessFunction<RowJson, String> {

    private final boolean isSharding;

    public ClickHouseRowToJsonProcess(boolean isSharding) {
        this.isSharding = isSharding;
    }

    @Override
    public void processElement(RowJson value, Context ctx, Collector<String> out) throws Exception {
        if (value.getOp() == OpType.DDL) {
            return;
        }
        Map<String, Object> map = value.getJson();
        if (map == null) {
            return;
        }
        if (isSharding) {
            map.put(FieldConstants.DB_NAME, value.getDb());
            map.put(FieldConstants.TABLE_NAME, value.getTable());
        }
        long ts = System.currentTimeMillis();
        map.put(SchemaConstants.SYNC_TS, ts);
        map.put(SchemaConstants.IS_DEL, value.getOp() == OpType.DELETE ? 1 : 0);
        out.collect(JacksonUtil.toJson(map));
    }
}
