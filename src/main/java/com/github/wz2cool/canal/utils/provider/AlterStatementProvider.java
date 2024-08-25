package com.github.wz2cool.canal.utils.provider;

import com.github.wz2cool.canal.utils.model.CanalRowChange;

import java.util.Optional;

public interface AlterStatementProvider {

    /**
     * 获取ALTER语句。
     *
     * @param canalRowChange CanalRowChange对象
     * @return 包含ALTER SQL语句的Optional，或为空的Optional
     */
    Optional<String> getAlterStatement(final CanalRowChange canalRowChange);
}
