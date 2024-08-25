package com.github.wz2cool.canal.utils.provider;

import com.github.wz2cool.canal.utils.model.CanalRowChange;

import java.util.Optional;

public class DefaultAlterStatementProvider implements AlterStatementProvider {

    @Override
    public Optional<String> getAlterStatement(final CanalRowChange canalRowChange) {
        // 仅处理类型为 "alter" 的情况，其他情况返回空
        if ("alter".equalsIgnoreCase(canalRowChange.getType())) {
            return Optional.ofNullable(canalRowChange.getSql());
        } else {
            return Optional.empty();
        }
    }
}
