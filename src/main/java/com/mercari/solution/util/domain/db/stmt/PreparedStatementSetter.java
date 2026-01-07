package com.mercari.solution.util.domain.db.stmt;

import java.io.Serializable;

@FunctionalInterface
public interface PreparedStatementSetter<T> extends Serializable {
    void setParameters(T element, PreparedStatementTemplate.PlaceholderSetterProxy preparedStatementProxy) throws java.lang.Exception;
}
