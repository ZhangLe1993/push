package com.biubiu.engine;

public interface ReadFunction
{
    Class<?> getJavaType();

    // This should be considered to have a method as below (it doesn't to avoid autoboxing)
    //    T read(ResultSet resultSet, int columnIndex)
    //            throws SQLException;
}
