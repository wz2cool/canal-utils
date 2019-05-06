canal-utils
=====================================

[![License](http://img.shields.io/:license-apache-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
[![Build Status](https://travis-ci.org/wz2cool/canal-utils.svg?branch=master)](https://travis-ci.org/wz2cool/canal-utils)
[![Maven central](https://maven-badges.herokuapp.com/maven-central/com.github.wz2cool/canal-utils/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.wz2cool/canal-utils)

# 简介
本项目是服务阿里同步项目 [canal](https://github.com/alibaba/canal) 的一个工具箱帮助类库。

# Maven
```xml
<dependency>
    <groupId>com.github.wz2cool</groupId>
    <artifactId>canal-utils</artifactId>
    <version>0.1.0</version>
</dependency>
```

# 功能
## 转化器
Mysql DDL 语句转化到不同数据库上，支持：db2, hive, mssql, oracle, postgresql. [wiki文档](https://github.com/wz2cool/canal-utils/wiki/converter)