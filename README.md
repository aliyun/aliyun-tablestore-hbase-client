# Aliyun Tablestore HBase client for Java

[![Software License](https://img.shields.io/badge/license-apache2-brightgreen.svg)](LICENSE)
[![GitHub version](https://badge.fury.io/gh/aliyun%2Faliyun-tablestore-hbase-client.svg)](https://badge.fury.io/gh/aliyun%2Faliyun-tablestore-hbase-client)

## Compile

```
# Java 8
# skip compile and install test
mvn clean install -Dmaven.test.skip=true
```

## Test
set your config in src/test/resources/hbase-site.xml

```xml
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<!--
/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
-->
<configuration>
    <property>
        <name>hbase.client.connection.impl</name>
        <value>com.alicloud.tablestore.hbase.TablestoreConnection</value>
    </property>
    <property>
        <name>tablestore.client.endpoint</name>
        <value>http://xxx:80</value>
    </property>
    <property>
        <name>tablestore.client.instancename</name>
        <value>xxx</value>
    </property>
    <property>
        <name>tablestore.client.accesskeyid</name>
        <value>xxx</value>
    </property>
    <property>
        <name>tablestore.client.accesskeysecret</name>
        <value>xxx</value>
    </property>
    <property>
        <name>hbase.client.tablestore.family</name>
        <value>s</value>
    </property>
    <property>
        <name>hbase.client.tablestore.table</name>
        <value>ots_adaptor</value>
    </property>
    <property>
        <name>hbase.defaults.for.version.skip</name>
        <value>true</value>
    </property>
    <property>
        <name>hbase.hconnection.meta.lookup.threads.core</name>
        <value>4</value>
    </property>
    <property>
        <name>hbase.hconnection.threads.keepalivetime</name>
        <value>3</value>
    </property>
</configuration>
```
