/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.extension.siddhi.store.ignite;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.sql.SQLException;

import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteTestUtils.PASSWORD;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteTestUtils.URL;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteTestUtils.USERNAME;

public class DeleteFromApacheIgniteTestCase {

    private static final Log log = LogFactory.getLog(DeleteFromApacheIgniteTestCase.class);

    @BeforeClass
    public static void startTest() {
        log.info("== Apache Ignite Table DELETE tests started ==");
    }

    @AfterClass
    public static void shutdown() {
        log.info("== Apache Ignite Table DELETE tests completed");
    }

    @BeforeMethod
    public void init() {
        try {
            ApacheIgniteTestUtils.dropTable(ApacheIgniteTestUtils.TABLE_NAME);
        } catch (SQLException e) {
            log.info("Test case ignored due to " + e.getMessage());
        }
    }

    @Test(description = "Testing deletion by using primary key in on condition")
    public void deleteFromTableWithPrimaryKeyTest() throws InterruptedException, SQLException {
        log.info("deleteFromTableTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStockStream (symbol string); " +
                "@Store(type=\"apacheignite\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD
                + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query1 = "" +
                "@info(name = 'query1') " +
                "from StockStream\n" +
                "select symbol, price, volume\n" +
                "insert into StockTable ;" +
                "@info(name = 'query2') " +
                "from DeleteStockStream " +
                "delete StockTable " +
                "   on StockTable.symbol == symbol ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query1);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 325.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"CSC", 85.6f, 200L});
        deleteStockStream.send(new Object[]{"WSO2"});
        deleteStockStream.send(new Object[]{"IBM"});

        Thread.sleep(500);
        int rowsInTable = ApacheIgniteTestUtils.getRowsInTable(ApacheIgniteTestUtils.TABLE_NAME);
        Assert.assertEquals(rowsInTable, 1, "Deletion failed");
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Testing deletion using constant ")
    public void deleteFromTableWithConstantInConditionTest() throws InterruptedException, SQLException {
        log.info("deleteFromTableTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStockStream (symbol string); " +
                "@Store(type=\"apacheignite\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD
                + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query1 = "" +
                "@info(name = 'query1') " +
                "from StockStream\n" +
                "select symbol, price, volume\n" +
                "insert into StockTable ;" +
                "@info(name = 'query2') " +
                "from DeleteStockStream " +
                "delete StockTable " +
                "   on StockTable.symbol == 'WSO2' ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query1);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 325.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"CSC", 85.6f, 200L});
        deleteStockStream.send(new Object[]{"WSO2"});
        deleteStockStream.send(new Object[]{"IBM"});

        Thread.sleep(500);
        int rowsInTable = ApacheIgniteTestUtils.getRowsInTable(ApacheIgniteTestUtils.TABLE_NAME);
        Assert.assertEquals(rowsInTable, 2, "Deletion failed");
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Testing deletion without using primary keys")
    public void deleteFromTableWithNonPrimaryKeysTest() throws InterruptedException, SQLException {
        log.info("deleteFromTableWithNonPrimaryKeysTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStockStream (volume long); " +
                "@Store(type=\"apacheignite\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD
                + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query1 = "" +
                "@info(name = 'query1') " +
                "from StockStream\n" +
                "select symbol, price, volume\n" +
                "insert into StockTable ;" +
                "@info(name = 'query2') " +
                "from DeleteStockStream " +
                "delete StockTable " +
                "   on StockTable.volume == volume ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query1);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 325.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"CSC", 85.6f, 200L});
        deleteStockStream.send(new Object[]{100});
        deleteStockStream.send(new Object[]{150});

        Thread.sleep(500);
        int rowsInTable = ApacheIgniteTestUtils.getRowsInTable(ApacheIgniteTestUtils.TABLE_NAME);
        Assert.assertEquals(rowsInTable, 1, "Deletion failed");
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Testing deletion using constant ")
    public void deleteFromTableWithConditionTest() throws InterruptedException, SQLException {
        log.info("deleteFromTableTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStockStream (symbol string); " +
                "@Store(type=\"apacheignite\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD
                + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query1 = "" +
                "@info(name = 'query1') " +
                "from StockStream\n" +
                "select symbol, price, volume\n" +
                "insert into StockTable ;" +
                "@info(name = 'query2') " +
                "from DeleteStockStream " +
                "delete StockTable " +
                "   on 'WSO2' == StockTable.symbol  ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query1);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 325.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"CSC", 85.6f, 200L});
        deleteStockStream.send(new Object[]{"WSO2"});
        deleteStockStream.send(new Object[]{"IBM"});

        Thread.sleep(500);
        int rowsInTable = ApacheIgniteTestUtils.getRowsInTable(ApacheIgniteTestUtils.TABLE_NAME);
        Assert.assertEquals(rowsInTable, 2, "Deletion failed");
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Testing deletion by using multiple conditions")
    public void deleteFromTableWithMultipleConditionsTest5() throws InterruptedException, SQLException {
        log.info("deleteFromTableTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStockStream (symbol string,price float); " +
                "@Store(type=\"apacheignite\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD
                + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query1 = "" +
                "@info(name = 'query1') " +
                "from StockStream\n" +
                "select symbol, price, volume\n" +
                "insert into StockTable ;" +
                "@info(name = 'query2') " +
                "from DeleteStockStream " +
                "delete StockTable " +
                "   on StockTable.symbol == symbol and StockTable.price>price ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query1);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 325.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"CSC", 85.6f, 200L});
        deleteStockStream.send(new Object[]{"WSO2", 315.6f});
        deleteStockStream.send(new Object[]{"IBM", 325.6f});

        Thread.sleep(500);
        int rowsInTable = ApacheIgniteTestUtils.getRowsInTable(ApacheIgniteTestUtils.TABLE_NAME);
        Assert.assertEquals(rowsInTable, 2, "Deletion failed");
        siddhiAppRuntime.shutdown();
    }
}
