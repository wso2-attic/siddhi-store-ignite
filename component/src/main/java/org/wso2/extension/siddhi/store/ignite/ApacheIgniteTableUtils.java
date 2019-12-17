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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Class which holds the utility methods which are used by Apache Ignite store implementation.
 */
public class ApacheIgniteTableUtils {

    private static final Log log = LogFactory.getLog(ApacheIgniteTableUtils.class);

    private ApacheIgniteTableUtils() { }

    public static boolean isEmpty(String field) {
        return (field == null || field.trim().length() == 0);
    }

    /**
     *  Method which can be used to clear up  SQL connectivity artifacts.
     * @param rs  ResultSet instance (can be null)
     * @param stmt  PreparedStatement instance (can be null)
     */
    public static void cleanup(ResultSet rs, Statement stmt) {
        if (rs != null) {
            try {
                rs.close();
                if (log.isDebugEnabled()) {
                    log.debug("Closed ResultSet");
                }
            } catch (SQLException e) {
                if (log.isDebugEnabled()) {
                    log.debug("Error closing ResultSet: " + e.getMessage(), e);
                }
            }
        }
        if (stmt != null) {
            try {
                stmt.close();
                if (log.isDebugEnabled()) {
                    log.debug("Closed PreparedStatement");
                }
            } catch (SQLException e) {
                if (log.isDebugEnabled()) {
                    log.debug("Error closing PreparedStatement: " + e.getMessage(), e);
                }
            }
        }
    }
}
