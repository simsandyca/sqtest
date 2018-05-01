/*
 * Copyright (C) 2013 Turn Inc.  All Rights Reserved.
 * Proprietary and confidential.
 */
package com.turn.sonarqube.test;

import com.turn.db.HBConnection;
import com.turn.db.HBConnectionManager;
import com.turn.platform.datalog.agg.AggregateType;
import com.turn.platform.reporting.util.SQLUtils;
import com.turn.util.Logger;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class ReportStoreManager {
	static Logger logger = new Logger(ReportStoreManager.class);

	private void sanityCheck() {
		String[] sqls = new String[]{"SELECT log_type, Sum(d1 - d2) daysBehind FROM "
				+ " ( SELECT log_type, Max(end_date) d1, 0 d2 FROM table_registry WHERE table_type = 10 GROUP BY log_type "
				+ "   UNION all "
				+ "   SELECT log_type, 0 d1, Min(start_date) d2 FROM table_registry WHERE table_type = 1 GROUP BY log_type "
				+ " ) t GROUP BY log_type HAVING Sum(d1 - d2) < 0 "};

		int errors = 0;
		for (String sql : sqls) {
			HBConnection conn = null;
			PreparedStatement pstmt = null;
			ResultSet rs = null;
			try {
				conn = SQLUtils.getConnection();
				pstmt = conn.getConnection().prepareStatement(sql);
				rs = pstmt.executeQuery();
				while (rs.next()) {
					String logType = rs.getString(1);
					AggregateType aggType = AggregateType
							.getAggregateType(logType);
					if (aggType != null && aggType.archiveHistorical()) {
						// only report historical table errors if we are supposed to roll data into
						// historical
						++errors;
						logger.error(
								"Failed sanity check - The historical table for %s is behind %s days",
								logType, rs.getInt(2));
					}
				}
			} catch (Exception ex) {
				logger.error(ex, "Failed to execute sanity check SQL: " + sql);
			} finally {
				try {
					rs.close();
				} catch (Exception ex) {
				}
				;
				try {
					pstmt.close();
				} catch (Exception ex) {
				}
				;
				try {
					HBConnectionManager.getInstance().returnConnection(conn);
				} catch (Exception ex) {
				}
				;
			}
		}

		if (errors > 0) {
			logger.error("Report store sanity check has %s errors", errors);
		}
	}

}

