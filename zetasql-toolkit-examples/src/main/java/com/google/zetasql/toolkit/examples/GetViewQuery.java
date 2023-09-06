package com.google.zetasql.toolkit.examples;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;

public class GetViewQuery {

    public static String getQuery(TableDefinition tdef) {
        String query = tdef.toString();
        if (query.indexOf(";") == -1) {
            query = query.substring(query.indexOf("query="), query.indexOf(", userDefinedFunctionsImmut="));
        } else {
            query = query.substring(query.indexOf("query="), query.indexOf(";"));
        }
        
        
        return query;
    }

    public static String getCreateTableStatement(String projectId, String datasetId, String viewId) {
        BigQuery bq = BigQueryOptions.getDefaultInstance().getService();

        TableId tableId = TableId.of(projectId, datasetId, viewId);

        Table table = bq.getTable(tableId);
        String viewQuery = "";

        if (table.getDefinition().getType().toString().trim().equals("VIEW")) {
            TableDefinition viewDefinition = table.getDefinition();
            viewQuery = getQuery(viewDefinition); 
            viewQuery = viewQuery.replace("query=", "CREATE TABLE `" + projectId + ".javaapi_ds." + viewId + "` AS (");
            viewQuery = viewQuery + ");";
        }

        return viewQuery;
    }

    public static void main(String[] args) throws Exception {
        String projectId = "financialreporting-223818";
        String datasetId = "reporting_model";
        String viewId = "income_expenses_2";

        
        System.out.println(getCreateTableStatement(projectId, datasetId, viewId));
        
    }

}
