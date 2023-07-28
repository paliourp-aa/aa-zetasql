/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.zetasql.toolkit.examples;

import java.util.ArrayList;
import java.util.List;

// [START bigquery_list_tables]
import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQuery.TableListOption;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Table;

public class ListTables {

  public static void main(String[] args) {
    // TODO(developer): Replace these variables before running the sample.
    String projectId = "bigquery-public-data";
    String datasetName = "samples";
    listTables(projectId, datasetName);
  }

  public static List<String> listTables(String projectId, String datasetName) {
    try {
      // Initialize client that will be used to send requests. This client only needs to be created
      // once, and can be reused for multiple requests.
      BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

      DatasetId datasetId = DatasetId.of(projectId, datasetName);
      Page<Table> tables = bigquery.listTables(datasetId, TableListOption.pageSize(100));
      //tables.iterateAll().forEach(table -> System.out.print(table.getTableId().getTable() + "\n"));

      //System.out.println("Tables listed successfully.");
      // ------------------------------------------------------
      // Addition to return a list of all the table names
      List<String> table_names = new ArrayList<>();
      tables.iterateAll().forEach(table -> table_names.add(projectId + "." + datasetName + "." + table.getTableId().getTable()));
      return table_names;
      // ------------------------------------------------------
    } catch (BigQueryException e) {
      System.out.println("Tables were not listed. Error occurred: " + e.toString());
    }
    return null;
  }
}
// [END bigquery_list_tables]
