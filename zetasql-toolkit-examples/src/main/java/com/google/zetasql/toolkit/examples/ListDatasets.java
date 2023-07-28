/*
 * Copyright 2019 Google LLC
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
import java.util.Collections;
import java.util.List;

// [START bigquery_list_datasets]
import com.google.api.gax.paging.Page;
import com.google.cloud.PageImpl;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQuery.DatasetListOption;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;

public class ListDatasets {

  public static void main(String[] args) {
    // TODO(developer): Replace these variables before running the sample.
    String projectId = "financialreporting-223818";
    listDatasets(projectId);
  }

  public static List<String> listDatasets(String projectId) {
    try {
      // Initialize client that will be used to send requests. This client only needs to be created
      // once, and can be reused for multiple requests.
      BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
      

      Page<Dataset> datasets = bigquery.listDatasets(projectId, DatasetListOption.pageSize(100));
      
      if (datasets == null) {
        System.out.println("Dataset does not contain any models");
        return null;
      }
      // datasets
      //     .iterateAll()
      //     .forEach(
      //         dataset -> System.out.printf("Success! Dataset ID: %s ", dataset.getDatasetId()));

      // ------------------------------------------------------
      // Addition to return a list of all the dataset names
      List<String> dataset_names = new ArrayList<>();
      datasets.iterateAll().forEach(dataset -> dataset_names.add(dataset.getDatasetId().getDataset()));
      return dataset_names;
      // ------------------------------------------------------
    } catch (BigQueryException e) {
      System.out.println("Project does not contain any datasets \n" + e.toString());
    }
    return null;
  }
}
// [END bigquery_list_datasets]
