"""
This is a boilerplate pipeline
generated using Kedro 0.18.1
"""

import logging
from typing import Dict, Tuple

from pandas import Series
from pyspark.sql.window import Window
from pyspark.sql.functions import rank
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, VectorSlicer
import numpy as np
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
import pyspark.sql.functions as func
import pandas as pd
import requests
import json
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler


def retrieve_data_merchant_data(parameters: Dict) -> pd.DataFrame:
    from snowflake import connector
    ctx = connector.connect(
        user=parameters["user"],
        role=parameters["role"],
        password=parameters["password"],
        account=parameters["account"],
        warehouse=parameters["warehouse"])
    sql = f'select * from"ETHOS_INGESTION"."AUDIT"."{parameters["merchant_table"]}" where mcc = 5812'
    cur = ctx.cursor()
    cur.execute(sql)
    snowflake_df = cur.fetch_pandas_all()

    return snowflake_df


def retrieve_data_banking_data(parameters: Dict) -> pd.DataFrame:
    from snowflake import connector
    ctx = connector.connect(
        user=parameters["user"],
        role=parameters["role"],
        password=parameters["password"],
        account=parameters["account"],
        warehouse=parameters["warehouse"])
    sql = f'select * from"ETHOS_INGESTION"."AUDIT"."{parameters["banking_table"]}"'
    cur = ctx.cursor()
    cur.execute(sql)
    snowflake_df = cur.fetch_pandas_all()

    return snowflake_df


def retrieve_data_aggregated_data(parameters: Dict) -> pd.DataFrame:
    from snowflake import connector
    ctx = connector.connect(
        user=parameters["user"],
        role=parameters["role"],
        password=parameters["password"],
        account=parameters["account"],
        warehouse=parameters["warehouse"])
    sql = parameters["agg_sql"]
    cur = ctx.cursor()
    cur.execute(sql)
    snowflake_df = cur.fetch_pandas_all()

    return snowflake_df


def data_modeling_merchant(merchant_data: DataFrame, parameters: Dict) -> DataFrame:
    sales_columns = [col('TOT_SALES2018'), col('TOT_SALES2019'), col('TOT_SALES2020'), col('TOT_SALES2021')]
    averageFunc = sum(x for x in sales_columns) / len(sales_columns)
    new_df = merchant_data.withColumn('AVG_SALES', averageFunc)
    new_df = new_df.withColumn('SALES_TREND_1',
                               (new_df["TOT_SALES2019"] - new_df["TOT_SALES2018"]) / new_df["TOT_SALES2019"])
    new_df = new_df.withColumn('SALES_TREND_2',
                               (new_df["TOT_SALES2020"] - new_df["TOT_SALES2019"]) / new_df["TOT_SALES2019"])
    new_df = new_df.withColumn('SALES_TREND_3',
                               (new_df["TOT_SALES2021"] - new_df["TOT_SALES2020"]) / new_df["TOT_SALES2020"])

    cnt_columns = [col('TOT_SALE_CNT2018'), col('TOT_SALES_CNT2019'), col('TOT_SALE_CNT2020'), col('TOT_SALE_CNT2021')]
    averageFunc = sum(x for x in cnt_columns) / len(cnt_columns)
    new_df = new_df.withColumn('AVG_SALES_CNT', func.round(averageFunc, 0))
    new_df = new_df.withColumn('SALES_CNT_TREND_1',
                               (new_df["TOT_SALES_CNT2019"] - new_df["TOT_SALE_CNT2018"]) / new_df["TOT_SALE_CNT2018"])
    new_df = new_df.withColumn('SALES_CNT_TREND_2',
                               (new_df["TOT_SALE_CNT2020"] - new_df["TOT_SALES_CNT2019"]) / new_df["TOT_SALES_CNT2019"])
    new_df = new_df.withColumn('SALES_CNT_TREND_3',
                               (new_df["TOT_SALE_CNT2021"] - new_df["TOT_SALE_CNT2020"]) / new_df["TOT_SALE_CNT2020"])

    new_df = new_df.na.drop()
    return new_df


def data_cleaning_banking(merchant_data: DataFrame, banking_data: DataFrame, parameters: Dict) -> DataFrame:
    return banking_data


def census_data(merchant_data: DataFrame, parameters: Dict) -> DataFrame:
    zipcode_list = merchant_data.select("ZIPCODE").rdd.flatMap(list).collect()
    apiKey = parameters['apiKey']
    lookup = parameters['census_lookup']
    df = pd.DataFrame(columns=['zipcode'], data=zipcode_list)
    df['zipcode'] = df['zipcode'].astype("string")
    for year in ['2020']:
        for key in lookup:
            calledAPI = f"https://api.census.gov/data/{year}/acs/acs5?get=NAME,{key}&for=zip%20code%20tabulation%20area:*&key={apiKey}"
            response = requests.get(calledAPI)
            formattedResponse = json.loads(response.text)[1:]
            formattedResponse = [item[-2:] for item in formattedResponse]
            response_df = pd.DataFrame(columns=[lookup[key] + '_' + year, 'zipcode'], data=formattedResponse)
            df = pd.merge(df, response_df, on='zipcode', how='left')
    for year in ['2019', '2018']:
        for key in lookup:
            calledAPI = f"https://api.census.gov/data/{year}/acs/acs5?get=NAME,{key}&for=zip%20code%20tabulation%20area:*&key={apiKey}"
            response = requests.get(calledAPI)
            formattedResponse = json.loads(response.text)[1:]
            formattedResponse = [item[::-2] for item in formattedResponse]
            response_df = pd.DataFrame(columns=['zipcode', lookup[key] + '_' + year], data=formattedResponse)
            df = pd.merge(df, response_df, on='zipcode', how='left')
    return df


def merge_data(merchant_data: DataFrame, agg_data: DataFrame, census_data: DataFrame, parameters: Dict) -> DataFrame:
    cols = ("mcc", "population")
    agg_data = agg_data.drop(*cols)
    new_df = merchant_data.join(agg_data, on=['BILLING_STATE', 'CITY'], how='inner')
    new_df = new_df.join(census_data, on=['zipcode'], how='inner')
    new_df = new_df.na.drop()
    return new_df


def prediction(merchant_data: DataFrame, parameters: Dict) -> DataFrame:
    return merchant_data


def ranking(merchant_data: DataFrame, parameters: Dict) -> DataFrame:
    new_df = merchant_data.withColumn('SALES_TREND_WEIGHTED',
                                      (merchant_data["SALES_TREND_1"] * parameters["sales_trend_1_weight"] +
                                       merchant_data["SALES_TREND_2"] * parameters["sales_trend_2_weight"]))
    new_df = new_df.withColumn('SALE_CNT_TREND_WEIGHTED',
                               (merchant_data["SALES_CNT_TREND_1"] * parameters["sale_cnt_1_weight"] +
                                merchant_data["SALES_CNT_TREND_2"] * parameters["sale_cnt_2_weight"]))
    new_df = new_df.withColumn('TREND_WEIGHTED',
                               (new_df["SALES_TREND_WEIGHTED"] + new_df["SALE_CNT_TREND_WEIGHTED"]) / 2)

    windowSpec = Window.orderBy("TREND_WEIGHTED")
    new_df = new_df.withColumn("TREND_RANKING", rank().over(windowSpec))
    return new_df


def feature_importance(merge_data: DataFrame, parameters: Dict) -> DataFrame:
    def ExtractFeatureImp(featureImp, dataset, featuresCol):
        list_extract = []
        for i in dataset.schema[featuresCol].metadata["ml_attr"]["attrs"]:
            list_extract = list_extract + dataset.schema[featuresCol].metadata["ml_attr"]["attrs"][i]
        varlist = pd.DataFrame(list_extract)
        varlist['score'] = varlist['idx'].apply(lambda x: featureImp[x])
        return varlist.sort_values('score', ascending=False)

    transformed = merge_data.limit(5000)
    print(transformed)
    input_col = 'TOT_SALES2021'
    encoding_var = [i[0] for i in transformed.dtypes if (i[1] == 'string') & (i[0] != input_col)]
    num_var = [i[0] for i in transformed.dtypes if ((i[1] == 'int') | (i[1] == 'double')) & (i[0] != input_col)]

    string_indexes = [StringIndexer(inputCol=c, outputCol='IDX_' + c, handleInvalid='keep') for c in encoding_var]
    onehot_indexes = [OneHotEncoder(inputCols=['IDX_' + c], outputCols=['OHE_' + c]) for c in encoding_var]
    label_indexes = StringIndexer(inputCol=input_col, outputCol='label', handleInvalid='keep')
    assembler = VectorAssembler(inputCols=num_var + ['OHE_' + c for c in encoding_var], outputCol="features")
    rf = RandomForestClassifier(labelCol="label", featuresCol="features", seed=8464,
                                numTrees=10, cacheNodeIds=True, subsamplingRate=0.7)

    pipe = Pipeline(stages=string_indexes + onehot_indexes + [assembler, label_indexes, rf])
    mod = pipe.fit(transformed)
    transformed = mod.transform(transformed)
    print(mod.stages[-1].featureImportances)
    result_df = ExtractFeatureImp(mod.stages[-1].featureImportances, transformed, "features")
    print(result_df)
    return result_df


def scoring_report(merge_data: DataFrame, parameters: Dict) -> DataFrame:
    def procentual_proximity(source_data: list, weights: list) -> list:

        """
        weights - int list
        possible values - 0 / 1
        0 if lower values have higher weight in the data set
        1 if higher values have higher weight in the data set
        """

        # getting data
        data_lists = []
        for item in source_data:
            for i in range(len(item)):
                try:
                    data_lists[i].append(float(item[i]))
                except IndexError:
                    # generate corresponding number of lists
                    data_lists.append([])
                    data_lists[i].append(float(item[i]))

        score_lists = []
        # calculating each score
        for dlist, weight in zip(data_lists, weights):
            mind = min(dlist)
            maxd = max(dlist)

            score = []
            # for weight 0 score is 1 - actual score
            if weight == 0:
                for item in dlist:
                    try:
                        score.append(1 - ((item - mind) / (maxd - mind)))
                    except ZeroDivisionError:
                        score.append(1)

            elif weight == 1:
                for item in dlist:
                    try:
                        score.append((item - mind) / (maxd - mind))
                    except ZeroDivisionError:
                        score.append(0)

            # weight not 0 or 1
            else:
                raise ValueError("Invalid weight of %f provided" % (weight))

            score_lists.append(score)

        # initialize final scores
        final_scores = [0 for i in range(len(score_lists[0]))]

        # generate final scores
        for i, slist in enumerate(score_lists):
            for j, ele in enumerate(slist):
                final_scores[j] = final_scores[j] + ele

        # append scores to source data
        for i, ele in enumerate(final_scores):
            source_data[i].append(ele)

        return source_data
    print(merge_data.columns)
    cols = parameters['scoring_cols']
    input_list = merge_data[cols].values.tolist()
    output_list = procentual_proximity(input_list, parameters['scoring_weight'])
    scoring_df = pd.DataFrame(columns=parameters['scoring_cols']+['SCORE'], data=output_list)
    # print(input_list)
    weight = pd.DataFrame(Series(parameters['sum_weight'], index=cols, name=0))
    merge_data['WEIGHTED_SUM'] = round(merge_data[cols].dot(weight), 2)
    print(merge_data)
    result_df = merge_data[['MERCHANT_ID', 'MERCHANT_NAME', 'ZIPCODE', 'BILLING_STATE', 'CITY', 'WEIGHTED_SUM']]
    print(result_df)
    frames = [scoring_df, result_df]
    result_df = pd.concat(frames, axis=1)
    result_df = result_df.drop_duplicates()
    return result_df

# def split_data(data: DataFrame, parameters: Dict) -> Tuple:
#     """Splits data into features and targets training and test sets.
#
#     Args:
#         data: Data containing features and target.
#         parameters: Parameters defined in parameters.yml.
#     Returns:
#         Split data.
#     """
#
#     # Split to training and testing data
#     data_train, data_test = data.randomSplit(
#         weights=[parameters["train_fraction"], 1 - parameters["train_fraction"]]
#     )
#
#     X_train = data_train.drop(parameters["target_column"])
#     X_test = data_test.drop(parameters["target_column"])
#     y_train = data_train.select(parameters["target_column"])
#     y_test = data_test.select(parameters["target_column"])
#
#     return X_train, X_test, y_train, y_test


# def make_predictions(
#         X_train: pd.DataFrame, X_test: pd.DataFrame, y_train: pd.DataFrame
# ) -> DataFrame:
#     """Uses 1-nearest neighbour classifier to create predictions.
#
#     Args:
#         X_train: Training data of features.
#         y_train: Training data for target.
#         X_test: Test data for features.
#
#     Returns:
#         y_pred: Prediction of the target variable.
#     """
#
#     X_train_numpy = X_train.to_numpy()
#     X_test_numpy = X_test.to_numpy()
#
#     squared_distances = np.sum(
#         (X_train_numpy[:, None, :] - X_test_numpy[None, :, :]) ** 2, axis=-1
#     )
#     nearest_neighbour = squared_distances.argmin(axis=0)
#     y_pred = y_train.iloc[nearest_neighbour]
#     y_pred.index = X_test.index
#
#     return y_pred
#
#
# def report_accuracy(y_pred: pd.Series, y_test: pd.Series):
#     """Calculates and logs the accuracy.
#
#     Args:
#         y_pred: Predicted target.
#         y_test: True target.
#     """
#     accuracy = (y_pred == y_test).sum() / len(y_test)
#     logger = logging.getLogger(__name__)
#     logger.info("Model has an accuracy of %.3f on test data.", accuracy)
