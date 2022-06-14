"""
This is a boilerplate pipeline
generated using Kedro 0.18.1
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import retrieve_data_merchant_data, retrieve_data_banking_data, data_modeling_merchant, ranking, \
    retrieve_data_aggregated_data, merge_data, census_data, feature_importance


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=retrieve_data_merchant_data,
                inputs=["parameters"],
                outputs="merchant_data",
                name="retrieve_merchant_data",
            ),
            node(
                func=retrieve_data_banking_data,
                inputs=["parameters"],
                outputs="banking_data",
                name="retrieve_banking_data",
            ),
            node(
                func=retrieve_data_aggregated_data,
                inputs=["parameters"],
                outputs="agg_data",
                name="retrieve_data_aggregated_data",
            ),
            node(
                func=data_modeling_merchant,
                inputs=["merchant_data_load", "parameters"],
                outputs="data_modeling_merchant@pyspark",
                name="data_modeling_merchant",
            ),
            node(
                func=ranking,
                inputs=["data_modeling_merchant@pyspark", "parameters"],
                outputs="ranking_data@pyspark",
                name="make_ranking",
            ),
            node(
                func=census_data,
                inputs=["data_modeling_merchant@pyspark", "parameters"],
                outputs="census_data@pandas",
                name="census_data",
            ),
            node(
                func=merge_data,
                inputs=["ranking_data@pyspark", "agg_data_load", "census_data_load", "parameters"],
                outputs="merge_data@pyspark",
                name="merge_data",
            ),
            node(
                func=feature_importance,
                inputs=["merge_data@pyspark", "parameters"],
                outputs="feature_importance_output@pandas",
                name="feature_importance",
            ),
            # node(
            #     func=split_data,
            #     inputs=["example_iris_data", "parameters"],
            #     outputs=[
            #         "X_train@pyspark",
            #         "X_test@pyspark",
            #         "y_train@pyspark",
            #         "y_test@pyspark",
            #     ],
            #     name="split",
            # ),
            # node(
            #     func=make_predictions,
            #     inputs=["X_train@pandas", "X_test@pandas", "y_train@pandas"],
            #     outputs="y_pred",
            #     name="make_predictions",
            # ),
            # node(
            #     func=report_accuracy,
            #     inputs=["y_pred", "y_test@pandas"],
            #     outputs=None,
            #     name="report_accuracy",
            # ),
        ]
    )
