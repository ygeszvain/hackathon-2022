"""
This is a boilerplate pipeline
generated using Kedro 0.18.1
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import make_predictions, report_accuracy, split_data,\
    retrieve_data_merchant_data, retrieve_data_banking_data, merge_data


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
                func=merge_data,
                inputs=["merchant_data_load", "banking_data_load", "parameters"],
                outputs="merge_data@pyspark",
                name="merge_data",
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
