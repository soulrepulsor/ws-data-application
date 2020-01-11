from pyspark.sql import SparkSession, DataFrame
from typing import Optional

import os

class SparkTools:

    def __init__(self, spark: SparkSession):
        self._spark = spark

    def csv_to_data_frame(self, path: str) -> Optional[DataFrame]:
        """
        If the directory path is valid, returns the first csv file as a DataFrame
        :param path: the path of the directory
        :return: csv as a DataFrame
        """
        if os.path.isfile(path):
            filename, file_extension = os.path.splitext(path)
            if file_extension == '.csv':
                return self.trim_header(self._spark.read.csv(path, header=True))

        return None

    def trim_header(self, df: DataFrame) -> DataFrame:
        """
        Modifies the DataFrame's columns so its headers does not have whitespace
        in the beginning and the end of the name
        :param df: Any valid DataFrame
        :return: Same DataFrame but no leading and trailing white space in
                 column names
        """
        result = df
        for title in df.columns:
            result = result.withColumnRenamed(title, title.strip())

        return result
