from pyspark.sql import SparkSession, DataFrame
from tools import SparkTools
from typing import Optional
import os
import sys

class Cleanup:

    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark
        self._import_files()

    def process(self) -> Optional[DataFrame]:
        """
        Remove duplicated requests based on geo location and request time
        :return: Dataframe without any duplicated requests
        """
        path = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(path, 'DataSample.csv')

        tools = SparkTools(self._spark)

        df = tools.csv_to_data_frame(file_path)

        if df is None:
            return None

        test = df.dropDuplicates(df.drop(df['_ID']).columns)

        return test

    def _import_files(self) -> None:
        """
        Import needed files
        :return: N/A
        """
        path = os.path.dirname(os.path.abspath(__file__))
        self._spark.sparkContext.addPyFile(os.path.join(path, 'tools.py'))
        sys.path.insert(0, 'tools.py')


if __name__ == '__main__':
    spark = SparkSession.builder.appName('Cleanup').getOrCreate()
    clean_up = Cleanup(spark)
    test = clean_up.process()
    print(test.count())
    test.printSchema()
