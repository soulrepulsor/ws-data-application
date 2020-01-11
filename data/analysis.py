from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import max, mean, stddev, pow, count, abs
from label import Label

import math
import os
import sys


class Analysis:

    def __init__(self, spark: SparkSession):
        self._spark = spark
        self._import_files()

    def _import_files(self):
        """
        Import needed files
        :return: N/A
        """
        path = os.path.dirname(os.path.join(__file__))
        self._spark.sparkContext.addPyFile(os.path.join(path, 'label.py'))

        sys.path.insert(0, 'label.py')

    def _complete_previous_process(self) -> DataFrame:
        """
        Retrieves the previous step's DataFrame
        :return: previous task's DataFrame
        """
        label = Label(self._spark)
        return label.process()

    def _remove_outlier(self, data: DataFrame) -> DataFrame:
        """
        Removes the requests/outliers that is too far away from the POI, in hope
        to ensure the mean is not too skewed
        :param data: target DataFrame to remove the outliers
        :return: Modified inputted DataFrame that excludes the outliers
        """
        main_df = self._complete_previous_process()
        outlier = data.withColumn(
            'Outlier',
            data['Standard Deviation'] * 3 + data['Average']
        ).select('Outlier', 'POIID')

        df = main_df.join(outlier, on='POIID', how='left')

        df = df.filter(abs(df['Distance']) <= df['Outlier'])

        return self._get_stddev_mean_avg(df)

    def _get_stddev_mean_avg(self, df: DataFrame) -> DataFrame:
        """
        Returns the mean, standard deviation, and density of each POI as a DataFrame
        :param df: DataFrame that contains the POI
        :return: returns mean, standard deviation and density of each POI
        """
        group_data = df.groupBy(['POIID'])

        group_poi = group_data.agg(
            max('Distance'), mean('Distance'), stddev('Distance'),
            count('POIID')
        ) \
            .withColumnRenamed('max(Distance)', 'Radius') \
            .withColumnRenamed('avg(Distance)', 'Average') \
            .withColumnRenamed('stddev_samp(Distance)', 'Standard Deviation') \
            .withColumnRenamed('count(POIID)', 'Density')

        group_poi = group_poi.withColumn(
            'Density',
            group_poi['Density'] / (pow(group_poi['Radius'], 2) * math.pi)
        )

        return group_poi

    def process(self, outlier=False) -> DataFrame:
        """
        Returns each POI's standard deviation, average and density based on the given
        assigned requests
        :param outlier: include outliers in the data or not
        :return: each POI's standard deviation, average and density
        """
        main_df = self._complete_previous_process()

        data = self._get_stddev_mean_avg(main_df)

        if outlier:
            return self._remove_outlier(data)

        return data


if __name__ == '__main__':
    spark = SparkSession.builder.appName('Analysis').getOrCreate()
    analysis = Analysis(spark)
    test = analysis.process()
    test.show()
