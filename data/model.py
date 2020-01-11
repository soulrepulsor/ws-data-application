from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, mean, stddev, round
from pyspark.sql.types import StringType
from analysis import Analysis

import os
import sys
import math


class Model:

    def __init__(self, spark: SparkSession):
        self._spark = spark
        self._import_files()

    def _import_files(self):
        """
        Import needed files
        :return: N/A
        """
        path = os.path.dirname(os.path.join(__file__))
        self._spark.sparkContext.addPyFile(os.path.join(path, 'analysis.py'))

        sys.path.insert(0, 'analysis.py')

    def _complete_previous_process(self) -> DataFrame:
        """
        Retrieves the previous step's DataFrame without outliers
        :return: previous task's DataFrame without outliers
        """
        analysis = Analysis(self._spark)
        return analysis.process(True)

    def process(self) -> DataFrame:
        """
        Return each POI's popularity by using its density and standard deviation
        :return: A DataFame that contains each POI's popularity
        """
        main_df = self._complete_previous_process()

        df = main_df.withColumn('ID', lit('1').cast(StringType()))

        group_data = df.groupBy('ID')

        group_df = group_data.agg(mean('Density'), stddev('Density'))

        df = df.join(group_df, on='ID', how='left')

        result = self._compute_popularity(df)

        result = result.withColumn(
            'Popularity', round(result['Popularity'], 2)
        )

        result = result.select('POIID', 'Popularity').orderBy(
            result['POIID'].asc()
        )

        return result

    def _normal_distribution_cdf(self, value: float) -> float:
        """
        Computes the CDF of a given value in a Standard Normal Distribution
        :param value: the point of interest in the Standard Normal Distribution
        :return: CDF value at the given point
        """
        return (1 + math.erf(value / math.sqrt(2))) / 2

    def _compute_popularity(self, df: DataFrame) -> DataFrame:
        """
        Compute each POI's popularity by removing the outliers to make sure each
        POI's density is not skewed. Each POI's density is then used to create a
        new standard normal distribution. The POI's density is then used to
        find the probability in the normal distribution, which becomes
        the popularity index for the POI. Since the probability is scaled
        between 0 and 1, then it is multiplied by 10 to upscale the result to
        between 0 and 10. The positive and negative signs are preserved thus,
        the popularity would be scaled between -10 to 10, which 0 is the average
        of the calculated distribution. Note that the generated distribution is
        sensitive to each POI's density which is affected by the mean of the
        included requests
        :param df: each POI's mean, density and radius
        :return: DataFrame that contains popularity index for each POI
        """
        rows = df.rdd.map(lambda row: row.asDict()).collect()

        result = []

        for r in rows:

            value = (r['Density'] - r['avg(Density)']) / r[
                'stddev_samp(Density)']

            if value < 0:
                r['Popularity'] = - (self._normal_distribution_cdf(value) * 10)
            else:
                r['Popularity'] = (self._normal_distribution_cdf(
                    value) - 0.5) * 10

            result.append(r)

        return self._spark.read.json(self._spark.sparkContext.parallelize(
            result
        ))


if __name__ == '__main__':
    spark = SparkSession.builder.appName('Model').getOrCreate()
    model = Model(spark)

    model.process().show()
