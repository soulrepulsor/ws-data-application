from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import min, sqrt, pow
from tools import SparkTools
from cleanup import Cleanup

import os
import sys


class Label:

    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark
        self._import_files()

    def _import_files(self):
        """
        Import needed files
        :return: N/A
        """
        path = os.path.dirname(os.path.join(__file__))
        self._spark.sparkContext.addPyFile(os.path.join(path, 'tools.py'))
        self._spark.sparkContext.addPyFile(os.path.join(path, 'cleanup.py'))

        sys.path.insert(0, 'tools.py')
        sys.path.insert(0, 'cleanup.py')

    def _complete_previous_process(self) -> DataFrame:
        """
        Retrieves the previous step's DataFrame
        :return: previous task's DataFrame
        """
        cleaning = Cleanup(self._spark)
        return cleaning.process()

    def process(self) -> DataFrame:
        """
        Labels each request to the closest POI based on its longitude and latitude
        :return: Returns similar DataFrame from the previous task but with
                 assigned closest POI
        """
        path = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(path, 'POIList.csv')

        tools = SparkTools(self._spark)

        clean_df = self._complete_previous_process()
        poi_df = tools.csv_to_data_frame(file_path)

        poi_df = poi_df.dropDuplicates(poi_df.drop('POIID').columns)

        joint_df = clean_df.select(['_ID', 'Latitude', 'Longitude']).crossJoin(
            poi_df.withColumnRenamed('Latitude', 'POI_LAT')
                .withColumnRenamed('Longitude', 'POI_LON')
        )

        joint_df = joint_df.withColumn(
            'Distance', sqrt(
                pow((joint_df['Latitude'] - joint_df['POI_LAT']), 2) +
                pow((joint_df['Longitude'] - joint_df['POI_LON']), 2)
            ))

        group_data = joint_df.groupBy(['Latitude', 'Longitude']).agg(
            min('Distance')
        ).withColumnRenamed('min(Distance)', 'Distance')

        joint_df = joint_df.join(group_data,
                                 on=['Latitude', 'Longitude', 'Distance'],
                                 how='left_semi')

        joint_df = clean_df.join(
            joint_df.select(['_ID', 'POIID', 'Distance']), on=['_ID'],
            how='left'
        )

        return joint_df


if __name__ == '__main__':
    spark = SparkSession.builder.appName('Label').getOrCreate()
    label = Label(spark)
    test = label.process()
    # test.show()
    # print(test.count())
    # test.printSchema()
