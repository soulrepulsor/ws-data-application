from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, split
from pyspark.sql.types import StringType
from graphframes import GraphFrame
from typing import List, Dict, Optional
from itertools import permutations

import os
import sys


class Pipline:

    def __init__(self, spark: SparkSession):
        self._spark = spark

    def progress(self) -> Optional[DataFrame]:
        """
        Returns a DataFrame that includes all of the possible paths from starting
        to end task with the prerequisites
        :return: DataFrame that contains all of the prerequisites paths
        """
        path = os.path.dirname(os.path.join(__file__))

        task = self._spark.read.text(os.path.join(path, 'task_ids.txt'),
                                     lineSep=',')
        task = task.withColumnRenamed('value', 'id')

        relation = self._spark.read.text(os.path.join(path, 'relations.txt'),
                                         lineSep='\n')

        src_dst_split = split(relation['value'], '->')

        relation = relation.withColumn('src', src_dst_split.getItem(0)) \
            .withColumn('dst', src_dst_split.getItem(1)).drop(relation['value'])

        g = GraphFrame(task, relation).bfs(fromExpr='id=73', toExpr='id=36')

        vertices = g.drop(
            *g.select(g.colRegex('`^e\d`')).columns) \
            .select(g.colRegex('`^v\d`'), g['to'])

        vertices = self._get_vertices(vertices)

        test = vertices.rdd.map(lambda row: row.asDict()).collect()

        result = []

        for row in test:
            result.append(self._row_operation(row, vertices.columns, relation))

        return self._join(list(filter(None, result)))

    def _get_vertices(self, df: DataFrame) -> DataFrame:
        """
        Return every vertices' column in the DataFrame
        :param df: DataFrame that contains the path
        :return: DataFrame that contains only the vertices column
        """
        result = df
        for id in df.columns:
            key = id + '.id'
            result = result.withColumn(id, result[key])
        return result

    def _get_header(self, df: DataFrame) -> DataFrame:
        """
        Rename the DataFrame's header to Steps number for better readability
        :param df: DataFrame that contains each step as a column
        :return: DataFrame with labeled steps
        """
        string = 'Step '
        ctr = 1
        result = df
        for i in df.columns:
            query = string + str(ctr)
            result = result.withColumnRenamed(i, query)
            ctr += 1
        return result

    def _match(self, bigger: DataFrame, smaller: DataFrame) -> DataFrame:
        """
        Make sure that the number of columns in the smaller DataFrame has the
        same amount of columns filled with nulls as the the bigger DataFrame
        :param bigger: The DataFrame that has more columns
        :param smaller: The DataFrame that has less columns
        :return: The smaller DataFrame but with same amount of columns as the
                 bigger DataFrame
        """
        iterate = (bigger.columns)[len(smaller.columns)::]
        result = smaller
        for i in iterate:
            result = result.withColumn(i, lit(None).cast(StringType()))
        return result

    def _union(self, main: DataFrame, next: DataFrame) -> DataFrame:
        """
        Merge the next DataFrame's rows to the main DataFrame
        :param main: DataFrame that's being merged to
        :param next: DataFrame that's merging to the main DataFrame
        :return:
        """
        if len(main.columns) > len(next.columns):
            return main.union(self._match(main, next))
        elif len(main.columns) < len(next.columns):
            return next.union(self._match(next, main))
        return main.union(next)

    def _join(self, dfs: List[DataFrame]) -> Optional[DataFrame]:
        """
        Merging a list of DataFrames together
        :param dfs: List of DataFrames
        :return: A merged DataFrame
        """
        if len(dfs) <= 0:
            return None

        if len(dfs) == 1:
            return dfs[0]

        final = dfs[0]
        for df in dfs[1::]:
            final = self._union(final, df)
        return final

    def _row_operation(self, row: Dict, sorted_keys: List,
                       relation: DataFrame) -> Optional[DataFrame]:
        """
        Return a DataFrame that contains all possible paths with prerequisites
        :param row: Each row of the DataFrame
        :param sorted_keys: Steps Name in order
        :param relation: DataFrame that contains the relationship between each task
        :return: DataFrame that contains all possible paths with prerequisites
        """
        possibilities = []
        for col_key in sorted_keys:
            filtered = relation.filter(relation['dst'] == row[col_key])
            flat = filtered.select('src').rdd.flatMap(lambda x: x).collect()
            possibilities.append(list(permutations(flat)))

            if col_key == 'to':
                possibilities.append([(row[col_key],)])

        if len(possibilities) < 1:
            return None
        elif len(possibilities) == 1:
            return self._get_header(
                self._spark.createDataFrame(possibilities[0])
            )

        result = self._get_header(self._spark.createDataFrame(possibilities[0]))

        for item in possibilities[1::]:
            result = result.crossJoin(
                spark.createDataFrame(item)
            ).distinct()
            result = self._get_header(result)
        return result


if __name__ == '__main__':

    path = os.path.dirname(os.path.join(__file__))

    spark = SparkSession.builder.appName('Pipeline Dependency').getOrCreate()

    pipline = Pipline(spark)
    result = pipline.progress().show()
