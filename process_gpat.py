#!/bin/python
import pyspark.sql.functions as F
from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from loguru import logger

spark = SparkSession.builder.getOrCreate()

a = spark.read.parquet('../gpat_202111').filter("country_code='US'")

a = a.withColumn('patnum', F.split('publication_number',
                 '-').getItem(1).cast(IntegerType())).dropna(subset=('patnum'))
a = a.orderBy(['patnum', 'grant_date']).dropDuplicates(subset=['patnum'])
# only keep granted patents
a = a.filter(F.col('kind_code').isin(['A', 'B1', 'B2']))

title = a.select('patnum', F.col('title_localized').getItem(
    0).getItem('text').alias('title'))
abstract = a.select('patnum', F.col('abstract_localized').getItem(
    0).getItem('text').alias('abstract'))

logger.info('process claims')
claims = a.select('patnum', F.col('claims_localized').getItem(
    0).getItem('text').alias('claims'))
claims = claims.withColumn('claims', F.split('claims', '\n')
                           ).withColumn('claims', F.explode('claims')
                                        ).withColumn('claims', F.trim('claims')
                                                     ).withColumn('claims', F.lower('claims')
                                                                  ).orderBy('patnum')

claims = claims.filter(F.col('claims').rlike(
    '[a-zA-Z]+')).filter(F.col('claims').rlike('^\d+\.\s+'))
claims.write.mode('overwrite').parquet('claims')
logger.info('finished process claims ....')

logger.info('process patent')
a.select(['publication_number', 'application_number', 'kind_code',
          'application_kind', 'application_number_formatted',
          'pct_number', 'family_id', 'publication_date', 'filing_date',
          'grant_date', 'priority_date', 'patnum']
         ).join(title, 'patnum', 'left'
                ).join(abstract, 'patnum', 'left'
                       ).write.mode('overwrite').parquet('patent')

logger.info('process citations')
citation = a.select('patnum', F.explode('citation').alias(
    'citation')).select('patnum', F.col("citation.*"))

citation.withColumn('country', F.split('publication_number', '-').getItem(0)
                    ).withColumn('pub_number', F.split('publication_number', '-').getItem(1)
                                 ).write.mode('overwrite').parquet('citation')

# logger.info('process description')
# a.select('patnum', F.explode('description_localized').alias('description')
#          ).select('patnum', F.col("description.*")).select('patnum', F.col('text').alias('description')
# ).write.mode('overwrite').parquet('description')

logger.info('process others')
a.select('patnum', F.explode('uspc').alias('uspc')
         ).select('patnum', F.col("uspc.*")).withColumnRenamed('code', 'uspc').write.mode('overwrite').parquet('uspc')

a.select('patnum', F.explode('ipc').alias('ipc')
         ).select('patnum', F.col("ipc.*")).withColumnRenamed('code', 'ipc').write.mode('overwrite').parquet('ipc')

a.select('patnum', F.explode('cpc').alias('cpc')
         ).select('patnum', F.col("cpc.*")).withColumnRenamed('code', 'cpc').write.mode('overwrite').parquet('cpc')

a.select('patnum', F.explode('inventor_harmonized').alias('inventor')
         ).select('patnum', F.col("inventor.*")).withColumnRenamed('name', 'inventor').write.mode('overwrite').parquet('inventor')

a.select('patnum', F.explode('assignee_harmonized').alias('assignee')
         ).select('patnum', F.col("assignee.*")).withColumnRenamed('name', 'assignee').write.mode('overwrite').parquet('assignee')

a.select('patnum', F.explode('examiner').alias('examiner')
         ).select('patnum', F.col("examiner.*")).withColumnRenamed('name', 'examiner').write.mode('overwrite').parquet('examiner')

a.select('patnum', F.explode('priority_claim').alias('priority_claim')
         ).select('patnum', F.col("priority_claim.*")).write.mode('overwrite').parquet('priority_claim')
