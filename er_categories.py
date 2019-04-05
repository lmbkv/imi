# entity_resolution.py
import re
import operator
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession, functions, types
import pandas as pd


# anomaly_detection.py
from pyspark.mllib.clustering import KMeans, KMeansModel
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import udf, lit
import operator
spark = SparkSession.builder.appName('param').getOrCreate()
sc = spark.sparkContext
import sys
from pyspark.sql.functions import explode, split, concat, pandas_udf

import csv

def read_reviews(fn):

	df = spark.read.csv(inputs, schema=tmax_schema)
	links_list = []
	with open(fn, newline='') as csvfile:
		asin_reader = csv.reader(csvfile, delimiter=',')
		for row in asin_reader:
			links_list.append(row[2])
	return links_list

class EntityResolution:
	def __init__(self, dataFile1, dataFile2, stopWordsFile):
		'''
		self.f = open(stopWordsFile, "r")
		self.stopWords = set(self.f.read().split("\n"))
		self.stopWordsBC = sc.broadcast(self.stopWords).value
		'''
		#self.df1 = spark.read.parquet(dataFile1).cache()
		#self.df2 = spark.read.parquet(dataFile2).cache()
		self.df1 = dataFile1
		self.df2 = dataFile2

	def removeWords(self, stopWords):
		def _inner(s):
			for word in s:
				if word in stopWords or word == '':
					s.remove(word)
			return s
		return _inner

	def preprocessDF(self, df, cols): 
		#df.show()
		#df_new = df.select(df['asin'], functions.concat(cols[0], functions.lit(" "), cols[1]).alias('joinKey'))
		def split_string_full(cat):
			line = re.sub('[\&]', '', cat)
			line = line.replace("  ", " ")
			line = list(map(str, line.split(", ")))
			line = list(filter(None, line))
			return line

		def split_string(cat):
			line = re.sub('[\&]', '', cat)
			line = line.replace("  ", " ")
			line = list(map(str, line.split()))
			line = list(filter(None, line))
			return line
			#return cat.replace(",","").replace("&","")


		df['tokens'] = df['category'].apply(split_string)

		return(df)

	def preprocessCats(self, df, cols): 
		#df.show()
		#df_new = df.select(df['asin'], functions.concat(cols[0], functions.lit(" "), cols[1]).alias('joinKey'))
		def split_string_full(cat):
			line = re.sub('[\&]', '', cat)
			line = line.replace("  ", " ")
			line = list(map(str, line.split(", ")))
			line = list(filter(None, line))
			return line

		def split_string(cat):
			#line = re.sub('[\&]', '', cat)
			#line = line.replace("  ", " ")
			line = list(map(str, cat.split()))
			line = list(filter(None, line))
			return line
			#return cat.replace(",","").replace("&","")

		df['product_category'] = df['product_category'].apply(split_string)
		#print(df['tokens'].iloc[0])

		#print(df)
		return(df)


	def filtering(self, df1, df2):
		#df1.show()
		#df2.show()
		df1 = spark.createDataFrame(df1)
		df2 = spark.createDataFrame(df2)

		df1.createOrReplaceTempView('df1')
		df2.createOrReplaceTempView('df2')

		df1_flat = df1.select(df1['asin'], functions.explode(df1['tokens']).alias('tokens'))
		#df2_flat = df2.select('id', 'product_category')
		df2_flat = df2.select(df2['id'], functions.explode(df2['product_category']).alias('product_category'))

		df1_flat.createOrReplaceTempView('df1_flat')
		df2_flat.createOrReplaceTempView('df2_flat')

		df_pairs = spark.sql(
		"""SELECT df1_flat.asin as asin, df1_flat.tokens as tokens, df2_flat.id as id, df2_flat.product_category as product_category
			FROM df1_flat, df2_flat
			WHERE df1_flat.tokens = df2_flat.product_category AND df1_flat.tokens != "" AND df2_flat.product_category != "" 
		"""
			)

		df_pairs.createOrReplaceTempView('df_pairs')

		candDF = spark.sql(
		"""SELECT df1.asin as asin, df1.tokens as tokens, df2.id as id, df2.product_category as product_category
			FROM df1
			INNER JOIN df_pairs pairs ON df1.asin = pairs.asin
			INNER JOIN df2 ON pairs.id = df2.id
		"""
			)

		
		#candDF = candDF.dropDuplicates()
		candDF = candDF.dropDuplicates()
		#candDF.show()

		return(candDF)

	def verification(self, candDF, threshold):

		def jaccCalculate(jk1, jk2):
			inter = len(set.intersection(*[set(jk1), set(jk2)]))
			uni = len(set.union(*[set(jk1), set(jk2)]))
			return inter/float(uni)

		inter_func = functions.udf(jaccCalculate, types.FloatType())
		candDF = candDF.withColumn("intersect", inter_func(candDF["tokens"], candDF["product_category"]))
		candDF.show()
		resultDF = candDF.filter(candDF["intersect"] >= threshold)
		#resultDF.show()

		return resultDF


	def jaccardJoin(self, cols1, cols2, threshold):
		newDF1 = self.preprocessDF(self.df1, "") #, cols1)
		newDF2 = self.preprocessCats(self.df2, "") #, "") #, cols2)
		#print(newDF1.iloc[0])
		#print(newDF2.iloc[0])

		#print ("Before filtering: %d pairs in total" %(self.df1.count()*self.df2.count()))

		candDF = self.filtering(newDF1, newDF2)
		#rint(candDF)
		print ("After Filtering: %d pairs left" %(candDF.count()))

		resultDF = self.verification(candDF, threshold)
		print ("After Verification: %d similar pairs" %(resultDF.count()))

		return resultDF

	#def __del__(self):
		#self.f.close()

if __name__ == "__main__":

	spark = SparkSession.builder.appName('example code').getOrCreate()
	assert spark.version >= '2.3' # make sure we have Spark 2.3+
	spark.sparkContext.setLogLevel('WARN')
	sc = spark.sparkContext

	'''

	reviews_schema = types.StructType([
		types.StructField('asin', types.StringType()),
		types.StructField('product_title', types.StringType()),
		types.StructField('rating', types.FloatType()),
		types.StructField('review_title', types.StringType()),
		types.StructField('variation', types.StringType()),
		types.StructField('review_text', types.StringType()),
		types.StructField('review-links', types.StringType()),
		types.StructField('review-date', types.StringType()),
		types.StructField('verified', types.StringType()),
	])
	'''

	inputs = "output_cat.csv"
	frame = pd.read_csv(inputs)
	inputs = 'categories_amazon_dataset.csv'
	categories_data = pd.read_csv(inputs)

	#df = spark.read.csv(inputs, reviews_schema, sep=';', header=True)
	


	reviews = frame.groupby(['asin', 'category']).agg({'review_title': 'count', 'rating': 'mean'}) #.unstack(fill_value=0).reset_index().rename_axis(None, axis=1)
	print(reviews.index)
	
	reviews['ind'] = reviews.index
	reviews['asin'], reviews['category'] = zip(*reviews.ind)
	product_list = reviews.reset_index(drop=True)

	print(product_list)

	#reviews.to_csv("reviews.csv")

	
	#print(reviews)

	er = EntityResolution(product_list, categories_data, "")
	resultDF = er.jaccardJoin("", "", 0.001)

	resultDF = resultDF.sort(col("asin").desc())
	#resultDF = resultDF.select("asin", "id", "category", max("intersect"))

	resultDF.createOrReplaceTempView('resultDF')

	categories_for_asins = spark.sql(
	"""SELECT a.asin, a.intersect, a.id, a.product_category
		FROM resultDF a
		INNER JOIN (
		    SELECT asin, MAX(intersect) inter
		    FROM resultDF
		    GROUP BY asin
		) b ON a.asin = b.asin AND a.intersect = b.inter
	"""
		)

	categories_for_asins = categories_for_asins.toPandas()

	products_with_cats = product_list.merge(categories_for_asins, on='asin')

	print(products_with_cats.dtypes)
	products_with_cats = products_with_cats.rename(columns={'review_title':'count_reviews'})
	products_with_cats = products_with_cats[['asin', 'product_category', 'count_reviews', 'rating']]

	products_with_cats.to_csv("products_with_cats.csv")

	'''
	
	er = EntityResolution("Amazon_sample", "Google_sample", "stopwords.txt")
	amazonCols = ["title", "manufacturer"]
	googleCols = ["name", "manufacturer"]

	resultDF = er.jaccardJoin(amazonCols, googleCols, 0.5)

	result = resultDF.rdd.map(lambda row: (row.id1, row.id2)).collect()

	groundTruth = spark.read.parquet("Amazon_Google_perfectMapping_sample").rdd \
						  .map(lambda row: (row.idAmazon, row.idGoogle)).collect()

	print ("(precision, recall, fmeasure) = ", er.evaluate(result, groundTruth))
	'''
