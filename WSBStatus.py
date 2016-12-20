#!/usr/bin/python27-virtual-hadoop
#/opt/spark/1.5.1/bin/spark-submit --master yarn-client /home/hdp_oc_team/WSBStatus.py

import sys, os
import json
import datetime
from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *
from datetime import *
from pyspark.sql.functions import *
#from string import Template
#import math

# sqlContext.sql('use dbmarketing')
# dc = sqlContext.table('date_calendar') # OR
# dc = sqlContext.sql("select * from dbmarketing.date_calendar")
# dc.show(10) 
# dc.printSchema()
# dates = dc.select('calendar_date') # OR
# dates = dc.select(dc.calendar_date)
# dates.show(10) 
# dates.printSchema()

def main():

	def parseFeed(x):
		
		def setVal(inrow, outrow):
			for k in outrow:
				try:
					#print '||', outrow[k] = inrow[k]
					outrow[k] = inrow[k]
				except:
					pass
			return outrow
			
		x = x.asDict()
		try:
			# print '|||', type(x['properties']), x['properties']
			# x_properties = json.loads(str(x['properties']))
			x_properties = x['properties']
			y = x_properties['firstPublishUtcDateTime']
		except:
			# x_properties = dict()
			y = ''
		out = dict()
		out['shopper_id'] = ''
		out['status'] = ''
		out['website_id'] = ''
		out['update_date'] = ''
		out['firstPublishUtcDateTime'] = ''
		
		# out['firstPublishUtcDateTime'] = ''
		# out['lastPublishSuccessUtcDateTime'] = ''
		# out['onboardingCompletedUtcDateTime'] = ''
		out = setVal(x, out)
		out['firstPublishUtcDateTime'] = y
		# out['shopper_id'] = x['shopper_id']
		# out['status'] = x['status']
		# out['website_id'] = x['website_id']
		# out['firstPublishUtcDateTime'] = x['firstPublishUtcDateTime']
		# out['lastPublishSuccessUtcDateTime'] = x['lastPublishSuccessUtcDateTime']
		# out['onboardingCompletedUtcDateTime'] = x['onboardingCompletedUtcDateTime']
		return Row(**out)
		
	# initialize spark context
	conf = (SparkConf()
                 .setMaster("yarn-client")
                 .setAppName("WSB Daily Status Snapshot")
                 #.set("spark.executor.instances", "150")
                 # .set("spark.executor.cores", "1")
                 # .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                 # .set("spark.locality.wait", "10000")
                 # .set("spark.locality.wait.rack", "20000")
                 # .set("spark.eventLog.enabled", "true")
                 # .set("spark.akka.heartbeat.interval", "2000") # default = 1000
				 # .set("spark.akka.heartbeat.pauses", "12000") # default = 6000
                 # .set("spark.akka.frameSize", "64") # default = 10MB
                 # .set("spark.core.connection.ack.wait.timeout","600")
                 # .set("spark.executor.memory", "20g")
                 # .set("spark.driver.memory", "8g")
                 # .set("spark.yarn.executor.memoryOverhead", "2500")
                 # .set("spark.rdd.compress", "true")
                 # .set("spark.shuffle.memoryfraction", "0.9")
                 # .set("spark.storage.unrollFraction", "0.5")
                 # .set("spark.storage.memoryFraction", "0.1")
                 # .set("spark.sql.shuffle.partitions", "500") # default 200
                 # .set("spark.shuffle.consolidateFiles", "true")
 	                )
	sc = SparkContext(conf = conf)
	hc = HiveContext(sc)
	
	dt = datetime.now().date()
	print dir(dt)
	# yyyy = dt.year
	# mm = '%02d' % dt.month
	# dd = '%02d' % dt.day
	# input_hdfs_path = "/user/tdriscoll/cassandra/websitebuilder_v8/website/year=%d/month=%s/day=%s" % (yyyy, mm, dd)
	input_hdfs_path = "/user/tdriscoll/cassandra/websitebuilder_v8/website/year=%s/month=%s/day=%s" % ("2016", "08", "12")
	print input_hdfs_path
	df = hc.read.parquet(input_hdfs_path)
	dfparse = df.map(parseFeed).toDF()
	out = dfparse.take(1000)
	for o in out:
		print o
	
	dfparse.registerTempTable("source")
	hc.sql("use hdp_oc_team")
	hc.sql("DROP TABLE IF EXISTS WSBSparkTest")
	hc.sql("CREATE TABLE WSBSparkTest AS SELECT * FROM source")
	
	sc.stop()
	
if __name__ == '__main__':
	main()