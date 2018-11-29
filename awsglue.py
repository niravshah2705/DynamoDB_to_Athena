#encoding=utf8
#from jira import JIRA
import sys
import datetime
import os
import json
import boto3.dynamodb
import logging
import decimal
import time
import re

now = datetime.datetime.now()
dir ="/content/deployment/"+now.strftime("%Y%m%d")+"/"
#dir =now.strftime("%Y%m%d")+"/"


if not os.path.exists(dir):
	os.mkdir(dir,0770)

logging.basicConfig(
    format="%(asctime)s [%(levelname)-5.5s]  %(message)s",
    datefmt="%Y-%m-%d %H:%M"
)
logger = logging.getLogger("Glue_logger")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s [%(levelname)-5.5s]  %(message)s")

# For file write
c = logging.FileHandler(filename="debug_glue.log",mode='w+')
c.setLevel(logging.INFO)
c.setFormatter(formatter)
logger.addHandler(c)

TableName = sys.argv[1]
TableNameForGlue = TableName.lower().replace('.','_')
Daynamodatabase = "DDB"
Parquedatabase = "Athena"
JobRole = "DYNAMODBGLUEROLE"
Parqueprefix = "ddb-s3-athena-"
s3datalocation = 's3://ddb-with-athena/glue/ddb-output-'+TableNameForGlue+'/'
s3bucket='ddb-with-athena'
s3prefix='glue/ddb-output-'+TableNameForGlue+'/'
s3scriptlocation='s3://ddb-with-athena/niravshah2705@gmail.com/'
s3scriptname='Script.py'
awsprofile='dynamoglueprod'

executionSuccess = {}

boto3.setup_default_session(profile_name=awsprofile)	
dynamodb = boto3.resource('dynamodb')
ddt = dynamodb.Table(TableName)
if ddt.item_count > 0:
	logger.info("Starting Glue process")
	glue = boto3.client('glue')
		
	try:
		response = glue.delete_crawler( Name = TableNameForGlue )
		logger.info("Glue job removed:"+TableNameForGlue)
	except:
		logger.info("Glue crawler job not exists!")
	# Create Crawler Job
	# --------------------------

	response = glue.create_crawler( 
			Name = TableNameForGlue ,
			Role = JobRole ,
			DatabaseName = Daynamodatabase ,
			Description='string',
			Targets={
				'DynamoDBTargets': [
      					{
						'Path': TableName
					},
					]
				}
			)
	logger.info("Glue job created:"+TableNameForGlue)	
	# Run Crawler Job
	# -------------------
	response = glue.start_crawler( Name = TableNameForGlue )
	logger.info("Glue job started:"+TableNameForGlue)
	
	# Wait for Fail or success
	# -------------------
	response = glue.get_crawler( Name = TableNameForGlue )

	delay = 30
	while(response['Crawler']['State'] != 'READY'):
		response = glue.get_crawler( Name = TableNameForGlue )
		logger.info("Waiting for crawler to complete:"+str(delay)+" seconds")
		time.sleep(delay)
		delay += 10
	try:
		#print(response)
		if(response['Crawler']['LastCrawl']['Status'] == "SUCCEEDED"):
			logger.info("Crawler completed")
			response = glue.get_table( 
				DatabaseName= Daynamodatabase,
				Name=TableNameForGlue
				)
			print(response['Table']['StorageDescriptor']['Columns'])
			tablecolumns=response['Table']['StorageDescriptor']['Columns']
			fields = []
			mappings = []
			for c in tablecolumns:
				if re.match("^[a-zA-Z0-9]*$", c['Name']):
					#if(c['Type'] == "string"):
					#if len(fields) < 200:
					if c['Name'] not in fields:
						fields.append(c['Name'])
						mappings.append((c['Name'],c['Type'],c['Name'],c['Type']))
			print(fields)
			print(mappings)
			try:
				response = glue.delete_table( 
					DatabaseName=Parquedatabase,
					Name = Parqueprefix+TableNameForGlue
				)
				logger.info("Glue table removed:"+TableNameForGlue)
			except Exception as e:
				logger.info("Glue table not exists!")

			# Create new s3 table
			# ----------------
			response = glue.create_table(
				DatabaseName=Parquedatabase,
				TableInput={
					'Name': Parqueprefix+TableNameForGlue,
					'Retention': 0,
					'StorageDescriptor': {
						'Columns': tablecolumns,
						'Location': s3datalocation,
						'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
						'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
						'Compressed': False,
						'NumberOfBuckets': -1,
						'SerdeInfo': {
							'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
							'Parameters': {
								'serialization.format': '1'
							}
						},
						'SortColumns': [],
						'StoredAsSubDirectories': False
					},
					'TableType': 'EXTERNAL_TABLE',
					'Parameters': {
						'EXTERNAL': 'TRUE',
						'classification': 'parquet',
						'has_encrypted_data': 'false'
					}
				}	
			)
			#print(response)
			
			# Create Glue Job
			# --------------
			foundjob = 0
			response = glue.get_jobs()
			#print(response)
			for j in response['Jobs']:
				if j['Name'] == 'Dyanamodb_to_Athena_Glue_Job':
					foundjob=1	
			if foundjob == 0:
				response = glue.create_job(
					Name= 'Dyanamodb_to_Athena_Glue_Job',
					Role= JobRole,
					Command={
						'Name': 'glueetl',
						'ScriptLocation': s3scriptlocation+'/'+s3scriptname
					},
					DefaultArguments={
						'TempDir': s3scriptlocation
					}
				)
			else:
				logger.info("Job already exists")
			

			# Clean up s3
			# -----------
			s3 = boto3.resource('s3')
			bucket = s3.Bucket(s3bucket)
			#print('bucket allocated')
			objects_to_delete = []
			for obj in bucket.objects.filter(Prefix=s3prefix):
				objects_to_delete.append({'Key': obj.key})
			#print(objects_to_delete)
			if len(objects_to_delete) > 0:
				response = bucket.delete_objects(
					Delete={
						'Objects': objects_to_delete
					}
				)
				#print(response)
			# Start Glue JOb
			# -------------
			response = glue.start_job_run(
				JobName='Dyanamodb_to_Athena_Glue_Job',
				AllocatedCapacity=2,
				Arguments = {
					'--sourcedb': Daynamodatabase,
					'--sourcetable': TableNameForGlue,
                                        '--mapping': str(mappings),
					'--fields': str(fields),
					'--destinationdb':Parquedatabase,
					'--destinationtable': Parqueprefix+TableNameForGlue
				}
			)
			#print("---------------------------------------------")
			#print(str(mappings))
			#print("---------------------------------------------")
			#print(str(fields))
			#print("---------------------------------------------")
			response = glue.get_job_run(
				JobName='Dyanamodb_to_Athena_Glue_Job',
				RunId = response['JobRunId']
			)
			#print(response)
		else:
			logger.error("Crawler error")
			sys.exit(1)
	except Exception as e:
		print(e.message)
		print(e.args)
		logger.error("Crawler error.. exiting system")
		sys.exit(1)
			
else:
	logger.error("Cannot copy no data in table!!")

logger.debug(executionSuccess)
logger.info("-------------------------------------------")
