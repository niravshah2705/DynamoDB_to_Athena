import sys
import ast
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','sourcedb','destinationdb','sourcetable','destinationtable','mapping','fields'])
 
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "aws-blogs-glue-database098234ytb2", table_name = "auto_billplatformcredentials", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = args['sourcedb'], table_name = args['sourcetable'], transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("cgimageurl", "string", "cgimageurl", "string"), ("id", "string", "id", "string"), ("billname", "string", "billname", "string"), ("billserverurl", "string", "billserverurl", "string"), ("billapiusername", "string", "billapiusername", "string"), ("billcurrency", "string", "billcurrency", "string"), ("billappname", "string", "billappname", "string"), ("billapipassword", "string", "billapipassword", "string"), ("billdescription", "string", "billdescription", "string"), ("billverifykey", "string", "billverifykey", "string"), ("billchannel", "string", "billchannel", "string"), ("billmerchantid", "string", "billmerchantid", "string"), ("billcountry", "string", "billcountry", "string"), ("billsubsamounttype", "string", "billsubsamounttype", "string"), ("billsubsenableretry", "string", "billsubsenableretry", "string"), ("billsubsppionly", "string", "billsubsppionly", "string"), ("billwebsiteweb", "string", "billwebsiteweb", "string"), ("billchannelweb", "string", "billchannelweb", "string"), ("billgeneratechecksumurl", "string", "billgeneratechecksumurl", "string"), ("billverifychecksumurl", "string", "billverifychecksumurl", "string"), ("billmerchantkey", "string", "billmerchantkey", "string"), ("buildtype", "string", "buildtype", "string"), ("billrequesttype", "string", "billrequesttype", "string"), ("billsubsfrequencyunit", "string", "billsubsfrequencyunit", "string"), ("billtheme", "string", "billtheme", "string"), ("billsubsfrequency", "string", "billsubsfrequency", "string"), ("billchannelwap", "string", "billchannelwap", "string"), ("billwebsitewap", "string", "billwebsitewap", "string"), ("billindustrytypeid", "string", "billindustrytypeid", "string"), ("billserverurlsandbox", "string", "billserverurlsandbox", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = ast.literal_eval(args['mapping']), transformation_ctx = "applymapping1")
## @type: SelectFields
## @args: [paths = ["cgimageurl", "id", "billname", "billserverurl", "billapiusername", "billcurrency", "billappname", "billapipassword", "billdescription", "billverifykey", "billchannel", "billmerchantid", "billcountry", "billsubsamounttype", "billsubsenableretry", "billsubsppionly", "billwebsiteweb", "billchannelweb", "billgeneratechecksumurl", "billverifychecksumurl", "billmerchantkey", "buildtype", "billrequesttype", "billsubsfrequencyunit", "billtheme", "billsubsfrequency", "billchannelwap", "billwebsitewap", "billindustrytypeid", "billserverurlsandbox"], transformation_ctx = "selectfields2"]
## @return: selectfields2
## @inputs: [frame = applymapping1]
selectfields2 = SelectFields.apply(frame = applymapping1, paths = ast.literal_eval(args['fields']), transformation_ctx = "selectfields2")
## @type: ResolveChoice
## @args: [choice = "MATCH_CATALOG", database = "aws-blogs-glue-database098234ytb2", table_name = "ddb-target-s3-table-auto-billplatformcredentials", transformation_ctx = "resolvechoice3"]
## @return: resolvechoice3
## @inputs: [frame = selectfields2]
resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = args['destinationdb'], table_name = args['destinationtable'], transformation_ctx = "resolvechoice3")
## @type: ResolveChoice
## @args: [choice = "make_struct", transformation_ctx = "resolvechoice4"]
## @return: resolvechoice4
## @inputs: [frame = resolvechoice3]
resolvechoice4 = ResolveChoice.apply(frame = resolvechoice3, choice = "make_struct", transformation_ctx = "resolvechoice4")
## @type: DataSink
## @args: [database = "aws-blogs-glue-database098234ytb2", table_name = "ddb-target-s3-table-auto-billplatformcredentials", transformation_ctx = "datasink5"]
## @return: datasink5
## @inputs: [frame = resolvechoice4]
datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = resolvechoice4, database = args['destinationdb'], table_name = args['destinationtable'] , transformation_ctx = "datasink5")
job.commit()
