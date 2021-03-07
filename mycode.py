#S3driver.py
#code to create S3 bucket
import boto3
s3 = boto3.resource('s3',
 aws_access_key_id='REDACTED',
 aws_secret_access_key='REDACTED'
)
try:
 s3.create_bucket(Bucket='datacont-gravitte', CreateBucketConfiguration={
 'LocationConstraint': 'us-east-2'})
except:
 print ("this may already exist")
 
 bucket = s3.Bucket("datacont-gravitte")
 
 bucket.Acl().put(ACL='public-read')

 body = open('path-to-a-file\exp1', 'rb')
 o = s3.Object('datacont-name', 'test').put(Body=body )
 s3.Object('datacont-name', 'test').Acl().put(ACL='public-read')
 
 #dyno.py
 #create dynodb
 dyndb = boto3.resource('dynamodb',
	region_name='us-west-2',
	aws_access_key_id='REDACTED',
	aws_secret_access_key='REDACTED'
)
try:
	table = dyndb.create_table(
		TableName='DataTable',
		KeySchema=[
			{
				'AttributeName': 'PartitionKey',
				'KeyType': 'HASH'
			},
			{
				'AttributeName': 'RowKey',
				'KeyType': 'RANGE'
			}
		],
		AttributeDefinitions=[
			{
				'AttributeName': 'PartitionKey',
				'AttributeType': 'S'
			},
			{
				'AttributeName': 'RowKey',
				'AttributeType': 'S'
			},
		],
		ProvisionedThroughput={
			'ReadCapacityUnits': 5,
			'WriteCapacityUnits': 5
		}
	)
except:
	#if there is an exception, the table may already exist. if so...
	table = dyndb.Table("DataTable")
	
table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')

print(table.item_count)

#csvdriver.py
#add queries to S3, and query nosql table
with open('C:/Users/Maxwell/Desktop/data/experiments.csv', 'r') as csvfile:
	csvf = csv.reader(csvfile, delimiter=',',quotechar='|')
	for item in csvf:
		#print(item)
		body = open('C:/Users/Maxwell/Desktop/data/'+item[3], 'rb')
		s3.Object('datacont-gravitte', item[3]).put(Body=body )
		md = s3.Object('datacont-gravitte', item[3]).Acl().put(ACL='public-read')
		url = " https://s3-us-east-2.amazonaws.com/datacont-gravitte/"+item[3]
		metadata_item = {'PartitionKey': item[0], 'RowKey': item[1],
		'description' : item[4], 'date' : item[2], 'url':url}
		try:
			table.put_item(Item=metadata_item)
		except:
			print("item may already be there or another failure")

response = table.get_item(
	Key = {
		'PartitionKey': 'experiment1',
		'RowKey':'1'
	}
)

item = response['Item']
print(item)

