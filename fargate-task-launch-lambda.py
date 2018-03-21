import boto3
def handler(event,context):
  client = boto3.client('ecs')


  for record in event['Records']:
    bucket = record['s3']['bucket']['name']
    key = record['s3']['object']['key']

  cmd = "python3 dataload.py --host flight-db.xxxxxxx.us-east-1.rds.amazonaws.com --port 5432 --user xxxxx --pass xxxxx --database flight --bucket %s --filename %s" % (bucket,key)
  response = client.run_task(
  cluster='default',
  launchType = 'FARGATE',
  taskDefinition='flight-dataload-taskdefinition:1',
  count = 1,
  platformVersion='LATEST',
  networkConfiguration={
        'awsvpcConfiguration': {
            'subnets': [
                'subnet-1f081f7b', # replace with your public subnet or a private with NAT
                'subnet-3d1cbb32' # Second is optional, but good idea to have two
            ],
            'assignPublicIp': 'ENABLED'
        }
    },
  overrides={
        'containerOverrides': [
            {
                'name': 'flight-dataset-load',
                'command': [
                    "sh", "-c", cmd
                ],
                'environment': [
                    {
                        'name': 'AWS_ACCESS_KEY_ID',
                        'value': 'xxxxxxxxxxxxxxxxxx'
                    },
                    {
                        'name': 'AWS_SECRET_ACCESS_KEY',
                        'value': 'xxxxxxxxxxxxxxxxxxx'
                    }
                ]
            }
        ]
    }
    )
  return str(response)
