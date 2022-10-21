import boto3

tamanho = 100
sqs = boto3.client('sqs')

for i in range(100):
  sqs.send_message(
      QueueUrl=QUEUE_URL,
      DelaySeconds=10,
      MessageBody=(
          f'{i} - Rafael'
      )
  )

