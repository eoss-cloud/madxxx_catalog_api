import boto3
import ujson

MAX_MESSAGES = 10


class Queue(object):
    def __init__(self):
        self.sqs = boto3.resource('sqs')
        self.queue_name = 'EOSS_notifications'

    def __len__(self):
        queue = self.sqs.get_queue_by_name(QueueName=self.queue_name)
        return int(queue.attributes.get('ApproximateNumberOfMessages'))


    def __create_queue__(self):
        # Create the queue. This returns an SQS.Queue instance
        queue = self.sqs.create_queue(QueueName=self.queue_name, Attributes={'DelaySeconds': '5'})

        # You can now access identifiers and attributes
        print(queue.url)
        print(queue.attributes.get('DelaySeconds'))

    def send(self, body):
        queue = self.sqs.get_queue_by_name(QueueName=self.queue_name)
        response = queue.send_message(MessageBody=ujson.dumps(body))


    def get_messages(self):
        # Get the queue
        queue = self.sqs.get_queue_by_name(QueueName=self.queue_name)

        # Process messages by printing out body and optional author name
        for message in queue.receive_messages(MaxNumberOfMessages=MAX_MESSAGES, WaitTimeSeconds=10, VisibilityTimeout=60,):
            yield message

    def delete_message(self, message):
        queue = self.sqs.get_queue_by_name(QueueName=self.queue_name)
        queue.delete_messages(Entries=message)


if __name__ == '__main__':
    q = Queue()
    print q.get_messages()