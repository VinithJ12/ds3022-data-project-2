"""
DP2 Quote Assembler Flow
Steps:
1. POST request to the API
2. Poll SQS and fetch all messages (wait for delayed messages to become visible)
3. Assemble the phrase
4. Submit the phrase to the submission queue
"""

#importing necessary packages
from prefect import flow, task, get_run_logger
import requests
import boto3
import time

# Setup AWS SQS client
sqs = boto3.client("sqs", region_name="us-east-1")

# URLs ( which include the api and submission urls)
api_url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/uhe5bj"
submission_url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"


@task
def post_api():
    """
    We send a POST request to the API endpoint. The API responds with a JSON- url of SQS queue where the 21 delayed messages will appear
    """
    logger = get_run_logger()
    logger.info(f"Sending request to {api_url}")
    #POST request to trigger message

    response = requests.post(api_url)
    payload = response.json()
   
    #Extracting queue URL from API response
    sqs_url = payload["sqs_url"]
    logger.info(f"Received SQS URL: {sqs_url}")
    return sqs_url


@task
def fetch_all_messages(sqs_url, total_messages=21):
    """
    Monitor SQS queue and fetch all messages.
    Logs Available, NotVisible, Delayed, Total, and messages fetched so far.
    We periodically poll the SQS queue to fetch messages --> store it --> deletes each message after reading it (preventing duplications)
    """
    logger = get_run_logger()
    logger.info("Starting to monitor and fetch messages from queue...")

    messages = []
    checks = 0
    start_time = time.time()

    #This loop runs until we have 21 messages
    while len(messages) < total_messages:
        checks += 1
        try:
            # Get queue attributes
            attrs = sqs.get_queue_attributes(
                QueueUrl=sqs_url,
                AttributeNames=[
                    "ApproximateNumberOfMessages",
                    "ApproximateNumberOfMessagesNotVisible",
                    "ApproximateNumberOfMessagesDelayed"
                ],
            )["Attributes"]

            available = int(attrs["ApproximateNumberOfMessages"])
            not_visible = int(attrs["ApproximateNumberOfMessagesNotVisible"])
            delayed = int(attrs["ApproximateNumberOfMessagesDelayed"])
            total_in_queue = available + not_visible + delayed

            elapsed = round(time.time() - start_time, 1)
            logger.info(
                f"[Check #{checks}] After {elapsed}s → "
                f"Available={available}, NotVisible={not_visible}, Delayed={delayed}, "
                f"Total={total_in_queue}, Fetched={len(messages)}"
            )

            # Fetch messages if any are available
            if available > 0:
                response = sqs.receive_message(
                    QueueUrl=sqs_url,
                    MaxNumberOfMessages=10,
                    MessageAttributeNames=["All"],
                    WaitTimeSeconds=5,
                )

                if "Messages" in response:
                    for msg in response["Messages"]:
                        attrs_msg = msg["MessageAttributes"]
                        order_no = int(attrs_msg["order_no"]["StringValue"])
                        word = attrs_msg["word"]["StringValue"]
                        messages.append((order_no, word))

                        # Delete after reading
                        sqs.delete_message(
                            QueueUrl=sqs_url,
                            ReceiptHandle=msg["ReceiptHandle"]
                        )

            # Wait a few seconds before next check
            time.sleep(5)

        except Exception as e:
            logger.warning(f"Error monitoring/fetching messages: {e}")
            time.sleep(5)

    logger.info(f"All {total_messages} messages fetched successfully!")
    return messages

@task
def assemble_phrase(messages):
    logger = get_run_logger()
    try:
        #Sorting messages by their order number
        messages.sort(key=lambda x: x[0])
        # Extract the words and put them together
        phrase = " ".join([word for _, word in messages])
        logger.info(f"Final Assembled Phrase: {phrase}")
        return phrase # returning the completed, full phrase
    except Exception as e:
        logger.error(f"Error assembling phrase: {e}")
        raise


@task
def send_solution(uvaid, phrase, platform):
    logger = get_run_logger()
    if not phrase:
        raise ValueError("Phrase is empty, cannot submit!")

    try:
        #This submits the phrase we returned above into the submission SQS queue
        response = sqs.send_message(
            QueueUrl=submission_url, #This is the  submission queue
            MessageBody="DP2 SUBMISSION", # this is the message header
            MessageAttributes={ # these are my submission attributes ( my computing id and the message content)
                "uvaid": {"DataType": "String", "StringValue": uvaid},
                "phrase": {"DataType": "String", "StringValue": phrase},
                "platform": {"DataType": "String", "StringValue": platform},
            },
        )
        #This extracts the HTTP code - if we get 200 it's a success
        status = response["ResponseMetadata"]["HTTPStatusCode"]
        if status == 200:
            logger.info("Submission successful! (HTTP 200)")
        else:
            logger.warning(f"Submission returned HTTP {status}")
    except Exception as e:
        logger.error(f"Submission failed: {e}")
        raise


@flow(name="DP2 Quote Assembler")
def quote_assembler_flow():
    logger = get_run_logger()
    logger.info("DP2 Quote Assembler Starting...")

    #This is the first step ( sending the POST request to API)
    sqs_url = post_api()
    #This is the second step ( polling and monitoring the SQS queue to fetch messages as they become ready)
    messages = fetch_all_messages(sqs_url)
    #This is the third step ( Sorting and assembling  the message using their order number)
    phrase = assemble_phrase(messages)
    #Last step is to send to the submission queue
    send_solution("uhe5bj", phrase, "prefect")

    logger.info("Flow complete. All steps successful!")

#Runs prefect flow
if __name__ == "__main__":
    quote_assembler_flow()

