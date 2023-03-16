import os
import boto3
import botocore
import json
import uuid
import logging
import tempfile
from io import BytesIO
import io
import email
import os
import trp
import pypdfium2 as pdfium
import ctypes
import time
import traceback
from bs4 import BeautifulSoup
import re
import email
import quopri, base64
import requests
import re
from chalice import Chalice
import sys

from PIL import Image

app = Chalice(app_name="_convertToText")

# Setup logging for debugging purposes
app.log.setLevel(logging.DEBUG)

# Load the environment variables
DPI = 300
if "DPI" in os.environ:
    try:
        DPI = int(os.environ["DPI"])
    except Exception as e:
        app.log.debug(
            f"Couldn't process DPI environment variable: {str(e)}.  Using the default: DPI=300"
        )
else:
    app.log.info(f"No DPI environment variable set.  Using the default: DPI=300")

FMT = "jpeg"
if "FMT" in os.environ:
    try:
        FMT = str(os.environ["FMT"])
    except Exception as e:
        app.log.debug(
            f"Couldn't process FMT environment variable: {str(e)}.  Using the default: FMT=jpeg"
        )
else:
    app.log.info(f"No FMT environment variable set.  Using the default: FMT=jpeg")

S3_BUCKET = ""
if "S3_BUCKET" in os.environ:
    S3_BUCKET = str(os.environ["S3_BUCKET"])
    app.log.info(f"Setting the S3 bucket: {S3_BUCKET}")
else:
    app.log.debug(f"Couldn't process the S3_BUCKET environment variable. ")

KEY = ""
if "KEY" in os.environ:
    KEY = str(os.environ["KEY"])
    app.log.info(f"Setting the KEY: {KEY}")
else:
    app.log.debug(f"Couldn't process the KEY environment variable. ")

REGION = ""
if "REGION" in os.environ:
    REGION = str(os.environ["REGION"])
    app.log.info(f"Setting the region: {REGION}. ")
else:
    app.log.debug(f"Couldn't process the REGION environment variable. ")

EMAILFROM = ""
if "EMAILFROM" in os.environ:
    EMAILFROM = str(os.environ["EMAILFROM"])
    app.log.info(f"Setting the EMAILFROM: {EMAILFROM}. ")
else:
    app.log.debug(f"Couldn't process the EMAILFROM environment variable. ")

EMAILTO = ""
if "EMAILTO" in os.environ:
    EMAILTO = str(os.environ["EMAILTO"])
    app.log.info(f"Setting the EMAILTO: {EMAILTO}. ")
else:
    app.log.debug(f"Couldn't process the EMAILTO environment variable. ")

def get_all_block(message, block_type = "text/plain"):
    content_type = message.get_content_type()
    main_type = message.get_content_maintype()
    if main_type == "multipart":
        if message.is_multipart():
            block = None
            for part in message.get_payload():
                result = get_all_block(part, block_type)
                if result:
                    if block is None:
                        block = result
                    else:
                        block += result
            return block
        else:
            return None
    elif content_type == block_type:
        result = message.get_payload(decode=True)
        if result is not None:
            charsets = message.get_charsets()
            print('charsets', charsets, result)
        return result
    else:
        return None

def start_job(client, s3_bucket_name, object_name):
    response = None
    response = client.start_document_text_detection(
        DocumentLocation={
            'S3Object': {
                'Bucket': s3_bucket_name,
                'Name': object_name
            }})

    return response["JobId"]

def is_job_complete(client, job_id):
    time.sleep(1)
    response = client.get_document_text_detection(JobId=job_id)
    status = response["JobStatus"]
    print("Job status: {}".format(status))

    while(status == "IN_PROGRESS"):
        time.sleep(1)
        response = client.get_document_text_detection(JobId=job_id)
        status = response["JobStatus"]
        print("Job status: {}".format(status))

    return status


def get_job_results(client, job_id):
    pages = []
    time.sleep(1)
    response = client.get_document_text_detection(JobId=job_id)
    pages.append(response)
    print("Resultset page received: {}".format(len(pages)))
    next_token = None
    if 'NextToken' in response:
        next_token = response['NextToken']

    while next_token:
        time.sleep(1)
        response = client.\
            get_document_text_detection(JobId=job_id, NextToken=next_token)
        pages.append(response)
        print("Resultset page received: {}".format(len(pages)))
        next_token = None
        if 'NextToken' in response:
            next_token = response['NextToken']

    return pages

def send_email(ses_client, to, subject, body):
    try:
        response = ses_client.send_email(
            Destination={
                'ToAddresses': [
                    to,
                ],
            },
            Message={
                'Body': {
                    'Text': {
                        'Charset': 'UTF-8',
                        'Data': body,
                    },
                },
                'Subject': {
                    'Charset': 'UTF-8',
                    'Data': subject,
                },
            },
            Source=EMAILFROM,
        )
    except Exception as e:
        print(e)
        return False

    return True

@app.lambda_function()
def convertPdfToText(event, context):

    s3 = boto3.resource("s3")
    s3client = boto3.client('s3')
    textractclient = boto3.client('textract', region_name=REGION)
    ses_client = boto3.client('ses')
    app.log.info("initialized clients")

    # local tmp file
    pdf_file = '/tmp/file.pdf'
    bucket = ''
    key = ''
    app.log.info("Checking event for bucket")
    if event.get('Records',None) != None:
        if event['Records'][0]['s3'] != None:
            bucket = event['Records'][0]['s3']['bucket']['name']
            key = event['Records'][0]['s3']['object']['key']  
        
            
            objectData = s3client.get_object(Bucket=bucket, Key=key)
            emailContent = objectData['Body'].read().decode("utf-8")
            
            message = email.message_from_string(emailContent)
            try:
                #only check first attachment (which can be insufficient for HTML emails)
                attachment = message.get_payload()[1]
                app.log.info(attachment.get_content_type())
                
                if attachment.get_content_type() != 'application/pdf':
                    app.log.info("no attachment - check for links")
                    
                    messagetxt = email.message_from_string(emailContent)
                    text = get_all_block(messagetxt, "text/plain")
                    urls = re.findall(r'(https?://\S+)', text.decode('utf8', 'strict'))
                    
                    # always take first link, second is Amazon customer service link
                    url=urls[0]
                    url = url[:-1]
                    app.log.info("Found the following link: " + str(url))
                    r = requests.get(url, stream=True)
                    with open(pdf_file, 'wb') as f:
                        f.write(r.content)
                else:
                    app.log.info("got attachment")
                    # Write the attachment to a temporary location
                    open(pdf_file, 'wb').write(attachment.get_payload(decode=True))

                # Upload the file at the temporary location to destination s3 bucket
                try:
                    object_name= 'pdf/note.pdf'
                    s3client.upload_file(pdf_file, bucket, object_name)
                except:
                    app.log.error("An exception occurred during the PDF S3 upload")
            except:
                app.log.error("An exception occurred during the PDF extraction")

        # Start converting PDF into JPEG Images
        page_num = 0
        result = ''
        if os.path.isfile(pdf_file):
            app.log.info("Converting PDF to images!")        
            pdf = pdfium.PdfDocument(pdf_file)
            version = pdf.get_version()  # get the PDF standard version
            page_num = len(pdf)  # get the number of pages in the document
            app.log.info("PDF has version " + str(version) + " and " + str(page_num) + "pages")
            y = 0
            #workaround since pdf.render_to uses parallel processing with queue which is not supported by Lambda
            while y < page_num:
                page_indices = [y]  # one page at a time
                renderer = pdf.render_to(
                    pdfium.BitmapConv.pil_image,
                    page_indices = page_indices,
                    scale = 300/72,  # 300dpi resolution
                )
                i = 0
                for i, image in zip(page_indices, renderer):
                    try:
                        DIR_ID = uuid.uuid4()
                        image_key_name = "/tmp/{0}_{1}{2}".format(DIR_ID, str(i), "." + FMT)
                        image.save(image_key_name)
                        image.close()
                        app.log.info("Image "+ image_key_name + " saved successfully.") 

                        #upload to S3 for processing via Textract
                        object_jpg_name = "img/{0}_{1}{2}".format(DIR_ID, str(i), "." + FMT)
                        output_path = 'text'

                        s3client.upload_file(image_key_name, bucket, object_jpg_name)
                        app.log.info("Image "+ image_key_name + " uploaded successfully to " + object_jpg_name)

                        job_id = start_job(textractclient, bucket, object_jpg_name)
                        app.log.info("Started job with id: {}".format(job_id))
                        if is_job_complete(textractclient, job_id):
                            response = get_job_results(textractclient, job_id)

                        # Append detected text
                        for result_page in response:
                            for item in result_page["Blocks"]:
                                if item["BlockType"] == "LINE":
                                    app.log.info('\033[94m' + item["Text"] + '\033[0m')
                                    result = result + "\n" + item["Text"]
                        
                    except:
                        app.log.info("PDF to image convertion failed")
                y = y +1

            app.log.info("send notes via email")
            send_email(ses_client, EMAILTO, 'meeting notes', str(result))

            # Prepare the JSON with uploaded images.
            app.log.info("images converted successfully!")
        else:
            result = "no file to convert"
            

    return {
        "Content-Type": "application/json",
        'statusCode': 200,
        'body': str(result)
    }