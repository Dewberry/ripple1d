import json
import os
import re

import boto3
import boto3.session


def list_keys(s3_client: boto3.Session.client, bucket: str, prefix: str, suffix=""):
    keys = []
    kwargs = {"Bucket": bucket, "Prefix": prefix}
    while True:
        resp = s3_client.list_objects_v2(**kwargs)
        keys += [obj["Key"] for obj in resp["Contents"] if obj["Key"].endswith(suffix)]
        try:
            kwargs["ContinuationToken"] = resp["NextContinuationToken"]
        except KeyError:
            break
    return keys


def list_keys_regex(s3_client: boto3.Session.client, bucket: str, prefix_includes: str, suffix=""):
    keys = []
    kwargs = {"Bucket": bucket, "Prefix": prefix_includes}
    prefix_pattern = re.compile(prefix_includes.replace("*", ".*"))
    while True:
        resp = s3_client.list_objects_v2(**kwargs)
        keys += [
            obj["Key"] for obj in resp["Contents"] if prefix_pattern.match(obj["Key"]) and obj["Key"].endswith(suffix)
        ]
        try:
            kwargs["ContinuationToken"] = resp["NextContinuationToken"]
        except KeyError:
            break
    return keys


def init_s3_resources():
    # Instantitate S3 resources
    session = boto3.Session(
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    )

    s3_client = session.client("s3")
    s3_resource = session.resource("s3")
    return session, s3_client, s3_resource


def check_s3_key_exists(bucket: str, key: str):
    s3 = boto3.client("s3")
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception as e:
        return False


def read_json_from_s3(bucket: str, key: str):
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=bucket, Key=key)
    file_content = response["Body"].read().decode("utf-8")
    json_content = json.loads(file_content)
    return json_content
