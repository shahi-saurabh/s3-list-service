from flask import Flask, jsonify, abort
import boto3
from botocore.exceptions import ClientError

app = Flask(__name__)

# Initialize S3 client
s3 = boto3.client('s3')

# S3 bucket name (replace with your actual bucket name)
BUCKET_NAME = 'one2n-s3'


def list_s3_objects(prefix=''):
    """Function to list objects in an S3 bucket for a given prefix"""
    try:
        response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix, Delimiter='/')
        if 'CommonPrefixes' in response:
            return [prefix['Prefix'].split('/')[-2] for prefix in response['CommonPrefixes']]
        return []
    except ClientError as e:
        return None


@app.route('/list-bucket-content', defaults={'path': ''}, methods=['GET'])
@app.route('/list-bucket-content/<path>', methods=['GET'])
def list_bucket_content(path):
    """Endpoint to list the content of the S3 bucket or subpath"""
    content = list_s3_objects(path)

    if content is None:
        abort(404, description="Bucket or path not found")

    return jsonify({"content": content})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)

