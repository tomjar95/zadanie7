from flask import Flask, request, redirect, render_template, send_file, Response
from azure.storage.blob import BlobServiceClient
import os

app = Flask(__name__)

# Azure Blob Storage Configuration
CONNECTION_STRING = 'DefaultEndpointsProtocol=https;AccountName=zadanie7;AccountKey=WmM4209PrW5HpgSOd53schB1DRn+NSf1mNeze1A88HQ74ue83OMLVjMd//zSnyjTPrbTZxPXYB7m+AStsYsdPQ==;EndpointSuffix=core.windows.net'
CONTAINER_NAME = 'zadanie7'

blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

@app.route('/')
def index():
    blob_list = container_client.list_blobs()
    return render_template('index.html', blobs=blob_list)

@app.route('/upload', methods=['POST'])
def upload():
    file = request.files['file']
    blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=file.filename)
    blob_client.upload_blob(file, overwrite=True)
    return redirect('/')

@app.route('/download/<blob_name>')
def download(blob_name):
    blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_name)
    blob_data = blob_client.download_blob()
    return Response(blob_data.readall(), mimetype='application/octet-stream', headers={'Content-Disposition': f'attachment; filename={blob_name}'})

if __name__ == '__main__':
    app.run(debug=True)
