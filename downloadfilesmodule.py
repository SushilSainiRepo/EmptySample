import sys
import os
from azure.storage.blob import BlobServiceClient, ContainerClient


class getInputsFiles:
    def __init__(self, localbasefolder, blobcontainer, connectionstring):
        self.localbasefolder = localbasefolder
        self.connectionstring = connectionstring
        self.blobcontainer = blobcontainer
        self.countfiles =0

    def downloadFilesinFolder(self, localfolder, blobfolder):
        blobservice = BlobServiceClient.from_connection_string(
            conn_str=self.connectionstring
        )
        

        container = blobservice.get_container_client(container=self.blobcontainer)
        blob_list = container.list_blobs(name_starts_with=blobfolder)
        for blob in blob_list:
            try:
                head, tail = os.path.split(blob["name"])

                path = f"{self.localbasefolder}/{localfolder}/{tail}"

                with open(path, "wb") as my_blob:
                    blob_data = container.download_blob(blob.name).readall()
                    my_blob.write(blob_data)
                self.countfiles=self.countfiles+1
            except:
                print(blob["name"], " couldn't be downloaded to ", path)
                continue
