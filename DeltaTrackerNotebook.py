# %% [markdown]
# 
# ###### Change storage account connection and run next cell to set initial variable
# 
# 
# ###### example of location on server 
# 
# `serverloca =archive/2023-11-29/85888437-7754-46f0-86d4-64b7397596fe/`
# `serverlocb =archive/FY24 P3 InputFiles/ `
# 
# ###### example of location on server 
# `storageaccount = "SharedAccessSignature=<SASToken>;BlobEndpoint=https://<server>.blob.core.windows.net/;FileEndpoint=https://<server>.file.core.windows.net/;"`
# `  storageaccount = "DefaultEndpointsProtocol=https;AccountName=<Accountname>;AccountKey=<Account key>==;BlobEndpoint=https://<Accountname>.blob.core.windows.net/;FileEndpoint=https://<Accountname>.file.core.windows.net/;"`
# 

# %% [markdown]
# 

# %%

storageaccount = "" 
blobcontainer = "files"
serverloca = 'archive1'
serverlocb = 'archive2'


localbasefolder ="Files"
localfolderA = "202401" 
localfolderB = "202403"



# %% [markdown]
# ###### Download files to local folder in next cell
# 
# > run next cell 

# %%
from filedeltatrackingmodules.downloadfilesmodule import getInputsFiles
import os
### Initiate instance of class getInputFiles to pass parameters localbasefolder, blobcontainer, connectionstring: ###

if os.path.exists(f"{localbasefolder}/{localfolderA}")== False:
          os.makedirs(f"{localbasefolder}/{localfolderA}")


if os.path.exists(f"{localbasefolder}/{localfolderB}")== False:
          os.makedirs(f"{localbasefolder}/{localfolderB}")

gi = getInputsFiles(localbasefolder=localbasefolder, blobcontainer=blobcontainer, connectionstring= storageaccount )

### download files to local folder location from storage account: ###
gi.downloadFilesinFolder(blobfolder=serverloca, localfolder=localfolderA)

print (str(gi.countfiles ) , " files downloaded")

gi2 = getInputsFiles(localbasefolder=localbasefolder, blobcontainer=blobcontainer, connectionstring= storageaccount )
gi2.downloadFilesinFolder(blobfolder=serverlocb, localfolder=localfolderB)

print (str(gi2.countfiles ) , " files downloaded")

# %% [markdown]
# ###### once files are downloaded to localfolderA and localfolderB, ensure following 
# 
# 1. Ensure config  file is in the Metadata folder and all metrics needed to compare are enabled
# 
# 2. if you amend any code in the filedeltatrackingmodule.py , remember to restart the kernel shown in the bar above
# 
# 3. use Clear all Output option above before checking back your changes in this file
# 
# 4. Run next cell
# 
# 

# %%
import time

from filedeltatrackingmodules.datacomparisonmodule import CompareFilesHandlePySpark 


CompareFileOps = CompareFilesHandlePySpark( localbasefolder, localfolderA, localfolderB)
# Validations.datafilesfolder=f'demoDynamicValidation/metadata/'
# Validations.metadatafilesfolder=f'demoDynamicValidation/files/'
start_time = time.time()
FilesMatrix = CompareFileOps.runFilescomparison()
End_time = time.time()
print("Time taken: ",End_time - start_time)


# %% [markdown]
# 
# > Clean any files downloaded files and purge
# 
# > Clear all outputs in the notebook
# 
# > Checkin changes to code or discard by undo git changes if not needed
# 

# %%
import os
import shutil
if os.path.exists(f"{localbasefolder}/{localfolderA}"):
        shutil.rmtree(f'{localbasefolder}/{localfolderA}')
if os.path.exists(f"{localbasefolder}/{localfolderB}"):
        shutil.rmtree(f'{localbasefolder}/{localfolderB}')


