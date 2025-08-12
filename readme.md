
>Use Ctrl+Shift+V to open preview


  
### File Delta Tracker

This project aims to track difference in any periods in the manual cost input files which finance teams can produces every month and has risk of change in postings for historical months. These manual files can be exported from systems like Dynamics or Oracle ebusiness Suite etc

## Whats included in FileDeltaTracker project

 - FileDeltaTrackingModules folder has custom code in python scripts for modular approach and simplified use in notebooks
 - Notebook which has cells. Individual cells can be run as per need. It allows user to run program to downlaod files or comapre data in files

> A notebook  [deltatrackingnotebook.ipynb](./deltatrackingnotebook.ipynb)  consumes above modules and can be run in conda environment updated with packages given in requirements.txt

## Pre-requisite

- Python 
          Go to [Download Python | Python.org](https://www.python.org/downloads/) and download latest version of python

- MiniConda is minimum requirement which then added with the packages needed in requirement.txt 

> [Miniconda documentation](https://docs.conda.io/projects/miniconda/en/latest/)


## Getting started
 - check out repo and open folder in visual studio code

 - Open terminal and Create a conda environment with name filedeltatrackerenv using following 
> use one of commands to create a new environment with name in conda environment folder

          `conda create -n filedeltatrackerenv python=3.11 azure-storage-blob openpyxl ipykernel pyspark findspark -c conda-forge`
         
OR         
> use one of commands to create a new environment in local folder
         
          `conda create --prefix .conda/filedeltatrackerenv python=3.11 azure-storage-blob openpyxl ipykernel pyspark findspark -c conda-forge`

- Activate Conda Enviornment
use on of two commands
>        `conda activate filedeltatrackerenv`

or 

>         'conda activate C:\FileDeltaTracker\.conda\filedeltatrackerenv'                  

add excel file with ValidationsConfix.xlsx
Tab: FilesList
Table Columns: fileidentifier	column	position	type	AllowNulls	length	unique	ValueColumn	PeriodColumn
Tab: Columns
Table Columns: fileidentifier	filename	Encoding	IsEnabled	Warehouse	Country	Accounts


Open Notebook [deltatrackingnotebook](/deltatrackingnotebook.py) and select the new environment in top right corner 



Run each cell

## Finally check outcomes 

read output in [Results](Files/results/) using excel






