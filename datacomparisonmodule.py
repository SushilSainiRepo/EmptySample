import pandas as pd
import findspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace 
from pyspark.sql.types import IntegerType, DecimalType

findspark.init()


class CompareFilesHandlePySpark:
    def __init__(self, basefolder, folderA, folderB):
        self.basefolder = basefolder
        self.metadatafilesfolder = f"{self.basefolder}/metadata/"
        self.datafilesfolderA = f"{self.basefolder}/{folderA}/"
        self.datafilesfolderB = f"{self.basefolder}/{folderB}/"
        self.baseurl = f"{self.basefolder}/results/"

    def runFilescomparison(self):
        header = True  # False;
        # print({self.metadatafilesfolder})
        Allfiles = pd.read_excel(
            f"{self.metadatafilesfolder}ValidationsConfig.xlsx", sheet_name="FilesList"
        )
        files = Allfiles[Allfiles["IsEnabled"] == True]
        Columns = pd.read_excel(
            f"{self.metadatafilesfolder}ValidationsConfig.xlsx", sheet_name="Columns"
        )
        FilesMatrix = []

        FilesMatrix.append([])  # COllection of Filenames
        FilesMatrix.append([])  # file identifier
        FilesMatrix.append([])  # file encoding
        FilesMatrix.append([])  # load failed from folder A
        FilesMatrix.append([])  # load failed from folder B
        print ("before")
        sc = SparkContext("local","test")
        spark = SparkSession(sc)
        print ("after")
        for label, content in files.items():
            if label == "fileidentifier":
                for item in content:
                    FilesMatrix[0].append(item)
            if label == "filename":
                for item in content:
                    FilesMatrix[1].append(item)
            if label == "Encoding":
                for item in content:
                    FilesMatrix[2].append(item)

        # len(FilesMatrix[6])
        ResultMatrix = []
        ResultMatrix.append([])
        ResultMatrix.append([])
        ResultMatrix.append([])
        ResultMatrix.append([])
        ResultMatrix.append([])
        ResultMatrix.append([])
       
        ResultDetailMatrix = []
        ResultDetailMatrix.append([])
        ResultDetailMatrix.append([])
        ResultDetailMatrix.append([])
        ResultDetailMatrix.append([])
        ResultDetailMatrix.append([])
     
        
        #Creates Empty RDD
        

        trackingdf = pd.DataFrame()  


        for j in range(0, len(FilesMatrix[1])):
            filename = FilesMatrix[1][j]
            fileIdentifier = FilesMatrix[0][j]
            #encoding = FilesMatrix[2][j]
            filesfound = False
            try:
                print(f"{self.datafilesfolderA}{filename}")
               
                sparkdfA = spark.read.option("inferSchema", "true").csv(
                    f"{self.datafilesfolderA}{filename}",
                    header=True,
                    encoding=f"utf8",
                )
                filesfound = True
            except Exception as e:
                print("File read exception : ", e)
                FilesMatrix[3].append(
                    f"{filename} file not found in {self.datafilesfolderA} with exception {e} "
                )
                continue

            try:
                # print( f"{self.datafilesfolderB}{filename}")

                sparkdfB = spark.read.option("inferSchema", "true").csv(
                    f"{self.datafilesfolderB}{filename}",
                    header=True,
                    encoding=f"utf8",
                )
                if filesfound:
                    filesfound = True

            except Exception as e:
                print("File read exception : ", e)
                FilesMatrix[4].append(
                    f"{filename} file not found in {self.datafilesfolderA} with exception {e}"
                )
                continue
            ValueColumns = Columns[
                (Columns["fileidentifier"] == fileIdentifier)
                & ((Columns["ValueColumn"] == True) | (Columns["PeriodColumn"] == True))
            ]

            # print(ValueColumns.count())
            # print(ValueColumns["ValueColumn"].count())
            # print(ValueColumns["PeriodColumn"].count())
            ResultMatrix[0].append(filename)
            if (
                filesfound
                and ValueColumns["ValueColumn"].count() > 0
                and ValueColumns["PeriodColumn"].count() > 0
            ):
                # print(filename, ', count of rows in file A' , sparkdfA.count(), 'Count rows in File B', sparkdfB.count() )
                ValueColumnsForFile = ValueColumns[
                    (ValueColumns["ValueColumn"] == True)
                ]
                GroupByColumnsForFile = ValueColumns[
                    (ValueColumns["PeriodColumn"] == True)
                ]
                grpColumns = GroupByColumnsForFile["column"].values[0]
                valuecolumn = ValueColumnsForFile["column"].values[0]

                maxPeriodA = sparkdfA.agg({grpColumns: "max"}).first()[0]
                maxPeriodB = sparkdfB.agg({grpColumns: "max"}).first()[0]

                sparkdfA = (
                    sparkdfA.withColumn(
                        grpColumns, (col(grpColumns)).cast(IntegerType())
                    )
                    .withColumn(valuecolumn, regexp_replace(valuecolumn, ",", ""))
                    .withColumn(valuecolumn, (col(valuecolumn)).cast(DecimalType()))
                    .orderBy([f"{grpColumns}"], ascending=[True])
                )
                sparkdfB = (
                    sparkdfB.withColumn(
                        grpColumns, (col(grpColumns)).cast(IntegerType())
                    )
                    .withColumn(valuecolumn, regexp_replace(valuecolumn, ",", "."))
                    .withColumn(valuecolumn, (col(valuecolumn)).cast(DecimalType()))
                    .orderBy([f"{grpColumns}"], ascending=[True])
                )

                grpsparkdfA = sparkdfA.groupBy(f"{grpColumns}").agg(
                    {valuecolumn: "sum"}
                )
                grpsparkdfB = sparkdfB.groupBy(f"{grpColumns}").agg(
                    {valuecolumn: "sum"}
                )

                grpsparkdfA.createOrReplaceTempView(f"{fileIdentifier}A")
                grpsparkdfB.createOrReplaceTempView(f"{fileIdentifier}B")

                alldiffFound = spark.sql(
                    f"select t1.{grpColumns} Period, t1.`sum({valuecolumn})`-t2.`sum({valuecolumn})` as Values, t1.`sum({valuecolumn})` as FolderA_Value, t2.`sum({valuecolumn})` as FolderB_Value  from {fileIdentifier}A t1, {fileIdentifier}B t2 where t2.{grpColumns} == t1.{grpColumns};"
                )

                diffFound = alldiffFound.filter("Values> 10000 or Values< -10000")


                # print('No of Periods with diff' ,diffFound.count())
                totaldiff = alldiffFound.agg({"Values": "sum"}).first()[0]

                additionalperiods = spark.sql(
                    f"select {grpColumns} Period, sum(`sum({valuecolumn})`) Values from {fileIdentifier}B where {grpColumns} >{maxPeriodA} group by {grpColumns}"
                )

                # pdalldiffFound= sparkdfA.toPandas()
                # pd2alldiffFound= grpsparkdfA.toPandas()
                # with pd.ExcelWriter(f"{self.baseurl}PySpark1.xlsx") as writer:
                #       pdalldiffFound.to_excel(
                #             writer, sheet_name="DataResults", index=False, header=header,
                # )
                # with pd.ExcelWriter(f"{self.baseurl}PySpark2.xlsx") as writer:
                #       pd2alldiffFound.to_excel(
                #             writer, sheet_name="DataResults2", index=False, header=header,
                # )

                if diffFound.count() > 0:
                    ResultMatrix[1].append(diffFound.collect())
                    ls =  diffFound.collect()
                    for items in ls:
                        ResultDetailMatrix[0].append(fileIdentifier)
                        ResultDetailMatrix[1].append(items[0])
                        ResultDetailMatrix[2].append(int(items[1]))
                        ResultDetailMatrix[3].append(int(items[2]))
                        ResultDetailMatrix[4].append(int(items[3]))
                else:
                    ResultMatrix[1].append("-")

                if totaldiff != 0:
                    ResultMatrix[2].append(totaldiff)
                else:
                    ResultMatrix[2].append("-")

                if additionalperiods.count() > 0:
                    ResultMatrix[3].append(additionalperiods.collect())
                else:
                    ResultMatrix[3].append("-")

                ResultMatrix[4].append(maxPeriodA)
                ResultMatrix[5].append(maxPeriodB)
            else:
                if (
                    filesfound
                    and ValueColumns["ValueColumn"].count() <= 0
                    and ValueColumns["PeriodColumn"].count() <= 0
                ):
                    ResultMatrix[1].append(
                        "No column specified to group by and compare"
                    )

                elif (
                    filesfound
                    and ValueColumns["PeriodColumn"].count() > 0
                    and ValueColumns["ValueColumn"].count() <= 0
                ):
                    ResultMatrix[1].append("No Value column specified")
                elif not filesfound:
                    ResultMatrix[1].append("Files unable to  load in dataframe")
                else:
                    ResultMatrix[1].append("unknow error")

                ResultMatrix[2].append("-")
                ResultMatrix[3].append("-")
                ResultMatrix[4].append("-")
                ResultMatrix[5].append("-")

    

        print("All files processed")

        dataframeoutput = pd.DataFrame(
            {
                "File name": ResultMatrix[0],
                "differences >10000": ResultMatrix[1],
                "Total Difference": ResultMatrix[2],
                "Additional periods in B": ResultMatrix[3],
                "Latest periods A": ResultMatrix[4],
                "Latest periods B": ResultMatrix[5],
            }
        )
        print("Write Output")
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    
        dataframeoutput2 = pd.DataFrame(
            {
            "File": ResultDetailMatrix[0],
            "Period": ResultDetailMatrix[1],
            "Differnce in Value": ResultDetailMatrix[2],
            "Value in folder A": ResultDetailMatrix[3],
            "Value in folder B": ResultDetailMatrix[4]
            }

        )
        

        with pd.ExcelWriter(f"{self.baseurl}comparisonResultsPySpark.xlsx") as writer:
            dataframeoutput.to_excel(
                writer,
                sheet_name="DataComparisonResults",
                index=False,
                header=header,
            )
            dataframeoutput2.to_excel(
                writer,
                sheet_name="DetailedResults",
                index=False,
                header=header,
            )
        
        
        # for hive table in databricks
        # sparkdataframeoutput=spark.createDataFrame(dataframeoutput)
        # sparkdataframeoutput
        
        print("Write Output Completed Successfully")
