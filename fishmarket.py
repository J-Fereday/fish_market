import boto3
import pandas as pd



class fishMarket():
    
    #class variables
    s3_client = boto3.client("s3")
    s3_resource = boto3.resource("s3")
    bucket_name = "data-eng-resources"
    Keys = []
    Data = []
    AverageData = []

    def KeysFromString(self,Start):
        Key_Array = []
        
        #get contents of bucket
        bucket_contents = self.s3_client.list_objects_v2(Bucket = self.bucket_name, Prefix = "python")
        
        #get keys from items that start with a particular string
        for keys in bucket_contents["Contents"]:
            if keys["Key"].startswith(Start) == True: 
                Key_Array.append(keys["Key"])

        return Key_Array

    def DataFrames(self):
        df_list = []
        
        #for every key, download data and add to a list
        for keys in self.Keys:
            s3_object = self.s3_client.get_object(Bucket = self.bucket_name, Key=keys)
            df_list.append(pd.read_csv(s3_object["Body"]))
            
        return(df_list)
        
    def AverageGroupData(self,Group):
        AveragedData = []
        
        #get averaged data in dataset
        for dataframe in self.Data:
            AveragedData.append(dataframe.groupby(Group).mean())
        
        return AveragedData
    
    def DataUpload(self):
        #send data to csv file
        for Set in self.AverageData:
            Set.to_csv("JacobF.csv")
        
        #send csv to file
        self.s3_client.upload_file(Filename="JacobF.csv",Bucket=self.bucket_name,Key="Data26/fish/JacobF.csv")




fish = fishMarket()

fish.Keys = fish.KeysFromString(Start="python/fish")
fish.Data = fish.DataFrames()
fish.AverageData = fish.AverageGroupData("Species")
fish.DataUpload()
