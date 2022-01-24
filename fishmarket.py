import boto3
import pandas as pd



class fishMarket():
    
    s3_client = boto3.client("s3")
    s3_resource = boto3.resource("s3")
    bucket_name = "data-eng-resources"
    Keys = []
    Data = []
    AverageData = []

    def KeysFromString(self,Start):
        Key_Array = []
        
        bucket_contents = self.s3_client.list_objects_v2(Bucket = self.bucket_name, Prefix = "python")
        
        for keys in bucket_contents["Contents"]:
            if keys["Key"].startswith(Start) == True: 
                Key_Array.append(keys["Key"])

        return Key_Array

    def DataFrames(self):
        Keys = self.Keys
        df_list = []
        
        for keys in Keys:
            s3_object = self.s3_client.get_object(Bucket = self.bucket_name, Key=keys)
            df_list.append(pd.read_csv(s3_object["Body"]))
            
        return(df_list)
        
    def AverageGroupData(self,Group):
        DataSet = self.Data
        AveragedData = []
        
        for dataframe in DataSet:
            AveragedData.append(dataframe.groupby(Group).mean())
        
        return AveragedData
    
    def DataUpload(self):
        for Set in self.AverageData:
            Set.to_csv("JacobF.csv")
            
        self.s3_client.upload_file(Filename="JacobF.csv",Bucket=self.bucket_name,Key="Data26/fish/JacobF.csv")




fish = fishMarket()

fish.Keys = fish.KeysFromString(Start="python/fish")
fish.Data = fish.DataFrames()
fish.AverageData = fish.AverageGroupData("Species")
fish.DataUpload()


print(fish.AverageData)
