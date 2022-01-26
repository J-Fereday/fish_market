import boto3
import pandas as pd
#import pymongo



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
            if keys["Key"].startswith(Start) == True and keys["Key"].endswith(".csv"): 
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
    
    def DataUploadS3(self):
        #send data to csv file
        for Set in self.AverageData:
            Set.to_csv("JacobF.csv")
                    
        #send csv to file
        self.s3_client.upload_file(Filename="JacobF.csv",Bucket=self.bucket_name,Key="Data26/fish/JacobF.csv")
        
    def DataUploadEC2(self):
        # #open client in mongodb
        # client = pymongo.MongoClient("mongodb://35.158.210.57:27017/Sparta")
        # db = client.Sparta
        
        # #loop through data, convert to dictionary and insert into database
        # for Set in self.AverageData:
        #     db.collection.insert_many(Set.to_dict('records'))
        return None


def test():
    test = fishMarket()

    test.Keys = test.KeysFromString(Start="python/fish")
    if test.Keys == ['python/fish-market-mon.csv', 'python/fish-market-tues.csv', 'python/fish-market.csv']: print("Keys are correct") 
    else: print("Keys are incorrect")

    test.Data = test.DataFrames()
    if test.Data != []: print("Data is correct")
    else: print("Data is empty")

    test.AverageData = test.AverageGroupData("Species")
    if test.AverageData != []: print("Averaged data is correct")
    else: print("Averaged data is empty")
    
    test.DataUploadS3()
    test_object = test.s3_client.get_object(Bucket = test.bucket_name, Key="Data26/fish/JacobF.csv")
    if test_object is not None: print("Data upload complete")
    else: print("No Data in target")


test()
