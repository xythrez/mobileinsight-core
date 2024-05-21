'''Functions used to partition MobileInsight data

All functions have the signiture DataFrame -> GroupedData.

Do NOT remove any existing columns from the dataframe!
'''

def file_path(dataframe):
    return dataframe.groupby('file_path')
