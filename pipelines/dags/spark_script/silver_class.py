import pyspark
from pyspark.sql.functions import explode_outer, ltrim

""" Create SilverLayer class to process data in the Silver layer. """
class SilverLayer:
    #init 
    def __init__(self, data: pyspark.sql.DataFrame, 
                 drop_columns: list = None, 
                 drop_null_columns: list = None,
                 fill_nulls_columns: dict = None,
                 duplicate_columns: list = None,
                 nested_columns: list = None,
                 rename_columns: dict = None,
                 ):
        
        #check valid params
        if data is not None and not isinstance(data, pyspark.sql.DataFrame):
            raise TypeError("data must be a DataFrame!")
        
        if drop_columns is not None and not isinstance(drop_columns, list):
            raise TypeError("drop_columns must be a list!")
        
        if drop_null_columns is not None and not isinstance(drop_null_columns, list):
            raise TypeError("drop_null_columns must be a list!")
        
        if fill_nulls_columns is not None and not isinstance(fill_nulls_columns, dict):
            raise TypeError("handle_nulls must be a dict!")
        
        if duplicate_columns is not None and not isinstance(duplicate_columns, list):
            raise TypeError("duplicate_columns must be a list!")
        
        if nested_columns is not None and not isinstance(nested_columns, list):
            raise TypeError("handle_nested must be a list!")
        
        if rename_columns is not None and not isinstance(rename_columns, dict):
            raise TypeError("rename_columns must be a dict!")
        """Initialize class attributes for data processing."""
        self._data = data
        self._drop_columns = drop_columns
        self._drop_null_columns = drop_null_columns
        self._fill_nulls_columns = fill_nulls_columns
        self._duplicate_columns = duplicate_columns
        self._nested_columns = nested_columns
        self._rename_columns = rename_columns


    """ Method to drop unnecessary columns. """
    def drop(self):
        self._data = self._data.drop(*self._drop_columns)

    
    """ Method to drop rows based on null values in each column. """
    def drop_null(self):
        self._data = self._data.dropna(subset = self._drop_null_columns, how = "all")

    
    """ Method to fill null values. """
    def fill_null(self):
        for column_list, value in self._fill_nulls_columns.items():
            self._data = self._data.fillna(value = value, subset = column_list)


    """ Method to rename columns. """
    def rename(self):
        for old_name, new_name in self._rename_columns.items():
            self._data = self._data.withColumnRenamed(old_name, new_name)


    """ Method to handle duplicates. """
    def handle_duplicate(self):
        self._data = self._data.dropDuplicates(self._duplicate_columns)


    """ Method to handle nested. """
    def handle_nested(self):
        for column in self._nested_columns:
            self._data = self._data.withColumn(column, explode_outer(column)) \
                                   .withColumn(column, ltrim(column))
    
    
    """ Main processing. """
    def process(self) -> pyspark.sql.DataFrame:
        #drop unnecessary columns
        if self._drop_columns:
            self.drop() 

        #drop rows contain null values for each col
        if self._drop_null_columns:
            self.drop_null()

        #fill null values
        if self._fill_nulls_columns:
            self.fill_null()
        
        #handle duplicate rows
        if self._duplicate_columns:
            self.handle_duplicate()

        #handle nested columns 
        if self._nested_columns:
            self.handle_nested()

        #rename columns
        if self._rename_columns:
            self.rename()

        return self._data