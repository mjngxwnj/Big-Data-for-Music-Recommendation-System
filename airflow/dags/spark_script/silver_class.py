import pyspark

class SilverLayer:
    #init 
    def __init__(self, data: pyspark.sql.DataFrame, 
                 drop_columns: list = None, 
                 rename_columns: dict[list] = None,
                 ):
