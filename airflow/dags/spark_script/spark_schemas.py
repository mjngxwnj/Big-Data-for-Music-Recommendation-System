from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DateType

""" Function for getting schemas. """
def get_schema(table_name: str) -> StructType:
    
    """ Artist_position schema. """
    artist_position_schema = [StructField('Pos',    StringType(), True),
                              StructField('Artist', StringType(), True)]
    #applying struct type
    artist_position_schema = StructType(artist_position_schema)  


    """ Artist schema. """
    artist_schema = [StructField('Artist_Id',    StringType(), True),
                     StructField('Artist_Name',  StringType(), True),
                     StructField('Genres',       ArrayType(StringType(), True), True),
                     StructField('Followers',    IntegerType(), True),
                     StructField('Popularity',   IntegerType(), True),
                     StructField('Artist_Image', StringType(), True),
                     StructField('Artist_Type',  StringType(), True),
                     StructField('External_Url', StringType(), True),
                     StructField('Href',         StringType(), True),
                     StructField('Artist_Uri',   StringType(), True)]
    #applying struct type
    artist_schema = StructType(artist_schema)


    """ Album schema. """
    album_schema = [StructField('Artist',               StringType(), True),
                    StructField('ID',                   StringType(), True),
                    StructField('Name',                 StringType(), True),
                    StructField('Type',                 StringType(), True),
                    StructField('Genres',               ArrayType(StringType(), True), True),
                    StructField('Label',                StringType(), True),
                    StructField('Popularity',           StringType(), True),
                    StructField('Available_Markets',    StringType(), True),
                    StructField('Release_Date',         DateType(), True),
                    StructField('ReleaseDatePrecision', StringType(), True),
                    StructField('TotalTracks',          IntegerType(), True),
                    StructField('Copyrights',           StringType(), True),
                    StructField('Restrictions',         StringType(), True),
                    StructField('External_URL',         StringType(), True),
                    StructField('Href',                 StringType(), True),
                    StructField('Image',                StringType(), True),
                    StructField('Uri',                  StringType(), True)]
    #Applying struct type
    album_schema = StructType(album_schema)


    """ Track schema. """
    track_schema = [StructField()]