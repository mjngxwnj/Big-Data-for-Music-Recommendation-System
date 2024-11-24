from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DateType, FloatType

""" Function for getting schemas. """
def get_schema(table_name: str) -> StructType:
    """ Artist schema. """
    artist_schema = [StructField('Artist_ID',      StringType(), True),
                     StructField('Artist_Name',    StringType(), True),
                     StructField('Genres',         ArrayType(StringType(), True), True),
                     StructField('Followers',      IntegerType(), True),
                     StructField('Popularity',     IntegerType(), True),
                     StructField('Artist_Image',   StringType(), True),
                     StructField('Artist_Type',    StringType(), True),
                     StructField('External_Url',   StringType(), True),
                     StructField('Href',           StringType(), True),
                     StructField('Artist_Uri',     StringType(), True),
                     StructField('Execution_date', DateType(), True)]
    #applying struct type
    artist_schema = StructType(artist_schema)
    
    """ Album schema. """
    album_schema = [StructField('Artist',               StringType(), True),
                    StructField('Artist_ID',            StringType(), True),
                    StructField('Album_ID',             StringType(), True),
                    StructField('Name',                 StringType(), True),
                    StructField('Type',                 StringType(), True),
                    StructField('Genres',               ArrayType(StringType(), True), True),
                    StructField('Label',                StringType(), True),
                    StructField('Popularity',           IntegerType(), True),
                    StructField('Available_Markets',    StringType(), True),
                    StructField('Release_Date',         DateType(), True),
                    StructField('ReleaseDatePrecision', StringType(), True),
                    StructField('TotalTracks',          IntegerType(), True),
                    StructField('Copyrights',           StringType(), True),
                    StructField('Restrictions',         StringType(), True),
                    StructField('External_URL',         StringType(), True),
                    StructField('Href',                 StringType(), True),
                    StructField('Image',                StringType(), True),
                    StructField('Uri',                  StringType(), True),
                    StructField('Execution_date',       DateType(), True)]
    #Applying struct type
    album_schema = StructType(album_schema)

    """ Track schema. """
    track_schema = [StructField("Artists",          StringType(), True),
                    StructField("Album_ID",         StringType(), True),
                    StructField("Album_Name",       StringType(), True),
                    StructField("Track_ID",         StringType(), True),
                    StructField("Name",             StringType(), True),
                    StructField("Track_Number",     IntegerType(), True),
                    StructField("Type",             StringType(), True),
                    StructField("AvailableMarkets", StringType(), True),
                    StructField("Disc_Number",      IntegerType(), True),
                    StructField("Duration_ms",      IntegerType(), True),
                    StructField("Explicit",         StringType(), True),
                    StructField("External_urls",    StringType(), True),
                    StructField("Href",             StringType(), True),
                    StructField("Restrictions",     StringType(), True),
                    StructField("Preview_url",      StringType(), True),
                    StructField("Uri",              StringType(), True),
                    StructField("Is_Local",         StringType(), True),
                    StructField('Execution_date',   StringType(), True)]
    #Applying struct type
    track_schema = StructType(track_schema)
    
    """ TrackFeature schema. """
    trackfeature_schema = [StructField("Track_ID",         StringType(), True),
                           StructField("Danceability",     FloatType(), True),
                           StructField("Energy",           FloatType(), True),
                           StructField("Key",              IntegerType(), True),
                           StructField("Loudness",         FloatType(), True),
                           StructField("Mode",             IntegerType(), True),
                           StructField("Speechiness",      FloatType(), True),
                           StructField("Acousticness",     FloatType(), True),
                           StructField("Instrumentalness", FloatType(), True),
                           StructField("Liveness",         FloatType(), True),
                           StructField("Valence",          FloatType(), True),
                           StructField("Tempo",            FloatType(), True),
                           StructField("Time_signature",   IntegerType(), True),
                           StructField("Track_href",       StringType(), True),
                           StructField("Type_Feature",     StringType(), True),
                           StructField("Analysis_Url",     StringType(), True),
                           StructField('Execution_date',   StringType(), True)]
    #Applying struct type
    trackfeature_schema = StructType(trackfeature_schema)

    #mapping
    mapping = {
        'artist': artist_schema,
        'album': album_schema,
        'track': track_schema,
        'trackfeature': trackfeature_schema
    }
    
    #return schema
    return mapping[table_name]