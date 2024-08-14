import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# node album 
album_node1723611178142 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-spotify-staging/Staging/albums.csv"], "recurse": True}, transformation_ctx="album_node1723611178142")

# node track
track_node1723611180423 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-spotify-staging/Staging/track.csv"], "recurse": True}, transformation_ctx="track_node1723611180423")

# node artist
artist_node1723611179705 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-spotify-staging/Staging/artists.csv"], "recurse": True}, transformation_ctx="artist_node1723611179705")

# Join by track
RenamedkeysforJoinbytrack_node1723611686683 = ApplyMapping.apply(frame=track_node1723611180423, mappings=[("track_id", "string", "right_track_id", "string"), ("track_popularity", "string", "right_track_popularity", "string")], transformation_ctx="RenamedkeysforJoinbytrack_node1723611686683")

# album & artist join
album_node1723611178142DF = album_node1723611178142.toDF()
artist_node1723611179705DF = artist_node1723611179705.toDF()
albumartistjoin_node1723611273116 = DynamicFrame.fromDF(album_node1723611178142DF.join(artist_node1723611179705DF, (album_node1723611178142DF['artist_id'] == artist_node1723611179705DF['id']), "left"), glueContext, "albumartistjoin_node1723611273116")

# Join by track
albumartistjoin_node1723611273116DF = albumartistjoin_node1723611273116.toDF()
RenamedkeysforJoinbytrack_node1723611686683DF = RenamedkeysforJoinbytrack_node1723611686683.toDF()
Joinbytrack_node1723611335058 = DynamicFrame.fromDF(albumartistjoin_node1723611273116DF.join(RenamedkeysforJoinbytrack_node1723611686683DF, (albumartistjoin_node1723611273116DF['track_id'] == RenamedkeysforJoinbytrack_node1723611686683DF['right_track_id']), "left"), glueContext, "Joinbytrack_node1723611335058")

# Target Amazon S3 DataWarehouse
AmazonS3_node1723612514883 = glueContext.write_dynamic_frame.from_options(frame=Joinbytrack_node1723611335058, connection_type="s3", format="glueparquet", connection_options={"path": "s3://project-spotify-staging/Data Warehouse/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1723612514883")

job.commit()