import os
from pyspark.sql import SparkSession


def create_spark_session(name):
    jars = '/opt/airflow/s3-jars/'
    endpoint = os.environ['S3_ENDPOINT']
    spark = (SparkSession.builder.appName(name)
            .master(os.environ.get('SPARK_MASTER', 'local[2]'))
            .config('spark.sql.session.timeZone', 'UTC')
            .config('spark.ui.enabled', 'false')
            .config('spark.jars', jars + 'hadoop-aws-3.3.4.jar,' + jars + 'aws-java-sdk-bundle-1.12.262.jar')
            .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
            .config('spark.hadoop.fs.s3a.endpoint', endpoint)
            .config('spark.hadoop.fs.s3a.endpoint.region', 'us-east-1')
            .config('spark.hadoop.fs.s3a.path.style.access', 'true')
            .config('spark.hadoop.fs.s3a.connection.ssl.enabled', str(endpoint.startswith('https://')).lower())
            .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'com.amazonaws.auth.EnvironmentVariableCredentialsProvider')
            .getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    return spark
