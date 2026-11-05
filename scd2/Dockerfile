FROM apache/spark:3.5.3

USER root

# Hadoop S3A
ADD https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/
ADD https://repo.maven.apache.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/

# Jupyter Notebook
RUN pip3 install --no-cache-dir \
    --default-timeout=120 \
    --retries=10 \
    notebook==7.2.2 \
    ipykernel==6.29.5

RUN mkdir -p /home/jupyter/work

ENV SPARK_HOME=/opt/spark
ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.7-src.zip

WORKDIR /home/jupyter/work
WORKDIR /home/jupyter/work

EXPOSE 8888

CMD ["jupyter", "notebook", \
     "--ip=0.0.0.0", \
     "--port=8888", \
     "--no-browser", \
     "--allow-root", \
     "--NotebookApp.token="]