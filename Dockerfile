FROM alpine
  
ENV pre_build_base /opt

RUN mkdir $pre_build_base/apache-spark
COPY ./.dist/local-repo/org/apache/spark/*/*/spark-distribution*.tgz $pre_build_base/apache-spark/
RUN rm $pre_build_base/apache-spark/spark-distribution*yarn-archive.tgz; cd $pre_build_base/apache-spark; tar -xzf spark-distribution*.tgz; rm $pre_build_base/apache-spark/spark-distribution*.tgz; mv $pre_build_base/apache-spark/spark-*-bin-*/* $pre_build_base/apache-spark

WORKDIR $pre_build_base
