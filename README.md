[![Open in Visual Studio Code](https://classroom.github.com/assets/open-in-vscode-f059dc9a6f8d3a56e377f745f24479a46679e63a5d9fe6f495e02850cd0d8118.svg)](https://classroom.github.com/online_ide?assignment_repo_id=6801763&assignment_repo_type=AssignmentRepo)

Spark Project
Spring 2022

Code authors
-----------
Simran Goindani |
Swarnima Deshmukh |
Pranjali Agarwal 


Installation
------------
These components are installed:
- JDK 1.8
- Scala 2.12.10
- Hadoop 3.2.2
- Spark 3.1.2 (without bundled Hadoop)
- Maven
- AWS CLI (for EMR execution)

Environment
-----------
1) Example ~/.bash_aliases:
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/home/swarnima/hadoop-3.2.2
export SCALA_HOME=/home/swarnima/scala/2.12.10
export SPARK_HOME=/home/swarnima/spark-3.1.2-bin-without-hadoop
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SCALA_HOME/bin:$SPARK_HOME/bin
export SPARK_DIST_CLASSPATH=$(hadoop classpath)

2) Explicitly set JAVA_HOME in $HADOOP_HOME/etc/hadoop/hadoop-env.sh:
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64


Execution
---------
All of the build & execution commands are organized in the Makefile.
1) Unzip project file.
2) Open command prompt.
3) Navigate to directory where project files unzipped.
4) Edit the Makefile to customize the environment at the top.
	Sufficient for standalone: hadoop.root, jar.name, local.input
	Other defaults acceptable for running standalone.

a) For V-H Matrix:
i. For executing on local machine:
	Change the job.name = vh.VH_Matrix_Main
	Change the app.name=Matrix Multiplication
	local.input1=input1
	local.input2=input2
	local: jar clean-local-output
	spark-submit --class ${job.name} --master ${local.master} --name "${app.name}" ${jar.name} ${local.input1} ${local.input2}  ${local.output}

	make switch-standalone		-- set standalone Hadoop environment (execute once)
	make local
ii. For executing on AWS
	Set the appropriate num nodes and instance types
	aws.input1=input1
	aws.input2=input2
	aws: jar upload-app-aws delete-output-aws
	aws emr create-cluster \
		--name "MATMUL_final Spark Cluster" \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop Name=Spark \
		--steps Type=CUSTOM_JAR,Name="${app.name}",Jar="command-runner.jar",ActionOnFailure=TERMINATE_CLUSTER,Args=["spark-submit","--deploy-mode","cluster","--class","${job.name}","s3://${aws.bucket.name}/${jar.name}","s3://${aws.bucket.name}/${aws.input1}","s3://${aws.bucket.name}/${aws.input2}","s3://${aws.bucket.name}/${aws.output}"] \
		--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
		--use-default-roles \
		--enable-debugging \
		--auto-terminate
	make upload-input-aws		-- only before first execution
	make aws			-- check for successful execution with web interface (aws.amazon.com)
	make download-output-aws	-- after successful execution & termination

b) For Spark MLlib: 
Decision Tree
i. For executing on local machine:
	Change the job.name = wc.DecisionTree
	make switch-standalone		-- set standalone Hadoop environment (execute once)
	make local
ii. For executing on AWS
	Set the appropriate num nodes and instance types
	make upload-input-aws		-- only before first execution
	make aws					-- check for successful execution with web interface (aws.amazon.com)
	make download-output-aws	-- after successful execution & termination

Random Forest
i. For executing on local machine:
	Change the job.name = wc.RandomForestBagging
	make switch-standalone		-- set standalone Hadoop environment (execute once)
	make local
ii. For executing on AWS
	Set the appropriate num nodes and instance types
	make upload-input-aws		-- only before first execution
	make aws					-- check for successful execution with web interface (aws.amazon.com)
	make download-output-aws	-- after successful execution & termination

GBT
i. For executing on local machine:
	Change the job.name = wc.GBT
	make switch-standalone		-- set standalone Hadoop environment (execute once)
	make local
ii. For executing on AWS
	Set the appropriate num nodes and instance types
	make upload-input-aws		-- only before first execution
	make aws					-- check for successful execution with web interface (aws.amazon.com)
	make download-output-aws	-- after successful execution & termination


c) For PySpark:
	Set the appropriate num nodes and instance types
	make upload-input-aws		-- only before first execution
	make pyspark-aws -- to run the python application on AWS EMR. 
	make download-output-aws -- after successful execution & termination
