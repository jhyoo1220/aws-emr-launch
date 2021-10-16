import boto3
from botocore.exceptions import WaiterError
from datetime import datetime
from typing import List
import sys


# We use r5.xlarge for master and r5.4xlarge for cores (https://aws.amazon.com/ko/ec2/instance-types/)
V_CPU = 16
RAM_IN_GB = 128

MASTER_INSTANCE_TYPES = ["r5.xlarge", "r5d.xlarge", "r5a.xlarge"]
CORE_INSTANCE_TYPES = ["r5d.4xlarge", "r5.4xlarge", "r5a.4xlarge"]


# We use emr-6.3.1
EMR_VERSION = "emr-6.3.1"


# To connect the other infrastructue, we must specify subnets and sgs of EC2 instances
SUBNETS = []
MASTER_SG = ""
SLAVE_SG = ""

# To connect with SSH, we should provide Ec2KeyName
EC2_KEY_NAME = ""

# To debug with logs, we should provide LogUri
LOG_URI = ""


class Emr:
    @staticmethod
    def _get_spark_configurations(v_cpu: int, ram_in_gb: int, num_nodes: int) -> dict:
        # Reference: https://aws.amazon.com/ko/blogs/big-data/best-practices-for-successfully-managing-memory-for-apache-spark-applications-on-amazon-emr/
        # spark.executors.cores = 5 (vCPU)
        num_cores = 5

        # Number of executors per instance = (total number of virtual cores per instance - 1)/ spark.executors.cores
        # Number of executors per instance = ({v_cpu} - 1) / 5 (rounded down)
        num_executors_per_node = int((v_cpu - 1) / num_cores)

        # Total executor memory = total RAM per instance / number of executors per instance
        # Leave 1 GB for the Hadoop daemons.
        # Total executor memory = {ram_in_gb} / {num_executors_per_node} (rounded down)
        total_memory = int(ram_in_gb / num_executors_per_node)

        # spark.executors.memory = total executor memory * 0.90
        # spark.executors.memory = {total_memory} * 0.9 (rounded down)
        memory = int(total_memory * 0.9)

        # spark.yarn.executor.memoryOverhead = total executor memory * 0.10
        # spark.yarn.executor.memoryOverhead = {total_memory} - {memory}
        memory_overhead = total_memory - memory

        # spark.driver.memory = spark.executors.memory
        # We will use spark.executors.memory for spark.driver.memory

        # spark.driver.cores = spark.executors.cores
        # We will use spark.executors.cores for spark.driver.cores

        # spark.executor.instances = (number of executors per instance * number of core instances) minus 1 for the driver
        # spark.executor.instances = 3 * {num_nodes} - 1
        num_total_executors = num_executors_per_node * num_nodes - 1

        # spark.default.parallelism = spark.executor.instances * spark.executors.cores * 2
        # spark.default.parallelism = {num_total_executors} * 5 * 2
        parallelism = num_total_executors * num_cores * 2

        return {
            'num_cores': num_cores,
            'num_executors_per_node': num_executors_per_node,
            'memory': f"{memory}g",
            'memory_overhead': f"{memory_overhead}g",
            'num_total_executors': num_total_executors,
            'parallelism': parallelism,
        }

    @staticmethod
    def _get_spark_instance_configurations(spark_conf: dict) -> List[dict]:
        return [
            {
                'Classification': "hive-site",
                'Properties': {
                    # Reference: https://aws.amazon.com/ko/blogs/big-data/best-practices-for-successfully-managing-memory-for-apache-spark-applications-on-amazon-emr/
                    'hive.metastore.client.factory.class': "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
                    'hive.optimize.skewjoin': "true",
                    'hive.optimize.skewjoin.compiletime': "true",
                    'hive.optimize.union.remove': "true",
                    'hive.exec.compress.output': "true",
                    'hive.exec.compress.intermediate': "true",
                    'hive.optimize.index.filter': "true",
                    'hive.optimize.index.groupby': "true",
                    'hive.exec.parallel': "true",
                    'hive.merge.mapredfiles': "true",
                    'hive.optimize.correlation': "true",
                    'hive.exec.orc.zerocopy': "true",
                    'hive.vectorized.execution.enabled': "true",
                    'hive.vectorized.execution.mapjoin.native.multikey.only.enabled': "true",
                    'hive.vectorized.execution.mapjoin.minmax.enabled': "true",
                    'hive.vectorized.use.vector.serde.deserialize': "true",
                    'hive.vectorized.use.row.serde.deserialize': "true",
                    'hive.spark.dynamic.partition.pruning': "true",
                    'hive.spark.optimize.shuffle.serde': "true",
                    'hive.exec.dynamic.partition': "true",
                    'hive.exec.dynamic.partition.mode': "nonstrict",
                    'fs.s3a.experimental.fadvise': "random",
                    'spark.sql.parquet.mergeSchema': "false",
                },
                'Configurations': [],
            },
            # Reference: https://aws.amazon.com/ko/blogs/big-data/best-practices-for-successfully-managing-memory-for-apache-spark-applications-on-amazon-emr/
            {
                'Classification': "mapred-site",
                'Properties': {
                    'mapreduce.map.output.compress': "true",
                },
                'Configurations': [],
            },
            {
                'Classification': "spark-hive-site",
                'Properties': {
                    'hive.metastore.client.factory.class': "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
                },
                'Configurations': [],
            },
            {
                'Classification': "yarn-site",
                'Properties': {
                    'yarn.nodemanager.vmem-check-enabled': "false",
                    'yarn.nodemanager.pmem-check-enabled': "false",
                    'yarn.resourcemanager.am.max-attempts': "1",
                    'yarn.scheduler.capacity.resource-calculator': "org.apache.hadoop.yarn.util.resource.DominantResourceCalculator",
                    'yarn.resourcemanager.scheduler.class': "org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler",
                },
                'Configurations': [],
            },
            {
                'Classification': "spark-defaults",
                'Properties': {
                    'spark.network.timeout': "600s",
                    'spark.executor.heartbeatInterval': "60s",
                    'spark.dynamicAllocation.enabled': "false",
                    'spark.driver.memory': spark_conf['memory'],
                    'spark.executor.memory': spark_conf['memory'],
                    'spark.driver.cores': str(spark_conf['num_cores']),
                    'spark.executor.cores': str(spark_conf['num_cores']),
                    'spark.executor.instances': str(spark_conf['num_total_executors']),
                    'spark.driver.memoryOverhead': spark_conf['memory_overhead'],
                    'spark.executor.memoryOverhead': spark_conf['memory_overhead'],
                    'spark.memory.fraction': "0.80",
                    'spark.memory.storageFraction': "0.30",
                    'spark.driver.extraJavaOptions': "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:OnOutOfMemoryError='kill -9 %p'",
                    'spark.executor.extraJavaOptions': "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:OnOutOfMemoryError='kill -9 %p'",
                    'spark.yarn.scheduler.reporterThread.maxFailures': "5",
                    'spark.storage.level': "MEMORY_AND_DISK_SER",
                    'spark.rdd.compress': "true",
                    'spark.shuffle.compress': "true",
                    'spark.shuffle.spill.compress': "true",
                    'spark.default.parallelism': str(spark_conf['parallelism']),
                    'spark.sql.sources.partitionOverwriteMode': "dynamic",
                    'spark.serializer': "org.apache.spark.serializer.KryoSerializer",
                    # TODO: Need performance test of zstd format
                    'spark.io.compression.codec': "zstd",
                    'spark.io.compression.zstd.level': "3",
                    'spark.checkpoint.compress': "true",
                    'spark.sql.orc.enabled': "true",
                    'spark.sql.hive.convertMetastoreOrc': "true",
                    'spark.sql.orc.filterPushdown': "true",
                    'spark.sql.orc.char.enabled': "true",
                    'spark.hadoop.fs.s3a.experimental.input.policy': "random",
                    'spark.hadoop.fs.s3a.impl': "org.apache.hadoop.fs.s3a.S3AFileSystem",
                    'spark.sql.parquet.filterPushdown': "true",
                    'spark.hadoop.parquet.enable.summary-metadata': "false",
                    'spark.sql.hive.metastorePartitionPruning': "true",
                    # https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/performance.html#For_large_data_uploads.2C_tune_the_block_size:_fs.s3a.block.size
                    'spark.hadoop.fs.s3a.block.size': "500M",
                    'spark.shuffle.service.enabled': "true",
                    'spark.hadoop.fs.s3a.experimental.input.fadvise': "random",
                    'spark.hadoop.fs.s3a.committer.name': "directory",
                },
                'Configurations': [],
            },
            # Reference: https://aws.amazon.com/ko/blogs/big-data/best-practices-for-successfully-managing-memory-for-apache-spark-applications-on-amazon-emr/
            {
                'Classification': "spark",
                'Properties': {
                    'maximizeResourceAllocation': "false",
                },
                'Configurations': [],
            },
        ]

    @staticmethod
    def _get_ebs_configuration() -> dict:
        return {
            'EbsBlockDeviceConfigs': [
                {
                    'VolumeSpecification': {
                        'VolumeType': "gp2",
                        'SizeInGB': 50,
                    },
                    'VolumesPerInstance': 1,
                },
            ],
            'EbsOptimized': True,
        }

    @staticmethod
    def _get_applications() -> list:
        return [
            {'Name': "hadoop"},
            {'Name': "hive"},
            {'Name': "spark"},
        ]

    @classmethod
    def _get_instance_fleet_configurations(cls, instance_types: list, instance_fleet_type: str, num_nodes: int, use_spot: bool) -> dict:
        spark_configurations = cls._get_spark_configurations(V_CPU, RAM_IN_GB, num_nodes)

        instance_type_configs = []
        for instance_type in instance_types:
            curr_conf = {
                'InstanceType': instance_type,
                'WeightedCapacity': 1,
                'Configurations': cls._get_spark_instance_configurations(spark_configurations),
                'EbsConfiguration': cls._get_ebs_configuration(),
            }
            instance_type_configs.append(curr_conf)

        fleet_config = {
            'Name': instance_fleet_type,
            'InstanceFleetType': instance_fleet_type,
            'InstanceTypeConfigs': instance_type_configs,
        }

        if use_spot:
            fleet_config['TargetSpotCapacity'] = num_nodes
        else:
            fleet_config['TargetOnDemandCapacity'] = num_nodes

        return fleet_config

    @classmethod
    def create_and_wait(cls, region: str, name_prefix: str, num_nodes: int, use_spot: bool):
        master_instance_fleet = cls._get_instance_fleet_configurations(MASTER_INSTANCE_TYPES, "MASTER", 1, use_spot)
        core_instance_fleet = cls._get_instance_fleet_configurations(CORE_INSTANCE_TYPES, "CORE", num_nodes, use_spot)

        instances = {
            'InstanceFleets': [master_instance_fleet, core_instance_fleet],
            'KeepJobFlowAliveWhenNoSteps': True,
            # 'Ec2KeyName': EC2_KEY_NAME,
            # 'Ec2SubnetIds': SUBNETS,
            # 'EmrManagedMasterSecurityGroup': MASTER_SG,
            # 'EmrManagedSlaveSecurityGroup': SLAVE_SG,
        }

        cluster_name = f"{name_prefix}-{datetime.now().strftime('%Y%m%d%H%M%S')}"
        instance_tags = [
            {
                'Key': "Name",
                'Value': f"EMR-{cluster_name}",
            },
        ]

        emr_client = boto3.client("emr", region_name=region)
        response = emr_client.run_job_flow(
            Name=cluster_name,
            # LogUri=LOG_URI,
            ReleaseLabel=EMR_VERSION,
            Instances=instances,
            EbsRootVolumeSize=20,
            Applications=cls._get_applications(),
            VisibleToAllUsers=True,
            ServiceRole="EMR_DefaultRole",
            JobFlowRole="EMR_EC2_DefaultRole",
            Tags=instance_tags,
        )

        cluster_id = response['JobFlowId']
        print(f"EMR cluster is being created with {cluster_id}...")
        waiter = emr_client.get_waiter("cluster_running")

        try:
            waiter.wait(ClusterId=cluster_id)

        except WaiterError:
            status = emr_client.describe_cluster(ClusterId=cluster_id)['Cluster']['Status']
            state = status['State']
            state_change_reason = status['StateChangeReason']

            raise Exception(f"Something is wrong during creating cluster: {state} with {state_change_reason}")

        print(f"{cluster_id} is successfully created.")

if __name__ == "__main__":
    region = sys.argv[1]
    name_prefix = sys.argv[2]
    use_spot = sys.argv[3].lower() == "true"
    num_nodes = int(sys.argv[4]) if len(sys.argv) > 4 else 1

    Emr.create_and_wait(region, name_prefix, num_nodes, use_spot)
