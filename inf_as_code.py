import re
import pandas as pd
import boto3
import json
import configparser
import boto3
import time
from botocore.exceptions import ClientError
from pandas.core.frame import DataFrame

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

# there parameters are coming from dwh.cfg as we define them to use create our cluster.

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config.get("DWH","DWH_DB")
DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = config.get("DWH","DWH_PORT")

DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

 # To able to run infrastructure code we need clients for required services like ec2,s3,iam and redshift. We are using boto for creating client.

# creating ec2 client
ec2 = boto3.resource('ec2',
                    region_name="us-west-2",
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET
                    )
# creating s3 client
s3 = boto3.resource('s3',
                    region_name="us-west-2",
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET
                )
# creating iam client
iam = boto3.client('iam',aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET,
                    region_name='us-west-2'
                )
# creating redshift client
redshift = boto3.client('redshift',
                    region_name="us-west-2",
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET
                    )


def creating_infrastructure_as_code():
    """
    
    This function works with accepting no parameter.
    We are going to create our infrastructura as code by this func.

    """

    (DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)

    pd.DataFrame({"Param":
                    ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
                "Value":
                    [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
                })

   

    # creating the role, Create an IAM Role that makes Redshift able to access S3 bucket (ReadOnly)

    try:
        print("Creating a new IAM Role") 
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                'Effect': 'Allow',
                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                'Version': '2012-10-17'})
        )    
    except Exception as e:
        print(e)
        

    print("Attaching Policy")

    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                        )['ResponseMetadata']['HTTPStatusCode']

    print("1.3 Get the IAM role ARN")
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

    print(roleArn)


    # creating redshift cluster with this block. Retreive required information from dwh.cfg file under header DWH
    try:
        response = redshift.create_cluster(        
            # DHW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            #Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            
            #Roles (for s3 access)
            IamRoles=[roleArn]  
        )
    except Exception as e:
        print(e)

    result=pd.DataFrame

    # Describe the cluster to see its status ,  we can check our  cluster status with this block
    def prettyRedshiftProps(props):
        pd.set_option('display.max_colwidth',None)
        keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
        x = [(k, v) for k,v in props.items() if k in keysToShow]
        return pd.DataFrame(data=x, columns=["Key", "Value"])

    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    
    # result=pd.DataFrame

    result=prettyRedshiftProps(myClusterProps)
    C_status=result.loc[result['Key']=='ClusterStatus','Value'].values[0]
    print(C_status)


    def get_cluster_prop():
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        return myClusterProps

    def check_status_redshift():
        prop=get_cluster_prop()
        result=prettyRedshiftProps(prop)
        C_status=result.loc[result['Key']=='ClusterStatus','Value'].values[0]
        
        if C_status !='available':
            print('Redshift Cluster Status: ',C_status,'. We are waing it to be Ready. . .')
            time.sleep(20)
            prop=get_cluster_prop()
            check_status_redshift()
        if C_status == 'available':
            print('Redshift Cluster is available')

    check_status_redshift()



    # Open an incoming TCP port to access the cluster ednpoint
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)

def cleanse_infrastructure():
    """

    This fucntion accepts no parameter.
    
    We are using this function to clean our resources of AWS.

    dont forget to uncomment  line # inf_clean() at "etl.py" for cleaning procces if you need to clean resources.

    """
    # Clean up your resources

    # deletes the created redshift cluster.
    redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)

    # deletes the created role for the redshift cluster.
    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)


