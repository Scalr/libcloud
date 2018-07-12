import base64

from libcloud.common.aws import SignedAWSConnection, AWSJsonResponse
from libcloud.container.base import ContainerDriver
from libcloud.container.types import ClusterState

__all__ = [
    'ElasticKubernetesDriver'
]

EKS_VERSION = '2017-11-01'
EKS_HOST = 'eks.%s.amazonaws.com'


class EKSCluster(object):
    """EKS cluster."""

    states_map = {
        'ACTIVE': ClusterState.ACTIVE,
        'CREATING': ClusterState.CREATING,
        'DELETING': ClusterState.DELETING
    }

    def __init__(self, driver, cluster_data):
        self._raw_data = cluster_data
        self.driver = driver
        self.name = cluster_data['name']
        self.endpoint = cluster_data['endpoint']
        self.arn = cluster_data['arn']
        self.created_at = cluster_data['createdAt']
        self.version = cluster_data['version']
        self.role_arn = cluster_data['roleArn']
        self.status = self.states_map.get(cluster_data['status'])
        self.resources_vpc_config = cluster_data['resourcesVpcConfig']

        cert = base64.b64decode(cluster_data['certificateAuthority']['data'].encode()).decode()
        self.cluster_certificate = cert

    def __repr__(self):
        return '<EKSCluster: name={}, endpoint={}, version={}, status={}>'.format(
            self.name, self.endpoint, self.version, self.status
        )

class EKSJsonConnection(SignedAWSConnection):
    version = EKS_VERSION
    host = EKS_HOST
    responseCls = AWSJsonResponse
    service_name = 'eks'


class ElasticKubernetesDriver(ContainerDriver):
    name = 'Amazon EKS'
    website = 'https://aws.amazon.com/eks/'
    connectionCls = EKSJsonConnection


    def __init__(self, access_id, secret, region):
        super(ElasticKubernetesDriver, self).__init__(access_id, secret, host=EKS_HOST % region)
        self.region = region
        self.region_name = region

    def _ex_connection_class_kwargs(self):
        return {'signature_version': '4'}

    def list_clusters(self):
        """
        List the Amazon EKS clusters in your AWS account in the specified region.

        :rtype: ``list`` of :class:`str`
        """
        data = self.connection.request(
            'clusters',
            method='GET',
        ).object

        return data['clusters']

    def destroy_cluster(self, cluster):
        """Delete a cluster.

        :param  cluster: cluster name to delete.
        :type   cluster: `str`

        :rtype: :class:`EKSCluster`
        """
        data = self.connection.request(
            'clusters/{}'.format(cluster),
            method='DELETE',
        ).object
        return EKSCluster(self, data['cluster'])

    def ex_describe_cluster(self, name):
        """
        Detailed info about Amazon EKS clusters with provided name.

        :param  name: Cluster name.
        :type   name: `str`

        :rtype: :class:`EKSCluster`
        """
        data = self.connection.request(
            'clusters/{}'.format(name),
            method='GET',
        ).object

        return EKSCluster(self, data['cluster'])



