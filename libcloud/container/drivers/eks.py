import base64

from libcloud.common.aws import SignedAWSConnection, AWSJsonResponse
from libcloud.container.base import ContainerDriver, ContainerCluster
from libcloud.container.types import ClusterState

__all__ = [
    'ElasticKubernetesDriver'
]

EKS_VERSION = '2017-11-01'
EKS_HOST = 'eks.%s.amazonaws.com'


class EKSCluster(ContainerCluster):
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

        decoded_cert = cluster_data['certificateAuthority']['data']
        certificate = base64.b64decode(decoded_cert.encode()).decode() if decoded_cert else None
        self.cluster_certificate = certificate
        super().__init__(cluster_data['arn'], cluster_data['name'], driver, cluster_data)

    def list_containers(self):
        raise NotImplementedError('EKS cluster can\'t list containers.')

    def destroy(self):
        self.driver.destroy_cluster(self.name)

    def __repr__(self):
        return '<EKSCluster: name={}, endpoint={}, version={}, status={}>'.format(
            self.name, self.endpoint, self.version, self.status
        )


class EKSJsonConnection(SignedAWSConnection):

    version = EKS_VERSION
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

    def ex_list_cluster_names(self):
        """
        List Amazon EKS clusters.

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
        cluster = EKSCluster(self, data['cluster'])
        return cluster.status == ClusterState.DELETING

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



