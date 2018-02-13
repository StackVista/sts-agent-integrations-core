# (C) Datadog, Inc. 2010-2016
# All rights reserved
# Licensed under Simplified BSD License (see LICENSE)

# stdlib
import json

# 3rd party

import boto3

# project
from checks import AgentCheck
import pprint

EVENT_TYPE = SOURCE_TYPE_NAME = 'aws'


class AwsCheck(AgentCheck):

    instance_key = {
        "type": "AWS",
        "url": "dummy"
    }

    client = boto3.client('elb')
    clientv2 = boto3.client('elbv2')


    def check(self, instance):

        self.start_snapshot(self.instance_key)

        self.process_elb_classic()
        # self.process_elb_v2()

        self.stop_snapshot(self.instance_key)

    def process_elb_classic(self):
        # ELB classic
        for elb in self.client.describe_load_balancers()['LoadBalancerDescriptions']:
            external_id = "classic_elb_" + elb["LoadBalancerName"]
            data = elb

            self.component(self.instance_key, external_id, {'name': 'elb_classic'}, data)

            instance_ports = [listener['Listener']['InstancePort'] for listener in elb['ListenerDescriptions']]

            for instance in elb['Instances']:
                for port in instance_ports:
                    instance_external_id = instance['InstanceId'] + ':' + str(port)
                    instance_data = {
                        'Instance': instance,
                        'ListenerDescriptions': elb['ListenerDescriptions']
                    }

                    self.component(self.instance_key, instance_external_id, {'name': 'elb_classic_instance'}, instance_data)

                    self.relation(self.instance_key, external_id, instance_external_id, {'name': 'elb_classic_has_instance'}, {})

    def process_elb_v2(self):
        for elbv2 in self.clientv2.describe_load_balancers()['LoadBalancers']:
            elb_external_id = elbv2['LoadBalancerArn']
            elb_data = elbv2
            elb_type = "elb_v2_" + elbv2['Type']  # for example, elb_application

            # listeners
            elb_data['listeners'] = []
            for listener in self.clientv2.describe_listeners(LoadBalancerArn=elbv2["LoadBalancerArn"])["Listeners"]:
                elb_data['listeners'].append(listener)

            self.component(self.instance_key, elb_external_id, {'name': elb_type}, elb_data)

        # target group
        for target_group in self.clientv2.describe_target_groups(LoadBalancerArn=elbv2["LoadBalancerArn"])["TargetGroups"]:
            target_group_external_id = target_group['TargetGroupArn']
            target_group_data = target_group

            self.component(self.instance_key, target_group_external_id, {'name': 'elb_v2_target_group'}, target_group_data)

            # relation between elb and target group
            for elb_arn in target_group['LoadBalancerArns']:
                self.relation(self.instance_key, elb_arn, target_group_external_id, {'name': 'elb_v2_has_target_group'}, {})

            # target
            for target in self.clientv2.describe_target_health(TargetGroupArn=target_group['TargetGroupArn'])['TargetHealthDescriptions']:
                pprint.pprint(target)

                # TODO health check information -> event

                # assuming instance here, IP is another option
                target_external_id = target['Target']['Id'] + ':' + str(target['Target']['Port'])
                target_data = {}

                self.component(self.instance_key, target_external_id, {'name': 'elb_v2_target_group_instance'}, target_data)

                # relation between target group and target
                self.relation(self.instance_key, target_group_external_id, target_external_id, {'name': 'elb_v2_is_target'}, {})
