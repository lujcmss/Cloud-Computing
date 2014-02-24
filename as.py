#!/usr/bin/python

import boto
from time import sleep
from boto.ec2.elb import HealthCheck
from boto.ec2.elb import ELBConnection
from boto.ec2.autoscale import AutoScaleConnection
from boto.ec2.autoscale import LaunchConfiguration
from boto.ec2.autoscale import AutoScalingGroup
from boto.ec2.autoscale import ScalingPolicy
from boto.ec2.autoscale import Tag
from boto.ec2.cloudwatch import CloudWatchConnection
from boto.ec2.cloudwatch import MetricAlarm

types = 'm1.small'
img_id = 'ami-99e2d4f0'
key_pair_name = 'jichuanl_project2.4'
sec_group = ['sg-98f73bfd']
zones = ['us-east-1d']
subnet = 'subnet-1fd7ee6b'
ports = [(80, 80, 'http'), (8080, 8080, 'http')]
arntopic = 'arn:aws:sns:us-east-1:266359701207:AS_notification'
notificationtypes = ['autoscaling:EC2_INSTANCE_LAUNCH', 'autoscaling:EC2_INSTANCE_LAUNCH_ERROR', 'autoscaling:EC2_INSTANCE_TERMINATE', 'autoscaling:EC2_INSTANCE_TERMINATE_ERROR', 'autoscaling:TEST_NOTIFICATION']

hc = HealthCheck(
	interval=30,
	healthy_threshold=2,
	unhealthy_threshold=10,
	target='HTTP:8080/upload')

#ec2 = boto.connect_ec2()
#key_pair = ec2.create_key_pair(key_pair_name)
#key_pair.save('/home/ubuntu/.ssh')

#start script
elb = ELBConnection()
lb = elb.create_load_balancer('mylb', zones, ports)
lb.configure_health_check(hc)
lb_dns = lb.dns_name

asconn = AutoScaleConnection()
lc = LaunchConfiguration(name='mylc', image_id=img_id,
			instance_type=types,
			key_name=key_pair_name,
			security_groups=sec_group,
			instance_monitoring=True)
asconn.create_launch_configuration(lc)
asg = AutoScalingGroup(group_name='myasg', load_balancers=['mylb'],
			availability_zones=zones,
			launch_config=lc, 
			tags=[Tag(key='Project', value='2.4', propagate_at_launch=True, resource_id='myasg')],
			min_size=2, max_size=5, desired_capacity=2,
                        connection=asconn)
asconn.create_auto_scaling_group(asg)
asconn.put_notification_configuration(asg, arntopic, notificationtypes) 

scaleout = ScalingPolicy(name='scaleout', adjustment_type='ChangeInCapacity',
			as_name='myasg', scaling_adjustment=1,  cooldown=180)
scalein =  ScalingPolicy(name='scalein' , adjustment_type='ChangeInCapacity',
			as_name='myasg', scaling_adjustment=-1, cooldown=180)
asconn.create_scaling_policy(scaleout)
asconn.create_scaling_policy(scalein)
scaleout = asconn.get_all_policies(policy_names=['scaleout'], as_group='myasg')[0]
scalein  = asconn.get_all_policies(policy_names=['scalein'], as_group='myasg')[0]

cw = CloudWatchConnection()
alarm_dimensions = {"AutoScalingGroupName": 'myasg'}

scaleup_alarm = MetricAlarm(name='scaleup', namespace='AWS/EC2',
			metric='CPUUtilization', statistic='Average',
			comparison='>', threshold='80',
			period='300', evaluation_periods=1,
			alarm_actions=[scaleout.policy_arn],
			dimensions=alarm_dimensions)
scaledown_alarm = MetricAlarm(name='scaledown', namespace='AWS/EC2',
			metric='CPUUtilization', statistic='Average',
			comparison='<', threshold='20',
			period='300', evaluation_periods=1,
			alarm_actions=[scalein.policy_arn],
			dimensions=alarm_dimensions)
cw.create_alarm(scaleup_alarm)
cw.create_alarm(scaledown_alarm)

#asg.shutdown_instances()
#ag.delete()
#lc.delete()
