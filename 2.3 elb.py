#!/usr/bin/python

import boto
import subprocess
import datetime
from time import sleep
from boto.ec2.elb import HealthCheck
from boto.ec2.elb import ELBConnection

types = 'm1.small'
img_id = 'ami-69e3d500'
key_pair_name = 'jichuanl_project2.3'
test_bash = ['./apache_bench.sh', 'sample.jpg', '100000', '100', '', '']
sec_group = ['sg-98f73bfd']
zones = ['us-east-1d']
subnet = 'subnet-1fd7ee6b'
ports = [(80, 80, 'http'), (8080, 8080, 'http')]

hc = HealthCheck(
	interval=30,
	healthy_threshold=2,
	unhealthy_threshold=10,
	target='HTTP:8080/upload')

elb = ELBConnection()
lb = elb.create_load_balancer('my-lb', zones, ports)
lb.configure_health_check(hc)
lb_dns = lb.dns_name

ec2 = boto.connect_ec2()
#key_pair = ec2.create_key_pair(key_pair_name)
#key_pair.save('/home/ubuntu/.ssh')

rpc = 0
count = 0
while rpc < 2000: #test until get 2000req/sec
  reservation = ec2.run_instances(image_id=img_id, key_name=key_pair_name, instance_type=types, security_group_ids=sec_group, subnet_id=subnet)

  instance = []
  for r in ec2.get_all_instances():
    if r.id == reservation.id:
      break

  while r.instances[0].update()!='running':
    sleep(10)
    r.instances[0].update()

  sleep(240) # wait for init

  lb.register_instances([r.instances[0].id])
  r.instances[0].add_tag('Project', '2.3')
  sleep(60)

  test_bash[4] = lb_dns
  test_bash[5] = './logs/'+types
  print test_bash

  fout = open('./output/'+types, 'w')
  subprocess.call(test_bash, stdout=fout)
  fout.close()
  
  fin = open('./output/'+types, 'r')
  for lines in fin:
    if 'Requests per second:' in lines:
      rpc = float(lines[len('Requests per second:'): lines.find('[#/sec]')])
      break

  count += 1
  fout = open('rpc', 'a')
  s = str(count) + ' : ' + str(rpc) + 'req/sec \n'
  fout.write(s)
  fout.close()
  
lb.delete()
