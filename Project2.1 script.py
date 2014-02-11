#!/usr/bin/python

import boto
import subprocess
import datetime
from time import sleep

types = ['m1.large', 'm1.medium', 'm1.small']
img_id = 'ami-69e3d500'
key_pair_name = 'jichuanl_project2.1'
test_bash = ['./apache_bench.sh', 'sample.jpg', '100000', '100', '', '']
sec_group = ['sg-98f73bfd']
subnet = 'subnet-1fd7ee6b'
time_period = 60

ec2 = boto.connect_ec2()
#key_pair = ec2.create_key_pair(key_pair_name)
#key_pair.save('/home/ubuntu/.ssh')

def timetostring(mytime):
  return str(mytime.hour) + ':' + str(mytime.minute) + ':' + str(mytime.second)

for i in range(0, 3): #test three kinds of instances
  reservation = ec2.run_instances(image_id=img_id, key_name=key_pair_name, instance_type=types[i], security_group_ids=sec_group, subnet_id=subnet)

  instance = []
  for r in ec2.get_all_instances():
    if r.id == reservation.id:
      break

  while r.instances[0].update()!='running':
    sleep(10)
    r.instances[0].update()
  sleep(300) # wait for init

  instance.append(r.instances[0].id)
  r.instances[0].add_tag('Project', '2.1')
  ec2.monitor_instances(instance, dry_run=False)

  start_time = datetime.datetime.now()

  for j in range(0, 10): #run 10 times for each instance
    test_bash[4] = r.instances[0].public_dns_name
    test_bash[5] = './logs/'+types[i]+'_'+str(j)
    print test_bash
    fout = open('./output/'+types[i]+'_'+str(j), 'w')
    subprocess.call(test_bash, stdout=fout)
    fout.close()

  end_time = datetime.datetime.now()

  sleep(300) #wait for CPU status update
  metrics = boto.connect_cloudwatch()
  m = metrics.list_metrics(dimensions={'InstanceId':instance[0]}, metric_name='CPUUtilization')[0]

  datas = m.query(start_time, end_time, ['Maximum', 'Minimum', 'Average'], 'Percent', period=time_period)

  fout = open('CPU_'+types[i], 'w')
  for i in range(0, len(datas)):
    s = timetostring(start_time + datetime.timedelta(seconds=time_period)*i) + '-' +  timetostring(start_time + datetime.timedelta(seconds=time_period)*(i+1)) + ' : ' + str(datas[i]['Maximum']) + ' ' +  str(datas[i]['Minimum']) + ' ' + str(datas[i]['Average']) + '\n'
    fout.write(s)
  fout.close()

  ec2.terminate_instances(instance_ids=instance)
