To test scheduler running locally

a) Charmander has to be up and running

b) Stop charmander-scheduler running on master1
Has to be run in root directory of charmander project
$ vagrant ssh master1
$ sudo service charmander-scheduler stop
$ exit

c) Run charmander-scheduler locally
Get the ip of your local machine using ifconfig and replace REPLACE_IP with your actual ip address
$ charmander-scheduler -stderrthreshold=INFO -local-ip="REPLACE_IP" -master="172.31.1.11:5050"

d) Deploy/run cAdvisor on Charmander
$ ./start_cadvisor

e) Kill cAdvisor
$ ./kill_cadvisor


