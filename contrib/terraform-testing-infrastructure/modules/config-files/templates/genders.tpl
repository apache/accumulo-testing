manager    manager
%{ for index, ip in worker_ips ~}
worker${index}    worker
%{ endfor ~}
