manager:
  - ${manager_ip}

monitor:
  - ${manager_ip}

gc:
  - ${manager_ip}

tserver:
%{ for ip in worker_ips ~}
  - ${ip}
%{ endfor ~}

compaction:
  coordinator:
    - ${manager_ip}
  compactor:
    - queue:
      - q1
      - q2
    - q1:
%{ for ip in worker_ips ~}
        - ${ip}
%{ endfor ~}
    - q2:
%{ for ip in worker_ips ~}
        - ${ip}
%{ endfor ~}
