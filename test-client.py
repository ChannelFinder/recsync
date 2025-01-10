#!/usr/bin/env python
'''Listen for recceiver Announcements
'''

import socket

S = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 0)

S.bind(('',5049))

print(S, S.fileno())
while True:
  print('>>', S.recvfrom(1024))
