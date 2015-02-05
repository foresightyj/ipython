# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>

#!pip install pika==0.9.5

# <markdowncell>

# # Sending

# <codecell>

#!/usr/bin/env python
import pika

connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))
channel = connection.channel()


channel.queue_declare(queue='hello')

channel.basic_publish(exchange='',
                      routing_key='hello',
                      body='Hello World!')
print " [x] Sent 'Hello World!'"
connection.close()

# <codecell>


