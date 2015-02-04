# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>

!pwd

# <codecell>

url_times = !cat urls_tested.log

# <codecell>

print len(url_times)

# <codecell>

times = []
urls = []
for line in url_times:
    try:
        time, url = line.split(":", 1)
        time = float(time)
        url = url.strip()
        times.append(time)
        urls.append(url)
    except ValueError:
        pass

# <codecell>

import pandas as pd
import numpy as np

# <codecell>

df = pd.DataFrame(data={'time':times, 'urls':urls})

# <codecell>

df.count()

# <codecell>

df["category"] = df.urls.map(lambda url: url.split('/')[3])

# <codecell>

df.sort('time', ascending=False).head()

# <codecell>

%pylab inline

# <codecell>

df.time.plot(style='r.')
ylabel(u"Time taken for this url (seconds)")

# <codecell>

df.sort('time', ascending=False).head(10)

# <markdowncell>

# # Minimum, Maximum, Mean and Median

# <codecell>

print 'Min: %.2f seconds\r\nMax: %.2f seconds\r\nMean: %.2f seconds\r\nMedian: %.2f seconds'% (df.time.min(), df.time.max(), df.time.mean(), df.time.median())

