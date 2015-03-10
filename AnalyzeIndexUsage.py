# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>

%pylab inline

# <codecell>

import numpy as np
import pandas as pd

# <codecell>

header=["", "", "key_name", "partition_no", "scan_count", "seek_count"]

# <codecell>

df = pd.ExcelFile("C:\\Users\\yj\\Desktop\\index.xlsx", columns = header).parse()

# <codecell>

df.columns

# <codecell>


# <codecell>

df[2]

# <codecell>


