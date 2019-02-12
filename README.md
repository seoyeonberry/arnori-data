## Batch Streak Counter

This is a Python program that populates streak counts for all users

## Usage

```python
import os
from sqlalchemy import create_engine
from batch_streak_count import build_streak_counts

url = os.environ['DATABASE_URL']
engine = create_engine(url)
effected = build_streak_counts(engine)

print(f'{effected} got inserted')
```
