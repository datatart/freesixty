# Freesixty

A simple Google Analytics API data extraction.

# Example:

#### 1. Install freesixty
```bash
pip install freesixty
```

#### 2. Get data
```python
import freesixty

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = 'gcp_keyfile.json'
VIEW_ID = 'XXXXXXX'

query = {
    'reportRequests': [
    {
        'viewId': VIEW_ID,
        'dateRanges': [{'startDate': '2009-01-01', 'endDate': '2019-01-05'}],
        'metrics': [{'expression': 'ga:sessions'}],
        'dimensions': [{'name': 'ga:country', 'name': 'ga:date'}]
    }]
}

folder_uri = 'file:///tmp/example/folder'
analytics = freesixty.initialize_analyticsreporting(KEY_FILE_LOCATION)
freesixty.store_query(analytics, query, folder_uri, 'json')
```

#### 3. Use it
```bash
ls /tmp/example/folder
```

#### 4. Profit
🍰🍰:cake:🍰🍰🍰🍰