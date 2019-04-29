# Freesixty

A simple Google Analytics API data extraction.

## Installation
```bash
pip install freesixty
```

## Access credentials

To set up access to your Google Analytics follow first step of [these instructions](https://developers.google.com/analytics/devguides/reporting/core/v4/quickstart/service-py).
Store them in your local machine and enter their path into `KEY_FILE_LOCATION` variable.

## Get data
```python
import freesixty

KEY_FILE_LOCATION = './client_secrets.json'
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

analytics = freesixty.initialize_analyticsreporting(KEY_FILE_LOCATION)
result, is_data_golden = freesixty.execute_query(analytics, query)
```

On the other hand if we want to store resulting data to a desired URI.
```python
import freesixty

KEY_FILE_LOCATION = './client_secrets.json'
VIEW_ID = 'XXXXXXX'
folder_uri = 'file:///tmp/example/folder'

query = {
    'reportRequests': [
    {
        'viewId': VIEW_ID,
        'dateRanges': [{'startDate': '2009-01-01', 'endDate': '2019-01-05'}],
        'metrics': [{'expression': 'ga:sessions'}],
        'dimensions': [{'name': 'ga:country', 'name': 'ga:date'}]
    }]
}

analytics = freesixty.initialize_analyticsreporting(KEY_FILE_LOCATION)
freesixty.store_query(analytics, query, folder_uri)
```

## Getting more data
In case a query would return over 100k rows of data it will fail. We can get around it by splitting the date range into smaller chunks:

```python
queries = freesixty.split_query(query=query, start_date='2019-01-01', end_date='2019-02-01', freq='D')

for query in queries:
    freesixty.store_query(analytics, query, folder_uri)
```


# Useful links

* [Try out queries](https://ga-dev-tools.appspot.com/query-explorer/)
* [Compose queries](https://ga-dev-tools.appspot.com/request-composer/)


# TODO:
* More complete tests


:cake: