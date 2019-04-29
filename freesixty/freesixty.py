import copy
from datetime import datetime
from dateutil.rrule import rrule, DAILY, WEEKLY, MONTHLY
from hashlib import md5
import json
import os
import urllib

import boto3
from botocore.exceptions import ClientError
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

SCOPES = ('https://www.googleapis.com/auth/analytics.readonly',)


def initialize_analyticsreporting(key_file_location, scopes=SCOPES):
    """Initializes an Analytics Reporting API V4 service object.

    Returns:
    An authorized Analytics Reporting API V4 service object.
    """
    credentials = ServiceAccountCredentials.from_json_keyfile_name(key_file_location, scopes)

    # Build the service object.
    analytics = build('analyticsreporting', 'v4', credentials=credentials)

    return analytics


def _exists(uri):
    """Checks if the passed URI exists.

    Args:
    uri, a string.
    Returns:
    Boolean, regarding if URI exists or not.
    """

    parsed_uri = urllib.parse.urlparse(uri)

    if parsed_uri.scheme.lower() == 's3':
        s3 = boto3.resource('s3')
        try:
            s3.Object(parsed_uri.netloc, parsed_uri.path.lstrip('/')).load()
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                return False
            else:
                raise e

    elif parsed_uri.scheme == 'file':
        return os.path.isfile(parsed_uri.path)

    else:
        raise NotImplementedError('Invalid URI on method not yet implemented. {uri}'.format(uri=uri))


def _write(data, uri):
    """Writes passed data into the passed URI.

    Args:
    data, a string of data to be written.
    uri, a location where to write the data to.
    """

    parsed_uri = urllib.parse.urlparse(uri)

    if parsed_uri.scheme.lower() == 's3':
        s3 = boto3.resource('s3')
        s3.Object(parsed_uri.netloc, parsed_uri.path.lstrip('/')).put(Body=data)

    elif parsed_uri.scheme == 'file':
        os.makedirs(os.path.dirname(parsed_uri.path), 0o755, exist_ok=True)
        with open(parsed_uri.path, 'w', encoding='utf-8') as f:
            f.write(data)

    else:
        raise NotImplementedError('Invalid URI on method not yet implemented. {uri}'.format(uri=uri))


def _generate_folder_uri(query):
    """Generate a relative path to store data into by hashing the passed query.

    query, a dict, Google Analytics reporting V4 query.
    Returns:
    relative folder path, a string.
    """

    assert len(query['reportRequests']) == 1  # Only allow one report per query.

    undated_query = copy.deepcopy(query)
    [report_request.pop('dateRanges') for report_request in undated_query['reportRequests']]
    query_hash = md5(str(undated_query).encode()).hexdigest()

    date_ranges = [date_range for report_request in query['reportRequests'] for date_range in report_request['dateRanges']]
    date_range_strings = ['-'.join(date_range.values()) for date_range in date_ranges]
    all_dates = '_'.join(date_range_strings)

    return '-'.join([query['reportRequests'][0]['viewId'], query_hash, all_dates])


def execute_query(analytics, query):
    """Queries the Analytics Reporting API V4 and returns result.

    Args:
    analytics: An authorized Analytics Reporting API V4 service object.
    query: Dict. Query for the reporting API.
    Returns:
    out: Returned API data.
    """

    assert len(query['reportRequests']) == 1  # Only allow one report per query.

    q = copy.deepcopy(query)
    out = {'reports': [{'data': {'rows': []}}]}
    is_data_golden = True

    while True:
        report = analytics.reports().batchGet(body=q).execute()

        if not report['reports'][0]["data"].get('isDataGolden', False):
            is_data_golden = False

        token = report.get('reports', [])[0].get('nextPageToken', '')
        out['reports'][0]['data']['rows'] += report['reports'][0]['data'].get('rows', [])

        if token:
            q['reportRequests'][0]['pageToken'] = token
        else:
            break

    out['reports'][0]['columnHeader'] = report['reports'][0]['columnHeader']

    return out, is_data_golden


def store_query(analytics, query, folder_uri, fmt='csv', delimiter = '\01', only_golden=False):
    """Queries the Analytics Reporting API V4 and stores the result of the query to the given URI.
    If data already exists the query is not executed.

    Args:
    analytics: An authorized Analytics Reporting API V4 service object.
    query: Dict. Query for the reporting API.
    folder_uri: URI of the location there to store the query output data.
    fmt: Format into which passed data is stored.
    Returns:
    file_uri: URI to stored data.
    """

    file_uri = os.path.join(folder_uri, _generate_folder_uri(query)) + '.' + fmt

    if _exists(file_uri):
        return file_uri

    out, is_data_golden = execute_query(analytics, query)

    if only_golden and not is_data_golden:
        raise ValueError("Data is not golden and we shouldn't write it.")

    if fmt == 'csv':
        newline = '\n'
        dimension_columns = out['reports'][0]['columnHeader']['dimensions']
        metric_headers = out['reports'][0]['columnHeader']['metricHeader']['metricHeaderEntries']
        metric_columns = [x['name'] for x in metric_headers]
        columns = [x.replace('ga:', '') for x in dimension_columns + metric_columns]
        data = delimiter.join(columns) + newline

        for row in out['reports'][0]['data']['rows']:
            row = row['dimensions'] + [y for x in row['metrics'] for y in x['values']]
            data = data + delimiter.join(row) + '\n'
    elif fmt == 'json':
        data = json.dumps(out, indent=4, sort_keys=True)
    else:
        raise NotImplementedError("Format {fmt} support is not yet implemented.")

    _write(data, file_uri)
    return file_uri


def split_query(query, start_date, end_date, freq='M', fmt='%Y-%m-%d', byweekday=0):
    """Splits a query into time periods to split it into smaller queries.

    Args:
    query: A Google Analytics Reporting API V4 query.
    start_date: Beginning of the query.
    end_date: The end of the query.
    freq: Frequency of periods into which to split the query. See: rfc5545.
    fmt: Format in which to pass the times are passed.
    byweekday: Which date should the week begin on. Only applies if freq is 'W'.
    Returns:
    A list of Google Analytics Reporting API V4 queries.
    """
    query = copy.deepcopy(query)
    start_date = datetime.strptime(start_date, fmt)
    end_date = datetime.strptime(end_date, fmt)

    if freq == 'D':
        dates = rrule(freq=DAILY, dtstart=start_date, until=end_date)

    elif freq == 'W':
        dates = rrule(freq=WEEKLY, dtstart=start_date, until=end_date, byweekday=byweekday)

    elif freq == 'M':
        dates = rrule(freq=MONTHLY, dtstart=start_date, until=end_date, bymonthday=1)

    else:
        raise NotImplementedError("Frequency {freq} split is not valid or implemented.".format(freq=freq))

    periods = [{'startDate': start_date.strftime(fmt),
                'endDate': end_date.strftime(fmt)} for start_date, end_date in zip(dates, dates[1:])]

    queries = []
    for period in periods:
        sub_query = copy.deepcopy(query)
        [report_request.update({'dateRanges': [period]}) for report_request in sub_query['reportRequests']]
        queries.append(sub_query)

    return queries
