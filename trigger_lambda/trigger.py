import fsspec
import json

def handler(event, context):
    fs = fsspec.filesystem('s3')
    polars = fs.ls('s3://riotrackerlambdastack-cape2riotrackingbucket493cd-ax0w0veyvbks/polars/')
    names = [' '.join(pol.split('/')[-1].split('.')[0].split('_')) for pol in polars]
    for name in names:
        file_name = '_'.join(name.split(' '))
        event = {'boat':name}
        fs = fsspec.filesystem('s3')
        with fs.open(f"s3://riotrackerlambdastack-cape2riotrackingbucket493cd-ax0w0veyvbks/inputs/{file_name}.json", 'w') as f:
            json.dump(event, f)