import fsspec
import datetime

def handler(event, context):
    fs = fsspec.filesystem('s3')
    files = fs.glob('s3://riotrackerlambdastack-cape2riotrackingbucket493cd-ax0w0veyvbks/results/*.json')
    for file in files:
        today = datetime.datetime.today().strftime('%y%m%d')
        if not today == fs.info(f's3://{file}')['LastModified'].strftime('%y%m%d'):
            print(file)
            name = ' '.join(file.split('/')[-1].split('.')[0].split('_'))
            file_name = '_'.join(name.split(' '))
            event = {'boat':name}
            fs = fsspec.filesystem('s3')
            with fs.open(f"s3://riotrackerlambdastack-cape2riotrackingbucket493cd-ax0w0veyvbks/inputs/{file_name}.json", 'w') as f:
                json.dump(event, f)