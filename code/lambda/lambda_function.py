import json
import awswrangler as wr

# This funtion converts new uploaded csv files into s3 to parquet 
def lambda_handler(event, context):
    for record in event["Records"]:
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]

        if key.endswith(".csv") and key.startswith("raw_data/"):
            input_path = f"s3://{bucket}/{key}"
            output_path = f"s3://{bucket}/parquet_data/{key.split('/')[-1].replace('.csv', '.parquet')}"

            df = wr.s3.read_csv(path=input_path)
            wr.s3.to_parquet(df=df, path=output_path)

            print(f"Converted {input_path} to {output_path}")
