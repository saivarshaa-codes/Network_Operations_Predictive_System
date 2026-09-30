# Telecom Spark Pipeline — Job Contract

## Input

The job expects a directory containing raw telecom
CSV files matching:

sms-call-internet-mi-*.csv

Required raw columns:

- datetime
- CellID
- countrycode
- smsin
- smsout
- callin
- callout
- internet

## Processing

1. Read all matching raw CSV files.
2. Apply the controlled raw-to-canonical column mapping.
3. Reject records with:
   - missing grid_id
   - missing timestamp
   - negative activity values
4. Replace remaining NULL activity measures with zero.
5. Derive total_sms, total_calls and total_activity.
6. Derive date, hour and day_of_week.
7. Aggregate country-level records to grid_id + timestamp.
8. Calculate hourly analytics including internet_share.
9. Validate the static Milan GeoJSON reference.
10. Keep Polygon geometry outside the hourly fact table.

## Outputs

output_path/activity/

Clean activity data in Parquet,
partitioned by date.

output_path/hourly_grid_summary/

One record per grid_id + timestamp in Parquet.
No geometry column is stored.

output_path/dashboard_summary/

Small CSV summary containing the top 10 grids
by total activity.

output_path/JOB_CONTRACT.md

Description of the pipeline input, processing,
outputs and failure conditions.

## Geographic Reference

The Milan GeoJSON remains a separate static reference
for map rendering.

The analytics data links to geography using grid_id.

## Failure Conditions

The job fails with a non-zero exit code when:

- the input folder does not exist
- no matching raw CSV files are found
- the reference GeoJSON does not exist
- required columns are missing
- geometry is incorrectly present in hourly analytics
- round-trip validation fails
- an unexpected processing error occurs