import boto3
from datetime import datetime, timezone

date = datetime.now()
date_dt = date.replace(tzinfo=timezone.utc)


ebs = boto3.client('ebs')
ec2 = boto3.client('ec2')

volumeid = input("Enter the volume ID:  ")

response = ec2.describe_snapshots(
    Filters=[
        {
            'Name': 'volume-id',
            'Values': [volumeid]
        },
    ]
)

def snapshot_size(snapshotid):
    response = ebs.list_snapshot_blocks(
        SnapshotId=snapshotid,
        # SnapshotId=snapshotid,
        MaxResults=1000
    )

    num_blks = len(response["Blocks"])
    blks = response["Blocks"]
    if len(blks) == 0:
        # print("no block changes")
        blk_size = 0
    else:
        blk_size = response["BlockSize"]


    while True:
        num_blks += len(response["Blocks"])
        if len(response) != 6 or len(blks) == 0:
            break
        token = response["NextToken"]
        response = ebs.list_snapshot_blocks(
            NextToken=token,
            SnapshotId=snapshotid,
            MaxResults=1000
        )
    return num_blks * blk_size / 1024 / 1024 / 1024


def snapshot_diff(prev_snapshotid, next_snapshotid):
    response = ebs.list_changed_blocks(
        FirstSnapshotId=prev_snapshotid,
        SecondSnapshotId=next_snapshotid,
        # SnapshotId=snapshotid,
        MaxResults=1000
    )

    num_blks = len(response["ChangedBlocks"])
    blks = response["ChangedBlocks"]
    if len(blks) == 0:
        blk_size = 0
    else:
        blk_size = response["BlockSize"]

    while True:
        num_blks += len(response["ChangedBlocks"])
        if len(response) != 6 or len(blks) == 0:
            break
        token = response["NextToken"]
        response = ebs.list_changed_blocks(
            NextToken=token,
            FirstSnapshotId=prev_snapshotid,
            SecondSnapshotId=next_snapshotid,
            MaxResults=1000
        )
    return num_blks * blk_size / 1024 / 1024 / 1024


snapshots = []

while True:
    snapshots.extend(response.get('Snapshots', []))

    token = response.get('NextToken', '')
    if len(response) == 2:
        break
    response = ec2.describe_snapshots(NextToken=token, MaxResults=1000)
if snapshots:
    sorted_snapshots = sorted(snapshots, key=lambda snapshot: snapshot['VolumeId'] + str(snapshot['StartTime']))
    vol_prev = ""
    snapshotid_prev = ""
    total_gb = 0
    num_snapshots = 0
    print('%25s %20s %25s %15s %25s %10s' % ("snapshot_id", "changed size (GB)", "percent of the volume (%)", "storage tier", "start time", "age"))
    for snapshot in sorted_snapshots:
        num_snapshots += 1
        age = (date_dt - snapshot["StartTime"]).days

        if snapshot["StorageTier"] == "archive":
            print('%25s %20s %25s %15s %25s %10s' % (snapshot["SnapshotId"], "N/A", "N/A", snapshot["StorageTier"], str(snapshot['StartTime']).split('.')[0],age))
        else:
            vol = snapshot["VolumeId"]
            snapshotid = snapshot["SnapshotId"]
            timestamp = str(snapshot["StartTime"]).split('.')[0]
            if vol == vol_prev and vol != 'vol-ffffffff' and snapshot["StorageTier"] != "Archive":
                diff = snapshot_diff(snapshotid_prev, snapshotid)
                print('%25s %20s %25s %15s %25s %10s' % (snapshotid, str(round(diff, 2)), str(round(diff / snapshot["VolumeSize"] * 100, 2)),snapshot["StorageTier"], str(snapshot['StartTime']).split('.')[0], age))
                total_gb += diff
                vol_prev = vol

            else:
                initial = snapshot_size(snapshotid)
                print('%25s %20s %25s %15s %25s %10s' % (snapshotid, str(round(initial, 2)), str(round(initial / snapshot["VolumeSize"] * 100, 2)),snapshot["StorageTier"], str(snapshot['StartTime']).split('.')[0], age))
                total_gb += initial
                vol_prev = vol
            snapshotid_prev = snapshotid
    print("Volume " + vol + " has " + str(num_snapshots) + " snapshots," + "with estimated consumed capacity of " + str(round(total_gb,2)) + "GB, excluding any archived snapshots.")
else:
    print("no snapshots found for " + volumeid)

