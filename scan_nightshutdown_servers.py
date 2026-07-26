import os
import sys
import time
import logging
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure Console Logging (No log files generated)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# AWS Configuration from .env
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN")
PATCH_BASELINE_ID = os.getenv("PATCH_BASELINE_ID")

TAG_KEY_24_7 = os.getenv("TAG_KEY_24_7", "Is_24-7")
TAG_VALUE_STANDBY = os.getenv("TAG_VALUE_STANDBY", "No")

SNS_ENABLED = os.getenv("SNS_ENABLED", "True").lower() == "true"

# Initialize Boto3 Clients
try:
    ec2 = boto3.client("ec2", region_name=AWS_REGION)
    ssm = boto3.client("ssm", region_name=AWS_REGION)
    sns = boto3.client("sns", region_name=AWS_REGION)
except Exception as e:
    logger.error(f"Failed to initialize AWS clients: {e}")
    sys.exit(1)


def send_sns_notification(message, subject="WalletHR Night Shutdown Servers Patch Scan Report"):
    """Sends SNS notification if enabled and ARN is present."""
    if not SNS_ENABLED or not SNS_TOPIC_ARN:
        logger.info("SNS notifications disabled or SNS_TOPIC_ARN not set. Skipping notification.")
        return
    try:
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject,
            Message=message
        )
        logger.info("SNS notification sent successfully.")
    except Exception as e:
        logger.error(f"Failed to send SNS notification: {e}")


def discover_nightshutdown_servers():
    """Discovers running Night Shutdown (standby) web servers using tags."""
    logger.info("Starting Resource Discovery for Night Shutdown servers...")
    discovered = []
    try:
        paginator = ec2.get_paginator('describe_instances')
        page_iterator = paginator.paginate(
            Filters=[
                {'Name': f'tag:{TAG_KEY_24_7}', 'Values': [TAG_VALUE_STANDBY]},
                {'Name': 'instance-state-name', 'Values': ['running']}
            ]
        )
        for page in page_iterator:
            for reservation in page.get('Reservations', []):
                for instance in reservation.get('Instances', []):
                    inst_id = instance.get('InstanceId')
                    tags = {tag['Key']: tag['Value'] for tag in instance.get('Tags', [])}
                    name = tags.get('Name', inst_id)
                    tg_name = tags.get('TargetGroup', 'N/A')
                    discovered.append({
                        'InstanceId': inst_id,
                        'Name': name,
                        'TargetGroup': tg_name
                    })

        logger.info(f"Discovered {len(discovered)} running Night Shutdown web server(s).")
        return discovered
    except Exception as e:
        logger.error(f"Resource Discovery failed: {e}")
        sys.exit(1)


def register_patch_baseline():
    """Registers default patch baseline if configured in .env."""
    if not PATCH_BASELINE_ID:
        return
    try:
        logger.info(f"Setting up Default Patch Baseline ID: {PATCH_BASELINE_ID}")
        ssm.register_default_patch_baseline(BaselineId=PATCH_BASELINE_ID)
        logger.info("Successfully registered default patch baseline.")
    except Exception as e:
        logger.warning(f"Default patch baseline registration warning: {e}")


def clear_wua_datastore_step(instance_ids):
    """Clears WUA DataStore cache to prevent null KBId errors prior to scan/patch operations."""
    if not instance_ids:
        return
    logger.info(f"Clearing WUA DataStore cache on {len(instance_ids)} instance(s)...")
    try:
        response = ssm.send_command(
            InstanceIds=instance_ids,
            DocumentName="AWS-RunPowerShellScript",
            Parameters={
                "commands": [
                    'Stop-Service wuauserv -Force -ErrorAction SilentlyContinue',
                    'Remove-Item "C:\\Windows\\SoftwareDistribution\\DataStore\\*" -Recurse -Force -ErrorAction SilentlyContinue',
                    'Start-Service wuauserv -ErrorAction SilentlyContinue'
                ]
            },
            TimeoutSeconds=120,
            Comment="Clear WUA DataStore cache to prevent null KBId errors"
        )
        time.sleep(5)
    except Exception as e:
        logger.warning(f"Notice: WUA DataStore cleanup SSM call warning: {e}")


def scan_patches(instances):
    """Triggers SSM AWS-RunPatchBaseline Scan operation and retrieves missing patches."""
    if not instances:
        logger.info("No instances provided for scanning.")
        return 0, []

    instance_ids = [inst['InstanceId'] for inst in instances]
    id_to_name = {inst['InstanceId']: inst['Name'] for inst in instances}

    # Pre-clean WUA DataStore cache to prevent null KBId errors
    clear_wua_datastore_step(instance_ids)

    logger.info(f"Triggering SSM Patch Scan on {len(instance_ids)} instance(s)...")
    try:
        response = ssm.send_command(
            InstanceIds=instance_ids,
            DocumentName="AWS-RunPatchBaseline",
            Parameters={
                "Operation": ["Scan"],
                "RebootOption": ["NoReboot"]
            },
            Comment="Automated scan for missing patches on Night Shutdown web servers"
        )
        command_id = response["Command"]["CommandId"]
    except Exception as e:
        logger.error(f"Failed to send SSM Scan command: {e}")
        sys.exit(1)

    # Wait for SSM command completion
    failed_scans = []
    for inst_id in instance_ids:
        name = id_to_name[inst_id]
        elapsed = 0
        while True:
            try:
                invocation = ssm.get_command_invocation(CommandId=command_id, InstanceId=inst_id)
                status = invocation["Status"]
                if status == "Success":
                    break
                elif status in ["Failed", "TimedOut", "Cancelled"]:
                    failed_scans.append(f"{name} ({inst_id}) - SSM Status: {status}")
                    break
            except ClientError:
                pass
            time.sleep(10)
            elapsed += 10
            if elapsed >= 1200:
                failed_scans.append(f"{name} ({inst_id}) - Wait timeout")
                break

    if failed_scans:
        logger.error("SSM Scan failed to complete on the following instances:\n" + "\n".join(failed_scans))
        sys.exit(1)

    logger.info("SSM Scan completed. Waiting 15 seconds for SSM compliance database sync...")
    time.sleep(15)

    # Retrieve missing patches list via describe_instance_patches
    missing_patches_report = []
    total_missing = 0

    for inst_id in instance_ids:
        name = id_to_name[inst_id]
        missing_list = []
        try:
            paginator = ssm.get_paginator('describe_instance_patches')
            page_iterator = paginator.paginate(
                InstanceId=inst_id,
                Filters=[{'Key': 'State', 'Values': ['Missing']}]
            )
            for page in page_iterator:
                for patch in page.get('Patches', []):
                    kb_id = patch.get('KBId', 'N/A')
                    title = patch.get('Title', 'N/A')
                    text_severity = patch.get('Severity', 'N/A')
                    missing_list.append(f"  * {kb_id} - {title} (Severity: {text_severity})")

            if missing_list:
                total_missing += len(missing_list)
                missing_patches_report.append(f"- {name} ({inst_id}):\n" + "\n".join(missing_list))
            else:
                missing_patches_report.append(f"- {name} ({inst_id}): No missing patches found.")
        except Exception as e:
            logger.error(f"Failed to retrieve patches for {name} ({inst_id}): {e}")

    return total_missing, missing_patches_report


def main():
    logger.info("==================================================")
    logger.info(" WalletHR: Night Shutdown Servers Patch Scan Script")
    logger.info("==================================================")

    # Step 1: Discover Night Shutdown Servers
    discovered_servers = discover_nightshutdown_servers()

    if not discovered_servers:
        msg = "WalletHR Night Shutdown Servers Patch Scan Report:\n\nNo running Night Shutdown web servers discovered."
        logger.info(msg)
        send_sns_notification(msg)
        sys.exit(0)

    discovery_msg_lines = ["WalletHR Night Shutdown Servers Discovered:\n"]
    for server in discovered_servers:
        discovery_msg_lines.append(f"- {server['Name']} ({server['InstanceId']}) [Target Group: {server['TargetGroup']}]")

    # Step 2: Register Patch Baseline
    register_patch_baseline()

    # Step 3: Trigger Scan & Gather Report
    total_missing, missing_report = scan_patches(discovered_servers)

    # Build Final Notification Message
    report_lines = ["WalletHR Night Shutdown Web Servers - Patch Scan Report\n"]
    report_lines.append("\n".join(discovery_msg_lines))
    report_lines.append("\n--------------------------------------------------")

    if total_missing > 0:
        report_lines.append(f"\nScan Summary: {total_missing} missing patch(es) found across fleet. Action required.\n")
        report_lines.append("Missing Patches Detail:")
        report_lines.extend(missing_report)
    else:
        report_lines.append("\nScan Summary: All Night Shutdown web servers are fully compliant. No missing patches found.\n")
        report_lines.extend(missing_report)

    final_report = "\n".join(report_lines)
    logger.info("\n" + final_report)
    send_sns_notification(final_report)


if __name__ == "__main__":
    main()
