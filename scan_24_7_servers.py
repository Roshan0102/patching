import os
import sys
import time
import logging
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# =========================================================================
# ENVIRONMENT & LOGGING INITIALIZATION
# =========================================================================
# Load environment variables from the local .env file (e.g. AWS_REGION, PATCH_BASELINE_ID)
load_dotenv()

# Configure console logging format to display timestamps and log severity levels
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# Read configuration settings from environment variables
AWS_REGION = os.getenv("AWS_REGION")
PATCH_BASELINE_ID = os.getenv("PATCH_BASELINE_ID")

# Tag filters to identify 24/7 web servers
TAG_KEY_24_7 = os.getenv("TAG_KEY_24_7")
TAG_VALUE_24_7 = os.getenv("TAG_VALUE_24_7")

# Initialize AWS Boto3 SDK Clients for EC2 and SSM
try:
    ec2 = boto3.client("ec2", region_name=AWS_REGION)
    ssm = boto3.client("ssm", region_name=AWS_REGION)
except Exception as e:
    logger.error(f"Failed to initialize AWS boto3 clients: {e}")
    sys.exit(1)


# =========================================================================
# FUNCTION 1: DISCOVER 24/7 SERVERS
# =========================================================================
def discover_24_7_servers():
    """
    Discovers all running 24/7 EC2 web servers using AWS EC2 Tags.
    Filter criteria:
      - Tag Key 'Is_24-7' = 'Yes'
      - Instance State = 'running'
    """
    logger.info("==================================================")
    logger.info("   Step 1: Discovering 24/7 Web Servers           ")
    logger.info("==================================================")
    discovered = []

    try:
        # Use Boto3 paginator to handle accounts with many EC2 instances smoothly
        paginator = ec2.get_paginator('describe_instances')
        page_iterator = paginator.paginate(
            Filters=[
                {'Name': f'tag:{TAG_KEY_24_7}', 'Values': [TAG_VALUE_24_7]},
                {'Name': 'instance-state-name', 'Values': ['running']}
            ]
        )

        # Loop through pages -> reservations -> instances
        for page in page_iterator:
            for reservation in page.get('Reservations', []):
                for instance in reservation.get('Instances', []):
                    inst_id = instance.get('InstanceId')

                    # Convert the list of tag dicts into a key-value dictionary for easy lookup
                    tags = {tag['Key']: tag['Value'] for tag in instance.get('Tags', [])}
                    name = tags.get('Name', inst_id)

                    # Append discovered instance details to the list
                    discovered.append({
                        'InstanceId': inst_id,
                        'Name': name
                    })

        # Print summary of discovered servers to the terminal
        logger.info(f"Discovered {len(discovered)} running 24/7 web server(s):")
        for server in discovered:
            logger.info(f"  - {server['Name']} ({server['InstanceId']})")

        return discovered

    except Exception as e:
        logger.error(f"Resource Discovery failed: {e}")
        sys.exit(1)


# =========================================================================
# FUNCTION 2: REGISTER DEFAULT PATCH BASELINE
# =========================================================================
def register_patch_baseline():
    """
    Registers the default AWS SSM Patch Baseline configured in the .env file.
    Ensures SSM uses the specified baseline rules for patch evaluations.
    """
    if not PATCH_BASELINE_ID:
        return

    try:
        logger.info(f"Registering Default Patch Baseline ID: {PATCH_BASELINE_ID}")
        ssm.register_default_patch_baseline(BaselineId=PATCH_BASELINE_ID)
        logger.info("Successfully registered default patch baseline.")
    except Exception as e:
        logger.warning(f"Default patch baseline registration warning: {e}")


# =========================================================================
# FUNCTION 3: CLEAR WUA DATASTORE CACHE
# =========================================================================
def clear_wua_datastore_step(instance_ids):
    """
    Clears Windows Update Agent (WUA) DataStore cache prior to scanning.
    Why this is needed:
      Prevents 'KBId: null' errors and SSM exit code 1 failures caused by
      corrupted or orphaned update catalog cache files on target servers.
    """
    if not instance_ids:
        return

    logger.info(f"Clearing WUA DataStore cache on {len(instance_ids)} instance(s)...")
    try:
        # Execute PowerShell script via SSM to stop wuauserv, remove DataStore files, and restart service
        ssm.send_command(
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
        # Give services 5 seconds to stabilize after restart
        time.sleep(5)
    except Exception as e:
        logger.warning(f"Notice: WUA DataStore cleanup SSM call warning: {e}")


# =========================================================================
# FUNCTION 4: TRIGGER SSM SCAN & GATHER MISSING PATCHES
# =========================================================================
def scan_patches(instances):
    """
    Triggers AWS-RunPatchBaseline SSM command with:
      - Operation: Scan (Only checks for missing updates, does not install)
      - RebootOption: NoReboot (Does not restart the server)
    Then polls for SSM completion and queries missing patch details per server.
    """
    if not instances:
        logger.info("No instances provided for scanning.")
        return 0, []

    # Extract instance IDs and mapping dictionary for logging
    instance_ids = [inst['InstanceId'] for inst in instances]
    id_to_name = {inst['InstanceId']: inst['Name'] for inst in instances}

    # Step A: Clean Windows Update Agent cache before scanning
    clear_wua_datastore_step(instance_ids)

    logger.info("==================================================")
    logger.info("   Step 2: SSM Patch Scan Started                 ")
    logger.info("==================================================")
    logger.info(f"Triggering SSM AWS-RunPatchBaseline (Operation: Scan, RebootOption: NoReboot) on {len(instance_ids)} instance(s)...")

    try:
        # Send SSM RunPatchBaseline command to all target instances concurrently
        response = ssm.send_command(
            InstanceIds=instance_ids,
            DocumentName="AWS-RunPatchBaseline",
            Parameters={
                "Operation": ["Scan"],
                "RebootOption": ["NoReboot"]
            },
            Comment="Automated scan for missing patches on 24/7 web servers"
        )
        command_id = response["Command"]["CommandId"]
        logger.info(f"SSM Scan Command ID: {command_id}")
    except Exception as e:
        logger.error(f"Failed to send SSM Scan command: {e}")
        sys.exit(1)

    # Step B: Poll SSM invocation status for each instance until completion
    failed_scans = []
    for inst_id in instance_ids:
        name = id_to_name[inst_id]
        elapsed = 0
        logger.info(f"Waiting for SSM scan completion on {name} ({inst_id})...")

        while True:
            try:
                invocation = ssm.get_command_invocation(CommandId=command_id, InstanceId=inst_id)
                status = invocation["Status"]

                if status == "Success":
                    logger.info(f"  * {name} ({inst_id}) -> SSM Scan Status: Success")
                    break
                elif status in ["Failed", "TimedOut", "Cancelled"]:
                    failed_scans.append(f"{name} ({inst_id}) - SSM Status: {status}")
                    logger.error(f"  * {name} ({inst_id}) -> SSM Scan Status: {status}")
                    break
            except ClientError:
                pass  # Invocation record might take a few seconds to register

            time.sleep(10)
            elapsed += 10
            if elapsed >= 1200:  # 20 minutes timeout
                failed_scans.append(f"{name} ({inst_id}) - Wait timeout")
                break

    # If any server failed during SSM scan, abort script and log error
    if failed_scans:
        logger.error("SSM Scan failed to complete on the following instances:\n" + "\n".join(failed_scans))
        sys.exit(1)

    # Step C: Wait 15 seconds to ensure AWS SSM compliance database synchronizes
    logger.info("SSM Scan execution complete. Syncing compliance database (15s)...")
    time.sleep(15)

    # Step D: Query AWS SSM describe_instance_patches API for missing KB IDs
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

            # Build report entry for this server
            if missing_list:
                total_missing += len(missing_list)
                missing_patches_report.append(f"- {name} ({inst_id}):\n" + "\n".join(missing_list))
            else:
                missing_patches_report.append(f"- {name} ({inst_id}): No missing patches found.")
        except Exception as e:
            logger.error(f"Failed to retrieve patches for {name} ({inst_id}): {e}")

    return total_missing, missing_patches_report


# =========================================================================
# MAIN EXECUTION FLOW
# =========================================================================
def main():
    logger.info("==================================================")
    logger.info("   WalletHR: 24/7 Web Servers Patch Scan Script   ")
    logger.info("==================================================")

    # Step 1: Discover 24/7 Servers tagged with Is_24-7 = Yes
    discovered_servers = discover_24_7_servers()

    if not discovered_servers:
        logger.info("No running 24/7 web servers discovered. Exiting.")
        sys.exit(0)

    # Step 2: Register Default Patch Baseline if configured
    register_patch_baseline()

    # Step 3: Execute SSM Scan (NoReboot) and collect missing patch details
    total_missing, missing_report = scan_patches(discovered_servers)

    # Step 4: Display final scan results in the terminal
    logger.info("==================================================")
    logger.info("   Step 3: Final Patch Scan Results Summary       ")
    logger.info("==================================================")

    if total_missing > 0:
        logger.info(f"Scan Result: Total {total_missing} missing patch(es) found across fleet. Action required.\n")
        logger.info("Missing Patches Detail:")
        for line in missing_report:
            print(line)
    else:
        logger.info("Scan Result: All 24/7 web servers are fully compliant. No missing patches found.\n")
        for line in missing_report:
            print(line)

    logger.info("==================================================")
    logger.info("   24/7 Web Servers Patch Scan Completed!         ")
    logger.info("==================================================")


# Entry point guard
if __name__ == "__main__":
    main()
