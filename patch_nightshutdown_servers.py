#!/usr/bin/env python3
import os
import time
import logging
import sys
import datetime
import boto3
from botocore.exceptions import ClientError

# =========================================================================
# CONFIGURATION & CONSTANTS LOAD
# =========================================================================
def load_env_file(filepath=".env"):
    """
    Loads environment variables from a .env file.
    If the file is not found, halts execution immediately.
    """
    if not os.path.exists(filepath):
        print(f"CRITICAL ERROR: Configuration file '{filepath}' was not found. Halting execution.")
        sys.exit(1)

    with open(filepath, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, val = line.split("=", 1)
                os.environ[key.strip()] = val.strip()

# Load env variables at start
load_env_file()

REGION = os.getenv("AWS_REGION", "ap-south-1")
PATCH_BASELINE_ID = os.getenv("PATCH_BASELINE_ID", "pb-0123456789abcdef0")
SNS_ENABLED = os.getenv("SNS_ENABLED", "True").lower() == "true"
SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN", "")
TAG_KEY_24_7 = os.getenv("TAG_KEY_24_7", "Is_24-7")
TAG_VALUE_24_7 = os.getenv("TAG_VALUE_24_7", "Yes")
TAG_VALUE_STANDBY = os.getenv("TAG_VALUE_STANDBY", "No")

# Logging setup
LOG_DIR = "night_shutdown_servers_logs"
os.makedirs(LOG_DIR, exist_ok=True)
timestamp_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = os.path.join(LOG_DIR, f"patch_nightshutdown_{timestamp_str}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("PatchNightShutdownWebServers")
logger.info(f"Logging initialized. Log file saved to: {log_filename}")

# Initialize AWS clients using loaded configurations
ec2 = boto3.client("ec2", region_name=REGION)
elbv2 = boto3.client("elbv2", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
sns = boto3.client("sns", region_name=REGION)

# =========================================================================
# CUSTOM EXCEPTION FOR FAILURE HANDLING
# =========================================================================
class StepFailure(Exception):
    def __init__(self, step_name, message):
        super().__init__(message)
        self.step_name = step_name
        self.message = message

# =========================================================================
# SNS NOTIFICATION HELPER
# =========================================================================
def send_sns_notification(message, subject="WalletHR NightShutdown Servers Patching Update"):
    """
    Sends a consolidated status message to the L1 operations team via SNS.
    """
    if not SNS_ENABLED or not SNS_TOPIC_ARN:
        logger.info(f"[SNS Disabled/No ARN] {message}")
        return

    try:
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=message,
            Subject=subject
        )
        logger.info(f"SNS notification sent successfully: \n{message}")
    except Exception as e:
        logger.error(f"Failed to publish SNS notification: {e}")

# =========================================================================
# STEP 1: DISCOVER INSTANCES
# =========================================================================
def discover_resources_step():
    logger.info("Starting Step 1: Resource Discovery...")
    groups = {}

    try:
        # 1. Discover all EC2 instances with the Is_24-7 tag key
        ec2_response = ec2.describe_instances(
            Filters=[
                {"Name": "tag-key", "Values": [TAG_KEY_24_7]}
            ]
        )
        
        discovered_instances = {}
        for reservation in ec2_response.get("Reservations", []):
            for instance in reservation.get("Instances", []):
                if instance.get("State", {}).get("Name") == "terminated":
                    continue
                
                instance_id = instance["InstanceId"]
                tags = {t["Key"]: t["Value"] for t in instance.get("Tags", [])}
                name = tags.get("Name", instance_id)
                is_24_7_val = tags.get(TAG_KEY_24_7)
                
                discovered_instances[instance_id] = {
                    "InstanceId": instance_id,
                    "Name": name,
                    "Is_24_7": is_24_7_val
                }

        # 2. Discover all Target Groups and find which of our discovered instances are registered
        tg_response = elbv2.describe_target_groups()
        for tg in tg_response.get("TargetGroups", []):
            tg_arn = tg["TargetGroupArn"]
            tg_name = tg["TargetGroupName"]

            try:
                health_resp = elbv2.describe_target_health(TargetGroupArn=tg_arn)
                for desc in health_resp.get("TargetHealthDescriptions", []):
                    target_id = desc["Target"]["Id"]
                    
                    if target_id in discovered_instances:
                        inst_info = discovered_instances[target_id]
                        is_24_7_val = inst_info["Is_24_7"]
                        
                        if tg_arn not in groups:
                            groups[tg_arn] = {
                                "24_7": None,
                                "NightShutdown": None,
                                "TargetGroupARN": tg_arn,
                                "TargetGroupName": tg_name
                            }
                        
                        if is_24_7_val == TAG_VALUE_24_7:
                            groups[tg_arn]["24_7"] = {
                                "InstanceId": target_id,
                                "Name": inst_info["Name"]
                            }
                        elif is_24_7_val == TAG_VALUE_STANDBY:
                            groups[tg_arn]["NightShutdown"] = {
                                "InstanceId": target_id,
                                "Name": inst_info["Name"]
                            }
            except ClientError as ce:
                logger.warning(f"Could not describe target health for {tg_name}: {ce}")

        # Validate discovered resources: each Target Group must have a 24/7 server and a Standby server
        validated_groups = {}
        for tg_arn, details in groups.items():
            if details["24_7"] and details["NightShutdown"]:
                validated_groups[details["TargetGroupName"]] = details
            else:
                missing = []
                if not details["24_7"]: missing.append("24/7 Server")
                if not details["NightShutdown"]: missing.append("Night Shutdown Server")
                logger.warning(f"Target Group '{details['TargetGroupName']}' is missing: {', '.join(missing)}. Skipping.")

        if not validated_groups:
            raise StepFailure("Step 1: Discover Instances", "No valid target group pairs found with both 24/7 and Standby servers tagged.")

        # Build and send discovery SNS notification
        msg_lines = ["Step 1: Discover Instances\nSuccessfully discovered the following servers:\n"]
        msg_lines.append("24/7 Servers:")
        for tg_name, details in sorted(validated_groups.items()):
            msg_lines.append(f"- {details['24_7']['Name']} ({details['24_7']['InstanceId']}) [Target Group: {tg_name}]")
        
        msg_lines.append("\nNight Shutdown (Standby) Servers (Expected to be Stopped):")
        for tg_name, details in sorted(validated_groups.items()):
            msg_lines.append(f"- {details['NightShutdown']['Name']} ({details['NightShutdown']['InstanceId']}) [Target Group: {tg_name}]")

        send_sns_notification("\n".join(msg_lines))
        return validated_groups

    except StepFailure:
        raise
    except Exception as e:
        raise StepFailure("Step 1: Discover Instances", f"Unexpected failure during discovery: {e}")

# =========================================================================
# SSM PATCH MANAGER SETUP
# =========================================================================
def setup_ssm_patch_groups():
    logger.info(f"Setting up Default Patch Baseline ID: {PATCH_BASELINE_ID}")
    try:
        ssm.register_default_patch_baseline(BaselineId=PATCH_BASELINE_ID)
        logger.info("Successfully registered default patch baseline.")
        return PATCH_BASELINE_ID
    except Exception as e:
        raise StepFailure("SSM Setup", f"Failed to register default patch baseline: {e}")

# =========================================================================
# STEP 2: START NIGHT SHUTDOWN SERVERS
# =========================================================================
def start_nightshutdown_step(groups_dict):
    logger.info("Starting Step 2: Starting Night Shutdown instances...")
    instance_ids = [details["NightShutdown"]["InstanceId"] for details in groups_dict.values()]
    id_to_name = {details["NightShutdown"]["InstanceId"]: details["NightShutdown"]["Name"] for details in groups_dict.values()}
    targets_to_wait = [(details["TargetGroupARN"], details["NightShutdown"]["InstanceId"], details["NightShutdown"]["Name"]) for details in groups_dict.values()]

    try:
        ec2.start_instances(InstanceIds=instance_ids)
    except Exception as e:
        raise StepFailure("Step 2: Start Night Shutdown Servers", f"Failed to trigger instance start: {e}")

    # Wait for running
    running_instances = set()
    failed_instances = []

    for inst_id in instance_ids:
        name = id_to_name[inst_id]
        elapsed = 0
        while True:
            try:
                resp = ec2.describe_instances(InstanceIds=[inst_id])
                state = resp["Reservations"][0]["Instances"][0]["State"]["Name"]
                if state == "running":
                    running_instances.add(inst_id)
                    break
                time.sleep(10)
                elapsed += 10
                if elapsed >= 300:
                    failed_instances.append(f"{name} ({inst_id}) - Timeout waiting for running state")
                    break
            except Exception as e:
                failed_instances.append(f"{name} ({inst_id}) - Error: {e}")
                break

    if failed_instances:
        raise StepFailure("Step 2: Start Night Shutdown Servers", "\n".join(failed_instances))

    # Wait for status checks
    failed_status_checks = []
    for inst_id in instance_ids:
        name = id_to_name[inst_id]
        elapsed = 0
        while True:
            try:
                resp = ec2.describe_instance_status(InstanceIds=[inst_id])
                statuses = resp.get("InstanceStatuses", [])
                
                inst_ok = False
                sys_ok = False
                ebs_ok = True

                if statuses:
                    inst_ok = statuses[0]["InstanceStatus"]["Status"] == "ok"
                    sys_ok = statuses[0]["SystemStatus"]["Status"] == "ok"
                    if "AttachedVolumeStatus" in statuses[0]:
                        ebs_ok = statuses[0]["AttachedVolumeStatus"]["Status"] in ["ok", "not-applicable"]

                if inst_ok and sys_ok and ebs_ok:
                    break
                time.sleep(15)
                elapsed += 15
                if elapsed >= 600:
                    failed_status_checks.append(f"{name} ({inst_id}) - Status checks wait timeout")
                    break
            except Exception as e:
                failed_status_checks.append(f"{name} ({inst_id}) - Status checks check failed: {e}")
                break

    if failed_status_checks:
        raise StepFailure("Step 2: Start Night Shutdown Servers", "\n".join(failed_status_checks))

    # Wait for ALB health checks
    failed_alb_health = []
    for tg_arn, inst_id, name in targets_to_wait:
        elapsed = 0
        while True:
            try:
                health_resp = elbv2.describe_target_health(TargetGroupArn=tg_arn, Targets=[{"Id": inst_id}])
                health_states = health_resp.get("TargetHealthDescriptions", [])
                if health_states:
                    state = health_states[0]["TargetHealth"]["State"]
                    if state == "healthy":
                        break
                time.sleep(10)
                elapsed += 10
                if elapsed >= 300:
                    failed_alb_health.append(f"{name} ({inst_id}) - ALB Health wait timeout")
                    break
            except Exception as e:
                failed_alb_health.append(f"{name} ({inst_id}) - ALB Health check failed: {e}")
                break

    if failed_alb_health:
        raise StepFailure("Step 2: Start Night Shutdown Servers", "\n".join(failed_alb_health))

    # Send success SNS
    msg_lines = ["Step 2: Start Night Shutdown Servers\nSuccessfully started and verified healthy instances:\n"]
    for inst_id in instance_ids:
        msg_lines.append(f"- {id_to_name[inst_id]} ({inst_id})")
    send_sns_notification("\n".join(msg_lines))

# =========================================================================
# STEP 3: SCAN THE NIGHT SHUTDOWN SERVERS FOR MISSING PATCHES
# =========================================================================
def scan_patches_step(groups_dict):
    logger.info("Starting Step 3: Scanning Night Shutdown servers for missing patches...")
    instance_ids = [details["NightShutdown"]["InstanceId"] for details in groups_dict.values()]
    id_to_name = {details["NightShutdown"]["InstanceId"]: details["NightShutdown"]["Name"] for details in groups_dict.values()}
    
    try:
        response = ssm.send_command(
            InstanceIds=instance_ids,
            DocumentName="AWS-RunPatchBaseline",
            Parameters={
                "Operation": ["Scan"],
                "RebootOption": ["NoReboot"]
            },
            Comment="Automated pre-patching scan on Night Shutdown servers"
        )
        command_id = response["Command"]["CommandId"]
    except Exception as e:
        raise StepFailure("Step 3: Scan for Missing Patches", f"Failed to send SSM Scan command: {e}")

    # Wait for scan command completion
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
            time.sleep(15)
            elapsed += 15
            if elapsed >= 1200:
                failed_scans.append(f"{name} ({inst_id}) - Wait timeout")
                break

    if failed_scans:
        raise StepFailure(
            "Step 3: Scan for Missing Patches",
            "SSM Scan failed to complete on the following instances:\n" + "\n".join(failed_scans)
        )

    # Retrieve missing patches list
    missing_patches_report = []
    total_missing = 0
    groups_to_patch = {}
    inst_to_grp = {details["NightShutdown"]["InstanceId"]: grp for grp, details in groups_dict.items()}

    for inst_id in instance_ids:
        name = id_to_name[inst_id]
        grp = inst_to_grp[inst_id]
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
                groups_to_patch[grp] = groups_dict[grp]
            else:
                logger.info(f"Group '{grp}': {name} ({inst_id}) has 0 missing patches. Skipping patching for this group.")
                missing_patches_report.append(f"- {name} ({inst_id}): No missing patches found.")
        except Exception as e:
            raise StepFailure("Step 3: Scan for Missing Patches", f"Failed to retrieve patches for {name} ({inst_id}): {e}")

    return total_missing, missing_patches_report, groups_to_patch

# =========================================================================
# STEP 4: DEREGISTER THE NIGHT SHUTDOWN SERVERS FROM THEIR TARGET GROUPS
# =========================================================================
def deregister_targets_step(groups_dict):
    logger.info("Starting Step 4: Deregistering Night Shutdown servers from Target Groups...")
    targets_to_deregister = []

    for grp, details in groups_dict.items():
        inst = details["NightShutdown"]
        tg_arn = details["TargetGroupARN"]
        targets_to_deregister.append((tg_arn, inst["InstanceId"], inst["Name"]))

    for tg_arn, inst_id, name in targets_to_deregister:
        try:
            elbv2.deregister_targets(TargetGroupArn=tg_arn, Targets=[{"Id": inst_id}])
        except Exception as e:
            raise StepFailure("Step 4: Deregister Night Shutdown Servers", f"Failed to deregister {name} ({inst_id}): {e}")

    # Wait for unused (drained)
    failed_draining = []
    for tg_arn, inst_id, name in targets_to_deregister:
        elapsed = 0
        while True:
            try:
                # Query all targets registered in the target group
                health_resp = elbv2.describe_target_health(TargetGroupArn=tg_arn)
                health_descriptions = health_resp.get("TargetHealthDescriptions", [])

                # Look for our specific target in the descriptions
                target_found = None
                for desc in health_descriptions:
                    if desc["Target"]["Id"] == inst_id:
                        target_found = desc
                        break

                # If the target is not found at all, it has successfully drained and vanished
                if not target_found:
                    logger.info(f"Target {name} ({inst_id}) is no longer registered in target group (fully drained).")
                    break

                # If it's found, check if its state has transitioned to 'unused'
                state = target_found["TargetHealth"]["State"]
                if state == "unused":
                    logger.info(f"Target {name} ({inst_id}) status is 'unused' (fully drained).")
                    break

                # Otherwise, it is still active/draining. Sleep and check again.
                time.sleep(30)
                elapsed += 30
                if elapsed >= 600: # 10 minutes timeout
                    failed_draining.append(f"{name} ({inst_id}) - Timeout waiting for deregistration (current state: {state})")
                    break
            except Exception as e:
                failed_draining.append(f"{name} ({inst_id}) - Failed describing health: {e}")
                break

    if failed_draining:
        raise StepFailure("Step 4: Deregister Night Shutdown Servers", "\n".join(failed_draining))

    # Send Success SNS
    msg_lines = ["Step 4: Deregister Night Shutdown Servers\nSuccessfully deregistered and drained targets:\n"]
    for tg_arn, inst_id, name in targets_to_deregister:
        msg_lines.append(f"- {name} ({inst_id})")
    send_sns_notification("\n".join(msg_lines))

# =========================================================================
# STEP 5 & 7: CREATE AMI BACKUPS
# =========================================================================
def create_backup_ami_step(groups_dict, prefix, step_num, step_name):
    logger.info(f"Starting Step {step_num}: Creating AMI backups ({prefix})...")
    amis_to_wait = []
    date_str = datetime.datetime.now().strftime("%d-%m-%Y")

    for grp, details in groups_dict.items():
        inst = details["NightShutdown"]
        name = inst["Name"]
        inst_id = inst["InstanceId"]
        # Format: Prod-Grp6-WebServer1-Before-Patching-12-06-2026
        ami_name = f"{name}-{prefix}-{date_str}"

        try:
            resp = ec2.create_image(
                InstanceId=inst_id,
                Name=ami_name,
                NoReboot=True,
                Description=f"{prefix} backup for {name}"
            )
            ami_id = resp["ImageId"]
            amis_to_wait.append((ami_id, ami_name, inst_id, name))
        except Exception as e:
            raise StepFailure(step_name, f"Failed to initiate AMI for {name} ({inst_id}): {e}")

    # Wait for available
    failed_amis = []
    for ami_id, ami_name, inst_id, name in amis_to_wait:
        elapsed = 0
        while True:
            try:
                images = ec2.describe_images(ImageIds=[ami_id])
                state = images["Images"][0]["State"]
                if state == "available":
                    break
                elif state == "failed":
                    failed_amis.append(f"{name} ({inst_id}) - AMI creation failed in AWS")
                    break
                time.sleep(20)
                elapsed += 20
                if elapsed >= 1200:
                    failed_amis.append(f"{name} ({inst_id}) - AMI creation timeout")
                    break
            except Exception as e:
                failed_amis.append(f"{name} ({inst_id}) - Wait failed: {e}")
                break

    if failed_amis:
        raise StepFailure(step_name, "\n".join(failed_amis))

    # Send Success SNS
    msg_lines = [f"Step {step_num}: Pre-patching AMI Backups Available\n" if prefix == "Before-Patching" else f"Step {step_num}: Post-patching AMI Backups Available\n"]
    msg_lines.append("Backup Details:")
    for ami_id, ami_name, inst_id, name in amis_to_wait:
        msg_lines.append(f"- Instance Name: {name}\n  Instance ID: {inst_id}\n  AMI Name: {ami_name}\n  AMI ID: {ami_id}\n")
    send_sns_notification("\n".join(msg_lines))

# =========================================================================
# STEP 6: PATCH THE NIGHT SHUTDOWN SERVERS
# =========================================================================
def patch_servers_step(groups_dict):
    logger.info("Starting Step 6: Patching Night Shutdown servers...")
    instance_ids = [details["NightShutdown"]["InstanceId"] for details in groups_dict.values()]
    id_to_name = {details["NightShutdown"]["InstanceId"]: details["NightShutdown"]["Name"] for details in groups_dict.values()}
    
    try:
        response = ssm.send_command(
            InstanceIds=instance_ids,
            DocumentName="AWS-RunPatchBaseline",
            Parameters={
                "Operation": ["Install"],
                "RebootOption": ["RebootIfNeeded"]
            },
            TimeoutSeconds=5400,
            Comment="Automated patching execution for Night Shutdown servers"
        )
        command_id = response["Command"]["CommandId"]
    except Exception as e:
        raise StepFailure("Step 6: Patch Night Shutdown Servers", f"Failed to trigger SSM patch execution: {e}")

    # Wait for completion
    failed_patching = []
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
                    failed_patching.append(f"{name} ({inst_id}) - SSM Status: {status}")
                    break
            except ClientError:
                pass
            time.sleep(15)
            elapsed += 15
            if elapsed >= 5400:
                failed_patching.append(f"{name} ({inst_id}) - Execution wait timeout")
                break

    if failed_patching:
        raise StepFailure("Step 6: Patch Night Shutdown Servers", "Patch execution failed on instances:\n" + "\n".join(failed_patching))

    # Send Success SNS
    msg_lines = ["Step 6: Patch Night Shutdown Servers\nSuccessfully applied patches and rebooted instances:\n"]
    for inst_id in instance_ids:
        msg_lines.append(f"- {id_to_name[inst_id]} ({inst_id})")
    send_sns_notification("\n".join(msg_lines))

# =========================================================================
# STEP 8: RE-REGISTER THE NIGHT SHUTDOWN SERVERS WITH THEIR TARGET GROUPS
# =========================================================================
def register_targets_step(groups_dict):
    logger.info("Starting Step 8: Re-registering patched Night Shutdown servers back to ALB...")
    targets_to_register = []

    for grp, details in groups_dict.items():
        inst = details["NightShutdown"]
        tg_arn = details["TargetGroupARN"]
        targets_to_register.append((tg_arn, inst["InstanceId"], inst["Name"]))

    for tg_arn, inst_id, name in targets_to_register:
        try:
            elbv2.register_targets(TargetGroupArn=tg_arn, Targets=[{"Id": inst_id}])
        except Exception as e:
            raise StepFailure("Step 8: Re-register Night Shutdown Servers", f"Failed to register {name} ({inst_id}): {e}")

    # Wait for healthy
    failed_health = []
    for tg_arn, inst_id, name in targets_to_register:
        elapsed = 0
        while True:
            try:
                health_resp = elbv2.describe_target_health(TargetGroupArn=tg_arn, Targets=[{"Id": inst_id}])
                health_states = health_resp.get("TargetHealthDescriptions", [])
                if health_states:
                    state = health_states[0]["TargetHealth"]["State"]
                    if state == "healthy":
                        break
                time.sleep(10)
                elapsed += 10
                if elapsed >= 300:
                    failed_health.append(f"{name} ({inst_id}) - Timeout waiting for healthy state (current: {state if health_states else 'pending'})")
                    break
            except Exception as e:
                failed_health.append(f"{name} ({inst_id}) - Health check failed: {e}")
                break

    if failed_health:
        raise StepFailure("Step 8: Re-register Night Shutdown Servers", "\n".join(failed_health))

    # Send Success SNS
    msg_lines = ["Step 8: Re-register Night Shutdown Servers\nSuccessfully registered and verified healthy targets:\n"]
    for tg_arn, inst_id, name in targets_to_register:
        msg_lines.append(f"- {name} ({inst_id})")
    send_sns_notification("\n".join(msg_lines))

# =========================================================================
# STEP 9: STOP THE NIGHT SHUTDOWN INSTANCES
# =========================================================================
def stop_nightshutdown_step(groups_dict):
    logger.info("Starting Step 9: Stopping Night Shutdown instances...")
    instance_ids = [details["NightShutdown"]["InstanceId"] for details in groups_dict.values()]
    id_to_name = {details["NightShutdown"]["InstanceId"]: details["NightShutdown"]["Name"] for details in groups_dict.values()}

    try:
        ec2.stop_instances(InstanceIds=instance_ids)
    except Exception as e:
        raise StepFailure("Step 9: Stop Night Shutdown Instances", f"Failed to trigger stop: {e}")

    # Wait for stopped
    failed_stop = []
    for inst_id in instance_ids:
        name = id_to_name[inst_id]
        elapsed = 0
        while True:
            try:
                resp = ec2.describe_instances(InstanceIds=[inst_id])
                state = resp["Reservations"][0]["Instances"][0]["State"]["Name"]
                if state == "stopped":
                    break
                time.sleep(10)
                elapsed += 10
                if elapsed >= 300:
                    failed_stop.append(f"{name} ({inst_id}) - Timeout waiting for stopped state")
                    break
            except Exception as e:
                failed_stop.append(f"{name} ({inst_id}) - State check failed: {e}")
                break

    if failed_stop:
        raise StepFailure("Step 9: Stop Night Shutdown Instances", "\n".join(failed_stop))

    # Send Success SNS
    msg_lines = ["Step 9: Stop Night Shutdown Instances\nSuccessfully stopped standby instances:\n"]
    for inst_id in instance_ids:
        msg_lines.append(f"- {id_to_name[inst_id]} ({inst_id})")
    send_sns_notification("\n".join(msg_lines))

# =========================================================================
# STEP 10: SEND FINAL COMPLETION NOTIFICATION
# =========================================================================
def send_final_notification(groups_dict):
    msg_lines = ["Step 10: Centralized Patching Complete\nAll patching steps successfully completed for the following Night Shutdown servers:\n"]
    for grp, details in sorted(groups_dict.items()):
        inst = details["NightShutdown"]
        msg_lines.append(f"- {inst['Name']} ({inst['InstanceId']})")
    send_sns_notification("\n".join(msg_lines))

# =========================================================================
# WORKFLOW EXECUTION
# =========================================================================
def main():
    logger.info("==================================================")
    logger.info("   WalletHR: Patching NightShutdown Servers       ")
    logger.info("==================================================")

    workflow_name = "Patch NightShutdown Servers"
    discovered_groups = {}

    try:
        # Step 1: Discover the instances
        discovered_groups = discover_resources_step()

        # Setup baseline default
        setup_ssm_patch_groups()

        # Step 2: Start all Night Shutdown servers (required to scan them)
        start_nightshutdown_step(discovered_groups)

        # Step 3: Scan Night Shutdown servers for missing patches
        total_missing, missing_report, groups_to_patch = scan_patches_step(discovered_groups)
        if total_missing > 0 and groups_to_patch:
            msg_lines = ["Step 3: Scan for Missing Patches - Missing patches found:\n"]
            msg_lines.extend(missing_report)
            send_sns_notification("\n".join(msg_lines))
        else:
            send_sns_notification("Step 3: Scan for Missing Patches - No missing patches found on any Night Shutdown server. Stopping instances and exiting.")
            logger.info("No missing patches found on any server. Stopping instances...")
            stop_nightshutdown_step(discovered_groups)
            logger.info("Instances stopped. Terminating execution.")
            sys.exit(0)

        # Batching Setup - process only groups with missing patches
        group_keys = list(groups_to_patch.keys())
        batches = []
        max_batch_size = int(os.getenv("MAX_CONCURRENT_BATCHES", "3"))
        batch_delay = int(os.getenv("BATCH_DELAY_SECONDS", "30"))

        for i in range(0, len(group_keys), max_batch_size):
            batch_keys = group_keys[i:i + max_batch_size]
            batch_dict = {k: groups_to_patch[k] for k in batch_keys}
            batches.append(batch_dict)

        logger.info(f"Total target groups split into {len(batches)} batch(es) (Size: {max_batch_size}).")

        # Process batch by batch
        for batch_index, batch_groups in enumerate(batches, start=1):
            logger.info(f"--- Processing Batch {batch_index}/{len(batches)}: {list(batch_groups.keys())} ---")

            # Step 4: Deregister Night Shutdown servers from their target groups for this batch
            deregister_targets_step(batch_groups)

            # Step 5: Create AMI backups before patching for this batch
            create_backup_ami_step(batch_groups, "Before-Patching", 5, "Step 5: Create Pre-Patch AMI Backup")

            # Step 6: Patch the Night Shutdown servers for this batch
            patch_servers_step(batch_groups)

            # Step 7: Create AMI backups after patching for this batch
            create_backup_ami_step(batch_groups, "After-Patching", 7, "Step 7: Create Post-Patch AMI Backup")

            # Step 8: Re-register the Night Shutdown servers with their target groups for this batch
            register_targets_step(batch_groups)

            # Step 9: Stop the Night Shutdown instances for this batch
            stop_nightshutdown_step(batch_groups)

            # Cool-down delay between batches
            if batch_index < len(batches):
                logger.info(f"Cool-down: waiting {batch_delay} seconds before starting the next batch...")
                time.sleep(batch_delay)

        # Step 10: Send the final completion notification
        send_final_notification(discovered_groups)

    except StepFailure as sf:
        error_msg = f"Workflow Error: {workflow_name} Failed.\n" \
                    f"Failed Step: {sf.step_name}\n" \
                    f"Reason: {sf.message}"
        logger.error(error_msg)
        send_sns_notification(error_msg, subject="WalletHR NightShutdown Servers Patching FAILURE")
        sys.exit(1)
    except Exception as e:
        error_msg = f"Workflow Error: {workflow_name} Failed.\n" \
                    f"Failed Step: Unknown (Unexpected General Error)\n" \
                    f"Reason: {e}"
        logger.error(error_msg)
        send_sns_notification(error_msg, subject="WalletHR NightShutdown Servers Patching FAILURE")
        sys.exit(1)

if __name__ == "__main__":
    main()
