"""ELB scheduler."""

from typing import List, Dict, Any
import re

import boto3
from botocore.exceptions import ClientError

from .exceptions import elb_exception


class ElbScheduler:
    """Abstract Valkey scheduler in a class."""

    # Normalization schema for common parameters
    _FIELD_TYPES = {
        # bool
        "EnableDeletionProtection": bool,
        "EnableHttp2": bool,
        "EnableWafFailOpen": bool,
        "IsDefault": bool,
        # int
        "IdleTimeoutSeconds": int,
        "Port": int,
        # list
        "SecurityGroups": list,
        "Subnets": list,
        "SubnetMappings": list,
        "Tags": list,
        # dict
        "FixedResponseConfig": dict,
    }

    def __init__(self, region_name=None) -> None:
        """Initialize ELBv2 service."""
        if region_name:
            self.elbv2 = boto3.client("elbv2", region_name=region_name)
            self.route53 = boto3.client("route53", region_name=region_name)
            self.cloudwatch = boto3.client("cloudwatch", region_name=region_name)
        else:
            self.elbv2 = boto3.client("elbv2")
            self.route53 = boto3.client("route53")
            self.cloudwatch = boto3.client("cloudwatch")


    # internal helpers
    @classmethod
    def _normalize_load_balancer(cls, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize JSON-derived config to boto3-friendly types."""
        config = dict(raw)

        # add some default values
        config.setdefault("Type", "application")
        config.setdefault("Scheme", "internet-facing")
        config.setdefault("IpAddressType", "ipv4")

        attributes = config.get("Attributes", [])
        attributes.append({
            "Key": "deletion_protection.enabled",
            "Value": "false"
        })
        attributes.append({
            "Key": "access_logs.s3.enabled",
            "Value": "true"
        })
        config["Attributes"] = attributes


        return cls._normalize_dict(config)


    @staticmethod
    def _cast_value(value: Any, expected_type: type) -> Any:
        if expected_type is bool:
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.strip().lower() == "true"
            raise ValueError(f"Invalid boolean value: {value}")

        if expected_type is list:
            if isinstance(value, dict):
                return [{"Key": k, "Value": str(v)} for k, v in value.items()]
            if isinstance(value, list):
                return value
            if isinstance(value, str):
                return [v.strip() for v in value.split(",") if v.strip()]
            raise ValueError(f"Invalid list value: {value}")

        try:
            return expected_type(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Cannot cast value '{value}' to {expected_type.__name__}") from exc

    @staticmethod
    def _is_null(value: Any) -> bool:
        """Check for Terraform null values (None or string "null")."""
        if value is None:
            return True
        if isinstance(value, str):
            return value.strip().lower() in ("null", "")
        return False

    @staticmethod
    def _normalize_dict(data: Dict[str, Any], parent_context: str = "") -> Dict[str, Any]:
        """Recursively normalize dict with better context awareness."""
        if not isinstance(data, dict):
            return data

        normalized = {}
        current_context = parent_context

        for key, value in data.items():
            if ElbScheduler._is_null(value):
                continue

            # Determine context for nested structures
            if key in ("RedirectConfig", "FixedResponseConfig", "ForwardConfig"):
                current_context = key

            if key == "IsDefault":
                normalized[key] = ElbScheduler._cast_value(value, bool)

            elif key == "Priority":
                normalized[key] = ElbScheduler._cast_value(value, int)

            # Special handling for Port based on context
            elif key == "Port":
                if current_context == "RedirectConfig":
                    normalized[key] = str(value)          # Must be string for RedirectConfig
                else:
                    normalized[key] = ElbScheduler._cast_value(value, int)  # Listener Port

            elif key in ElbScheduler._FIELD_TYPES:
                normalized[key] = ElbScheduler._cast_value(value, ElbScheduler._FIELD_TYPES[key])

            elif isinstance(value, list):
                normalized[key] = [
                    ElbScheduler._normalize_value(item, current_context) for item in value
                ]
            elif isinstance(value, dict):
                normalized[key] = ElbScheduler._normalize_dict(value, current_context)
            else:
                normalized[key] = value

        return normalized

    @staticmethod
    def _normalize_value(value: Any, parent_context: str = "") -> Any:
        """Helper for normalizing list items (DefaultActions, Certificates, etc.)."""
        if isinstance(value, dict):
            return ElbScheduler._normalize_dict(value, parent_context)
        return value

    @staticmethod
    def _clean_none_values(data: Any) -> Any:
        """Recursively clean null values."""
        if isinstance(data, dict):
            return {
                k: ElbScheduler._clean_none_values(v)
                for k, v in data.items()
                if not ElbScheduler._is_null(v)
            }
        elif isinstance(data, list):
            return [ElbScheduler._clean_none_values(item) for item in data if not ElbScheduler._is_null(item)]
        else:
            return data

    def update_cloudwatch_alarms(self, old_pattern: str, new_lb_dimension_value: str):
        """Update CloudWatch alarms by replacing LoadBalancer dimension value.

        old_pattern: regex to match old dimension (e.g. r'app/my-app-prod/.*')
        new_lb_dimension_value: Full new dimension value like 'app/my-app-prod-new/abc123def456'
        """
        try:
            updated_count = 0
            paginator = self.cloudwatch.get_paginator('describe_alarms')

            # Valid parameters for put_metric_alarm, and without Dimensions, b/c we are going to update it (if matches)
            valid_params = {
                "AlarmName", "AlarmDescription", "ActionsEnabled", "OKActions",
                "AlarmActions", "InsufficientDataActions", "MetricName", "Namespace",
                "Statistic", "ExtendedStatistic", "Period", "Unit",
                "EvaluationPeriods", "DatapointsToAlarm", "Threshold",
                "ComparisonOperator", "TreatMissingData", "EvaluateLowSampleCountPercentile",
                "Metrics", "Tags", "ThresholdMetricId", "EvaluationCriteria",
                "EvaluationInterval"
            }

            for page in paginator.paginate():
                for alarm in page.get('MetricAlarms', []):
                    alarm_name = alarm['AlarmName']
                    dimensions = alarm.get('Dimensions', [])

                    lb_dim_index = None
                    old_value = None

                    for i, dim in enumerate(dimensions):
                        if dim.get('Name') == 'LoadBalancer':
                            lb_dim_index = i
                            old_value = dim.get('Value')
                            break

                    if lb_dim_index is None or not old_value:
                        continue

                    if not re.search(old_pattern, old_value):
                        continue

                    # Replace with new full dimension value
                    new_dimensions = dimensions.copy()
                    new_dimensions[lb_dim_index] = {
                        'Name': 'LoadBalancer',
                        'Value': new_lb_dimension_value
                    }

                    try:
                        # Filter only valid parameters
                        alarm_config = {
                            k: v for k, v in alarm.items()
                            if k in valid_params
                        }

                        self.cloudwatch.put_metric_alarm(
                            **alarm_config,
                            Dimensions=new_dimensions
                        )

                        print(f"Updated CloudWatch alarm: {alarm_name}")
                        print(f"  LoadBalancer: {old_value} → {new_lb_dimension_value}")
                        updated_count += 1

                    except ClientError as exc:
                        elb_exception("CloudWatch Alarm Update", alarm_name, exc)

            print(f"Finished updating CloudWatch alarms. Total updated: {updated_count}")

        except ClientError as exc:
            elb_exception("CloudWatch Alarms", "describe_alarms", exc)


    def stop(self, elb_names: List[str]) -> None:
        """Aws ELBv2 load balancer stop (disable) function.

        Since we cannot stop ELBv2 Load Balancer, we delete it.

        This includes listeners

        :param list elb_names:
            List of ELBv2 load balancers to stop (delete).
            For example:
            ['core-staging']
        """
        for lb_name in elb_names:
            try:
                # boto3 describe_load_balancer accepts either Name or LoadBalancerArn, but we rely on name only
                describe = self.elbv2.describe_load_balancers(Names=[lb_name])
                if not describe.get('LoadBalancers'):
                    raise ClientError({"Error": {"Code": "LoadBalancerNotFound"}}, "describe_load_balancers")
                arn = describe['LoadBalancers'][0]['LoadBalancerArn']
                delete = self.elbv2.delete_load_balancer(LoadBalancerArn=arn)

                print(delete)

                print(f"ELBv2 Load Balancer {lb_name} disabled (deleted).")
            except ClientError as exc:
                elb_exception("ELBv2 Load Balancer", lb_name, exc)

    def start(self, elb_config: List[dict]) -> None:
        """Aws ELBv2 start (create) function.

        Create ELBv2, and all related resources.

        :param list elb_config:
            List[dict] of ELBv2 Load Balancers to create, with configs.

            Should also contain:
            - route53 config (for domains that should be covered by ELBv2)
            - target groups
            - cloudwatch alarms

            For example:
            {
                "Name": "core-staging",
                "Type": "application",
                "Subnets": ["subnet-xxx", "subnet-yyy"],
                "SecurityGroups": ["sg-xxx"],
                "Scheme": "internet-facing",
                # ...

                "Listeners": [
                    {
                        "Protocol": "HTTP",
                        "Port": 80,
                        "DefaultActions": [
                        ...
                    }
                ],

                "Route53Domains": [
                    {
                        "HostedZoneId": "Z1234567890ABC",    # Route53 Hosted Zone ID
                        "DomainName": "api.example.com",     # Full domain name
                        "RecordType": "A",                   # Usually A or AAAA
                        "TTL": 300
                    },
                    {
                        "HostedZoneId": "Z1234567890ABC",
                        "DomainName": "www.example.com",
                        "RecordType": "A",
                        "TTL": 300
                    }
                ]
            }
        """
        for config in elb_config:
            lb_name = config.get("Name")
            old_alarm_pattern = config.pop("OldAlarmPattern")
            try:
                normalized_lb = self._normalize_load_balancer(config)
                listeners = normalized_lb.pop("Listeners", [])
                attributes = normalized_lb.pop("Attributes", [])
                route53_domains = normalized_lb.pop("Route53Domains", [])

                response = self.elbv2.create_load_balancer(**normalized_lb)
                print(response)

                lb_data = response['LoadBalancers'][0]
                lb_arn = lb_data['LoadBalancerArn']
                dns_name = lb_data['DNSName']
                canonical_hosted_zone_id = lb_data['CanonicalHostedZoneId']
                # Format: app/<lb-name>/<lb-id>, for CW Alarm
                lb_dimension_value = lb_arn.split("loadbalancer/")[-1]

                if listeners:
                    self._create_listeners(lb_arn, listeners)
                else:
                    print(f"No listeners defined for {lb_name}")

                if attributes:
                    self._modify_load_balancer_attributes(lb_arn, attributes)

                if old_alarm_pattern:
                    self.update_cloudwatch_alarms(old_alarm_pattern, lb_dimension_value)

                print(f"Elastic Load Balancer {lb_name} created successfully: {lb_arn}; DNS Name: {dns_name}")

                if route53_domains:
                    self._create_route53_records(
                        lb_name=lb_name,
                        elb_dns_name=dns_name,
                        canonical_zone_id=canonical_hosted_zone_id,
                        domains=route53_domains
                    )

            except ClientError as exc:
                elb_exception("Elastic Load Balancer", lb_name, exc)


    def _modify_load_balancer_attributes(self, load_balancer_arn: str, attributes: List[dict]):
        """Apply load balancer attributes using modify_load_balancer_attributes."""
        try:
            # Convert to boto3 expected format: [{"Key": "...", "Value": "..."}, ...]
            attr_list = []
            for attr in attributes:
                if isinstance(attr, dict) and "Key" in attr and "Value" in attr:
                    attr_list.append({
                        "Key": attr["Key"],
                        "Value": str(attr["Value"])
                    })
                elif isinstance(attr, dict) and len(attr) == 1:
                    # Support simple dict like {"deletion_protection.enabled": "true"}
                    key = next(iter(attr))
                    value = str(attr[key])
                    attr_list.append({"Key": key, "Value": value})

            if not attr_list:
                print("No valid attributes to apply")
                return

            response = self.elbv2.modify_load_balancer_attributes(
                LoadBalancerArn=load_balancer_arn,
                Attributes=attr_list
            )

            print(f"Load Balancer attributes applied ({len(attr_list)} attributes)")

        except ClientError as exc:
            elb_exception("Load Balancer Attributes", load_balancer_arn, exc)

    def _create_listeners(self, load_balancer_arn: str, listeners: List[dict]) -> dict:
        """Create HTTP and/or HTTPS listeners for the load balancer."""
        listener_arns = {}

        for listener in listeners:
            try:
                protocol = listener.get("Protocol", "UNKNOWN")
                port = listener.get("Port", "?")
                rules = listener.get("Rules", [])

                listener_for_create = {k: v for k, v in listener.items() if k != "Rules"}

                normalized_listener = self._normalize_dict(listener_for_create)
                # Clean None values (fixes FixedResponseConfig: None etc.)
                #cleaned_listener = self._clean_none_values(normalized_listener)

                response = self.elbv2.create_listener(
                    LoadBalancerArn=load_balancer_arn,
                    **normalized_listener
                )

                print(f"Listener created: {protocol}:{port}")

                listener_arn = response['Listeners'][0]['ListenerArn']
                listener_arns[port] = listener_arn
                if rules:
                    self._create_rules(listener_arn, rules)

            except ClientError as exc:
                elb_exception("ELB Listener", f"{protocol}:{port}", exc)

        return listener_arns

    def _create_rules(self, listener_arn: str, rules: List[dict]):
        """Create listener rules using create_rule boto3 call."""
        for rule in rules:
            try:
                priority = rule.get("Priority")
                if priority is None:
                    print("Skipping rule without Priority")
                    continue

                normalized_rule = self._normalize_dict(rule)
                cleaned_rule = self._clean_none_values(normalized_rule)

                response = self.elbv2.create_rule(
                    ListenerArn=listener_arn,
                    **cleaned_rule
                )

                print(f"Rule created for listener (Priority: {priority})")

            except ClientError as exc:
                elb_exception("ELB Listener Rule", f"Priority {priority}", exc)


    def _create_route53_records(
        self,
        lb_name: str,
        elb_dns_name: str,
        canonical_zone_id: str,
        domains: List[dict]
    ):
        """Create alias records in Route53 pointing to the ELB."""
        for record in domains:
            try:
                hosted_zone_id = record["HostedZoneId"]
                domain_name = record["DomainName"]

                change_batch = {
                    "Changes": [{
                        "Action": "UPSERT",
                        "ResourceRecordSet": {
                            "Name": domain_name,
                            "Type": record.get("RecordType", "A"),
                            "AliasTarget": {
                                "HostedZoneId": canonical_zone_id,
                                "DNSName": elb_dns_name,
                                "EvaluateTargetHealth": False
                            }
                        }
                    }]
                }

                self.route53.change_resource_record_sets(
                    HostedZoneId=hosted_zone_id,
                    ChangeBatch=change_batch
                )

                print(f"Route53 alias: {domain_name} : {elb_dns_name}")

            except ClientError as exc:
                elb_exception("Route53 record", f"{lb_name} : {domain_name}", exc)
            except KeyError as exc:
                print(f"Missing field in Route53 config for {lb_name}: {exc}")


    def _normalize_listener(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize listener config (especially ensure Port is int)."""
        normalized = {}

        for key, value in raw.items():
            if value is None:
                continue

            if key == "Port":
                normalized[key] = ElbScheduler._cast_value(value, int)
            elif key == "IsDefault":
                normalized[key] = ElbScheduler._cast_value(value, bool)
            elif key in ("DefaultActions", "Certificates") and isinstance(value, list):
                normalized[key] = value
            else:
                normalized[key] = value

        return normalized


