"""Valkey Cache scheduler."""

from typing import List, Dict, Any

import boto3
from botocore.exceptions import ClientError

from .exceptions import valkey_exception


class ValkeyScheduler:
    """Abstract Valkey scheduler in a class."""

    # normalization schema, as per boto3 docs
    _FIELD_TYPES = {
        # bool
        "AtRestEncryptionEnabled": bool,
        "AutomaticFailoverEnabled": bool,
        "AutoMinorVersionUpgrade": bool,
        "MultiAZEnabled": bool,
        "TransitEncryptionEnabled": bool,
        # int
        "NumNodeGroups": int,
        "ReplicasPerNodeGroup": int,
        "SnapshotRetentionLimit": int,
        # list
        "LogDeliveryConfigurations": list,
        "SecurityGroupIds": list,
        "Tags": list,
    }

    def __init__(self, region_name=None) -> None:
        """Initialize ElastiCache service."""
        if region_name:
            self.elasticache = boto3.client("elasticache", region_name=region_name)
        else:
            self.elasticache = boto3.client("elasticache")

    # internal helpers
    @classmethod
    def _normalize_replication_group(cls, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize JSON-derived config to boto3-friendly types."""
        normalized = {}

        for key, value in raw.items():
            if value is None:
                continue

            if key in cls._FIELD_TYPES:
                normalized[key] = cls._cast_value(value, cls._FIELD_TYPES[key])
            else:
                normalized[key] = value

        return normalized

    @staticmethod
    def _cast_value(value: Any, expected_type: type) -> Any:
        if expected_type is bool:
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.strip().lower() == "true"
            raise ValueError(f"Invalid boolean value: {value}")

        if expected_type is list:
            # Tags
            if isinstance(value, dict):
                return [{"Key": k, "Value": str(v)} for k, v in value.items()]
            # Already-correct list (Tags, LogDeliveryConfigurations)
            if isinstance(value, list):
                return value
            # SecurityGroupIds from comma-separated string
            if isinstance(value, str):
                return [v.strip() for v in value.split(",") if v.strip()]

            raise ValueError(f"Invalid list value: {value}")

        try:
            return expected_type(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Cannot cast value '{value}' to {expected_type.__name__}") from exc

    def stop(self, replication_group_names: List[str]) -> None:
        """Aws ElastiCache Valkey schedules stop (disable) function.

        Since we cannot stop Valkey replication group, we delete it.

        :param list replication_group_names:
            List of Valkey replication group to stop (delete).
            For example:
            ['project-app-cache-prod-us-east-1', 'project-anotherapp-cache-staging-us-east-1']
        """
        for replication_group in replication_group_names:
            try:
                response = self.elasticache.delete_replication_group(
                    ReplicationGroupId=replication_group,
                    RetainPrimaryCluster=False
                )

                print(response)

                print(f"ElastiCache Valkey replication group {replication_group} disabled (deleted).")
            except ClientError as exc:
                valkey_exception("ElastiCache Valkey replication group", replication_group, exc)

    def start(self, replication_groups: List[dict]) -> None:
        """Aws Elasticache Valkey replication group start (create) function.

        Create Valkey replication group.

        :param list replication_groups:
            List[dict] of Valkey replication group to create, with configs.
            For example:
            [
                {
                    "ReplicationGroupId": "",
                    "ReplicationGroupDescription": "",
                },
                {
                    ...
                }
            ]
        """
        for group in replication_groups:
            try:
                normalized_group = self._normalize_replication_group(group)

                response = self.elasticache.create_replication_group(**normalized_group)
                print(response)
                print(f"ElastiCache Valkey replication group {group['ReplicationGroupId']} created.")

            except ClientError as exc:
                valkey_exception("ElastiCache Valkey replication group", group['ReplicationGroupId'], exc)

