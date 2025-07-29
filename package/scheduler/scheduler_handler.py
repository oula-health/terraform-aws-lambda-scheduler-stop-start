"""EventBridge Scheduler scheduler."""

from typing import List

import boto3
from botocore.exceptions import ClientError

from .exceptions import scheduler_exception


class EventBridgeSchedulerScheduler:
    """Abstract EventBridge Scheduler scheduler in a class."""

    def __init__(self, region_name=None) -> None:
        """Initialize ECS service scheduler."""
        if region_name:
            self.scheduler = boto3.client("scheduler", region_name=region_name)
        else:
            self.scheduler = boto3.client("scheduler")

    def stop(self, schedule_names: List[str]) -> None:
        """Aws EventBridge Scheduler schedules stop (disable) function.

        Disable EventBridge Scheduler schedule.

        :param list schedule_names:
            List of EventBridge Scheduler schedules to stop (disable).
            For example:
            ['project-app-afternoon-job-prod-us-east-1', 'project-anotherapp-staging-us-east-1']
        """
        for schedule in schedule_names:
            try:
                response = self.scheduler.get_schedule(Name=schedule)

                # Update State and remove fields not accepted by update_schedule()
                payload = {
                    key: value for key, value in response.items()
                    if key not in ["Arn", "CreationDate", "LastModificationDate", "ResponseMetadata"]
                }
                payload["State"] = "DISABLED"

                self.scheduler.update_schedule(**payload)
                print(f"EventBridge Scheduler schedule {schedule} disabled")
            except ClientError as exc:
                scheduler_exception("EventBridge Scheduler schedule", schedule, exc)

    def start(self, schedule_names: List[str]) -> None:
        """Aws EventBridge Scheduler schedules start (enable) function.

        Enable EventBridge Scheduler schedule.

        :param list schedule_names:
            List of EventBridge Scheduler schedules to start (enable).
            For example:
            ['project-app-afternoon-job-prod-us-east-1', 'project-anotherapp-staging-us-east-1']
        """
        for schedule in schedule_names:
            try:
                response = self.scheduler.get_schedule(Name=schedule)
                # Update State and remove fields not accepted by update_schedule()
                payload = {
                    key: value for key, value in response.items()
                    if key not in ["Arn", "CreationDate", "LastModificationDate", "ResponseMetadata"]
                }
                payload["State"] = "ENABLED"

                self.scheduler.update_schedule(**payload)
                print(f"EventBridge Scheduler schedule {schedule} enabled")
            except ClientError as exc:
                scheduler_exception("EventBridge Scheduler schedule", schedule, exc)

