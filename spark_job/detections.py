"""IAM detection functions used by the consolidated Spark batch."""

from __future__ import annotations

from collections.abc import Iterable
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

PRIVILEGED_GROUPS = ("Security-Admins", "DBA", "DevOps")
FAILURE_STATUSES = ("FAILED", "MFA_FAILED")
APPROVED_STATUSES = ("APPROVED", "COMPLETED")
REJECTED_STATUSES = ("REJECTED", "DENIED")

COUNTRY_COORDINATES = {
    "USA": (38.0, -97.0),
    "UK": (54.0, -2.0),
    "India": (22.0, 79.0),
    "Japan": (36.0, 138.0),
    "Brazil": (-10.0, -55.0),
    "Singapore": (1.35, 103.82),
    "Germany": (51.0, 9.0),
    "Australia": (-25.0, 133.0),
    "Canada": (56.0, -106.0),
}


def union_all(frames: Iterable[DataFrame]) -> DataFrame:
    frames = list(frames)
    if not frames:
        raise ValueError("union_all requires at least one dataframe")
    return reduce(lambda left, right: left.unionByName(right, allowMissingColumns=True), frames)


def normalized_status(column: str) -> F.Column:
    return F.upper(F.trim(F.coalesce(F.col(column), F.lit(""))))


def nonblank(column: str) -> F.Column:
    return F.col(column).isNotNull() & (F.trim(F.col(column)) != "")


def with_finding_metadata(
    frame: DataFrame,
    *,
    detection_name: str,
    identity_columns: tuple[str, ...],
    event_timestamp_column: str | None,
    run_id: str,
) -> DataFrame:
    identity = [
        F.coalesce(F.col(column).cast("string"), F.lit(""))
        for column in identity_columns
        if column in frame.columns
    ]
    event_date = (
        F.to_date(F.col(event_timestamp_column))
        if event_timestamp_column and event_timestamp_column in frame.columns
        else F.current_date()
    )
    return (
        frame.withColumn("detection_name", F.lit(detection_name))
        .withColumn(
            "finding_id",
            F.sha2(F.concat_ws("||", F.lit(detection_name), *identity), 256),
        )
        .withColumn("event_date", event_date)
        .withColumn("pipeline_run_id", F.lit(run_id))
        .withColumn("detected_at", F.current_timestamp())
    )


def impossible_login(okta_df: DataFrame, app_df: DataFrame, run_id: str) -> DataFrame:
    success = (
        okta_df.withColumn("login_ts", F.to_timestamp("ts"))
        .filter(normalized_status("status") == "SUCCESS")
        .select(
            F.col("user").alias("login_user"),
            F.col("session_id").alias("login_session_id"),
            "login_ts",
        )
    )
    app = app_df.withColumn("app_ts", F.to_timestamp("ts"))

    with_session = app.filter(nonblank("session_id")).alias("a").join(
        success.filter(nonblank("login_session_id")).alias("o"),
        (F.col("a.user") == F.col("o.login_user"))
        & (F.col("a.session_id") == F.col("o.login_session_id")),
        "left",
    )
    with_session = (
        with_session.withColumn(
            "seconds_from_login",
            F.unix_timestamp("a.app_ts") - F.unix_timestamp("o.login_ts"),
        )
        .withColumn(
            "anomaly_reason",
            F.when(F.col("o.login_ts").isNull(), F.lit("missing_session_login"))
            .when(F.col("seconds_from_login") < 0, F.lit("application_before_login"))
            .otherwise(F.lit("application_too_long_after_login")),
        )
        .filter(
            F.col("o.login_ts").isNull()
            | (F.col("seconds_from_login") < 0)
            | (F.col("seconds_from_login") > 1_800)
        )
        .select(
            F.col("a.user").alias("user"),
            F.col("a.usage_id").alias("usage_id"),
            F.col("a.session_id").alias("session_id"),
            F.col("a.app").alias("app"),
            F.col("o.login_ts").alias("login_time"),
            F.col("a.app_ts").alias("app_access_time"),
            "seconds_from_login",
            "anomaly_reason",
        )
    )

    without_session = app.filter(~nonblank("session_id")).alias("a").join(
        success.alias("o"),
        (F.col("a.user") == F.col("o.login_user"))
        & (F.col("o.login_ts") <= F.col("a.app_ts")),
        "left",
    )
    closest_login = Window.partitionBy("a.usage_id").orderBy(
        F.col("o.login_ts").desc_nulls_last()
    )
    without_session = (
        without_session.withColumn("login_rank", F.row_number().over(closest_login))
        .filter(F.col("login_rank") == 1)
        .withColumn(
            "seconds_from_login",
            F.unix_timestamp("a.app_ts") - F.unix_timestamp("o.login_ts"),
        )
        .withColumn(
            "anomaly_reason",
            F.when(F.col("o.login_ts").isNull(), F.lit("no_preceding_login")).otherwise(
                F.lit("application_too_long_after_login")
            ),
        )
        .filter(F.col("o.login_ts").isNull() | (F.col("seconds_from_login") > 1_800))
        .select(
            F.col("a.user").alias("user"),
            F.col("a.usage_id").alias("usage_id"),
            F.col("a.session_id").alias("session_id"),
            F.col("a.app").alias("app"),
            F.col("o.login_ts").alias("login_time"),
            F.col("a.app_ts").alias("app_access_time"),
            "seconds_from_login",
            "anomaly_reason",
        )
    )

    result = with_session.unionByName(without_session).dropDuplicates(["usage_id"])
    return with_finding_metadata(
        result,
        detection_name="impossible_login",
        identity_columns=("usage_id", "anomaly_reason"),
        event_timestamp_column="app_access_time",
        run_id=run_id,
    )


def mfa_bypass_detection(
    okta_df: DataFrame, app_df: DataFrame, run_id: str
) -> DataFrame:
    failed = (
        okta_df.withColumn("failure_ts", F.to_timestamp("ts"))
        .filter(normalized_status("status").contains("FAIL"))
        .select(
            F.col("req_id").alias("failed_request_id"),
            F.col("user").alias("failed_user"),
            F.col("session_id").alias("failed_session_id"),
            F.col("status").alias("failure_status"),
            "failure_ts",
        )
    )
    app = app_df.withColumn("app_ts", F.to_timestamp("ts"))
    joined = app.alias("a").join(
        failed.alias("f"),
        (F.col("a.user") == F.col("f.failed_user"))
        & (F.col("a.app_ts") > F.col("f.failure_ts"))
        & (
            F.unix_timestamp("a.app_ts") - F.unix_timestamp("f.failure_ts")
            <= 3_600
        )
        & (
            ~nonblank("f.failed_session_id")
            | ~nonblank("a.session_id")
            | (F.col("a.session_id") == F.col("f.failed_session_id"))
        ),
        "inner",
    )
    result = (
        joined.withColumn(
            "seconds_after_failure",
            F.unix_timestamp("a.app_ts") - F.unix_timestamp("f.failure_ts"),
        )
        .select(
            F.col("a.user").alias("user"),
            F.col("f.failed_request_id").alias("failed_request_id"),
            F.col("a.usage_id").alias("usage_id"),
            F.col("f.failed_session_id").alias("failed_session_id"),
            F.col("a.session_id").alias("app_session_id"),
            F.col("f.failure_status").alias("failure_status"),
            F.col("f.failure_ts").alias("mfa_failure_time"),
            F.col("a.app").alias("app"),
            F.col("a.app_ts").alias("app_access_time"),
            "seconds_after_failure",
        )
        .dropDuplicates(["failed_request_id", "usage_id"])
    )
    return with_finding_metadata(
        result,
        detection_name="mfa_bypass",
        identity_columns=("failed_request_id", "usage_id"),
        event_timestamp_column="app_access_time",
        run_id=run_id,
    )


def mfa_bypass_all_apps(mfa_events: DataFrame, run_id: str) -> DataFrame:
    result = mfa_events.groupBy(
        "user", "failed_request_id", "mfa_failure_time", "failure_status"
    ).agg(
        F.sort_array(F.collect_set("app")).alias("applications_accessed"),
        F.countDistinct("usage_id").alias("usage_event_count"),
        F.min("app_access_time").alias("first_app_access_time"),
        F.max("app_access_time").alias("last_app_access_time"),
    )
    return with_finding_metadata(
        result,
        detection_name="mfa_bypass_all_apps",
        identity_columns=("failed_request_id",),
        event_timestamp_column="first_app_access_time",
        run_id=run_id,
    )


def impossible_travel(okta_df: DataFrame, run_id: str) -> DataFrame:
    latitude_items: list[F.Column] = []
    longitude_items: list[F.Column] = []
    for country, (latitude, longitude) in COUNTRY_COORDINATES.items():
        latitude_items.extend([F.lit(country), F.lit(latitude)])
        longitude_items.extend([F.lit(country), F.lit(longitude)])
    latitude_map = F.create_map(*latitude_items)
    longitude_map = F.create_map(*longitude_items)

    ordered = Window.partitionBy("user").orderBy("login_ts")
    enriched = (
        okta_df.withColumn("login_ts", F.to_timestamp("ts"))
        .filter(normalized_status("status") == "SUCCESS")
        .withColumn("latitude", F.element_at(latitude_map, F.col("country")))
        .withColumn("longitude", F.element_at(longitude_map, F.col("country")))
        .withColumn("previous_country", F.lag("country").over(ordered))
        .withColumn("previous_login_ts", F.lag("login_ts").over(ordered))
        .withColumn("previous_latitude", F.lag("latitude").over(ordered))
        .withColumn("previous_longitude", F.lag("longitude").over(ordered))
        .withColumn(
            "seconds_between",
            F.unix_timestamp("login_ts") - F.unix_timestamp("previous_login_ts"),
        )
    )
    lat_delta = F.radians(F.col("latitude") - F.col("previous_latitude"))
    lon_delta = F.radians(F.col("longitude") - F.col("previous_longitude"))
    haversine = F.pow(F.sin(lat_delta / 2), 2) + F.cos(
        F.radians("previous_latitude")
    ) * F.cos(F.radians("latitude")) * F.pow(F.sin(lon_delta / 2), 2)
    result = (
        enriched.withColumn(
            "distance_km", F.lit(2 * 6_371.0) * F.asin(F.sqrt(haversine))
        )
        .withColumn(
            "required_speed_kmh",
            F.col("distance_km") / (F.col("seconds_between") / F.lit(3_600.0)),
        )
        .filter(
            F.col("previous_country").isNotNull()
            & (F.col("country") != F.col("previous_country"))
            & (F.col("seconds_between") > 0)
            & (F.col("seconds_between") <= 21_600)
            & F.col("distance_km").isNotNull()
            & (F.col("required_speed_kmh") > 900)
        )
        .select(
            "user",
            "req_id",
            "previous_country",
            F.col("country").alias("current_country"),
            "previous_login_ts",
            F.col("login_ts").alias("current_login_ts"),
            "seconds_between",
            F.round("distance_km", 1).alias("distance_km"),
            F.round("required_speed_kmh", 1).alias("required_speed_kmh"),
        )
    )
    return with_finding_metadata(
        result,
        detection_name="impossible_travel",
        identity_columns=("req_id", "previous_country", "current_country"),
        event_timestamp_column="current_login_ts",
        run_id=run_id,
    )


def shadow_access(ad_df: DataFrame, sav_df: DataFrame, run_id: str) -> DataFrame:
    grants = (
        ad_df.withColumn("grant_ts", F.to_timestamp("ts"))
        .filter(
            F.col("group").isin(*PRIVILEGED_GROUPS)
            & F.col("action").isin("add_member", "privilege_escalation")
        )
        .alias("a")
    )
    approved = (
        sav_df.filter(normalized_status("status").isin(*APPROVED_STATUSES))
        .filter(nonblank("req_id"))
        .select(
            F.col("req_id").alias("approved_request_id"),
            F.col("user").alias("approved_user"),
        )
        .alias("s")
    )
    result = grants.join(
        approved,
        (F.col("a.request_id") == F.col("s.approved_request_id"))
        & (F.col("a.user") == F.col("s.approved_user")),
        "left_anti",
    ).select(
        F.col("a.user").alias("user"),
        F.col("a.event_id").alias("event_id"),
        F.col("a.request_id").alias("request_id"),
        F.col("a.group").alias("privileged_group"),
        F.col("a.action").alias("action"),
        F.col("a.initiator").alias("granted_by"),
        F.col("a.grant_ts").alias("grant_time"),
    )
    return with_finding_metadata(
        result,
        detection_name="shadow_access",
        identity_columns=("event_id",),
        event_timestamp_column="grant_time",
        run_id=run_id,
    )


def privilege_escalation_without_request(
    ad_df: DataFrame, sav_df: DataFrame, run_id: str
) -> DataFrame:
    escalations = (
        ad_df.withColumn("escalation_ts", F.to_timestamp("ts"))
        .filter(F.col("action") == "privilege_escalation")
        .alias("a")
    )
    approved = (
        sav_df.filter(normalized_status("status").isin(*APPROVED_STATUSES))
        .filter(nonblank("req_id"))
        .select(
            F.col("req_id").alias("approved_request_id"),
            F.col("user").alias("approved_user"),
        )
        .alias("s")
    )
    result = escalations.join(
        approved,
        (F.col("a.request_id") == F.col("s.approved_request_id"))
        & (F.col("a.user") == F.col("s.approved_user")),
        "left_anti",
    ).select(
        F.col("a.user").alias("user"),
        F.col("a.event_id").alias("event_id"),
        F.col("a.request_id").alias("request_id"),
        F.col("a.group").alias("group"),
        F.col("a.initiator").alias("initiator"),
        F.col("a.escalation_ts").alias("escalation_time"),
    )
    return with_finding_metadata(
        result,
        detection_name="privilege_escalation_without_request",
        identity_columns=("event_id",),
        event_timestamp_column="escalation_time",
        run_id=run_id,
    )


def ad_volume_spikes(ad_df: DataFrame, run_id: str) -> DataFrame:
    daily = (
        ad_df.withColumn("event_ts", F.to_timestamp("ts"))
        .groupBy(F.to_date("event_ts").alias("activity_date"), "action")
        .agg(F.count("*").alias("event_count"))
    )
    history = (
        Window.partitionBy("action")
        .orderBy(F.col("activity_date").cast("timestamp").cast("long"))
        .rangeBetween(-7 * 86_400, -86_400)
    )
    result = (
        daily.withColumn("rolling_average", F.avg("event_count").over(history))
        .withColumn("history_days", F.count("event_count").over(history))
        .filter(
            (F.col("history_days") >= 2)
            & (F.col("event_count") > F.col("rolling_average") * 3)
        )
    )
    return with_finding_metadata(
        result,
        detection_name="ad_volume_spike",
        identity_columns=("activity_date", "action"),
        event_timestamp_column=None,
        run_id=run_id,
    ).withColumn("event_date", F.col("activity_date"))


def rejected_but_executed(
    ad_df: DataFrame, sav_df: DataFrame, run_id: str
) -> DataFrame:
    rejected = (
        sav_df.withColumn("decision_ts", F.to_timestamp("ts"))
        .filter(normalized_status("status").isin(*REJECTED_STATUSES))
        .alias("s")
    )
    executed = ad_df.withColumn("execution_ts", F.to_timestamp("ts")).alias("a")
    result = executed.join(
        rejected,
        (F.col("a.request_id") == F.col("s.req_id"))
        & (F.col("a.user") == F.col("s.user")),
        "inner",
    ).select(
        F.col("a.user").alias("user"),
        F.col("a.event_id").alias("event_id"),
        F.col("a.request_id").alias("request_id"),
        F.col("s.status").alias("request_status"),
        F.col("s.app").alias("requested_app"),
        F.col("s.role").alias("requested_role"),
        F.col("s.decision_ts").alias("decision_time"),
        F.col("a.group").alias("executed_group"),
        F.col("a.action").alias("executed_action"),
        F.col("a.execution_ts").alias("execution_time"),
    )
    return with_finding_metadata(
        result,
        detection_name="rejected_but_executed",
        identity_columns=("event_id", "request_id"),
        event_timestamp_column="execution_time",
        run_id=run_id,
    )


def high_risk_sequence(app_df: DataFrame, run_id: str) -> DataFrame:
    app = (
        app_df.withColumn("app_ts", F.to_timestamp("ts"))
        .withColumn(
            "session_key",
            F.when(nonblank("session_id"), F.col("session_id")).otherwise(
                F.concat(F.lit("user:"), F.col("user"))
            ),
        )
    )
    ordered = Window.partitionBy("user", "session_key").orderBy("app_ts")
    result = (
        app.withColumn("previous_app", F.lag("app").over(ordered))
        .withColumn("next_app", F.lead("app").over(ordered))
        .filter(
            (F.col("previous_app") == "GitHub")
            & (F.col("app") == "Snowflake")
            & (F.col("next_app") == "Databricks")
        )
        .select(
            "user",
            "usage_id",
            "session_id",
            "previous_app",
            F.col("app").alias("current_app"),
            "next_app",
            F.col("app_ts").alias("sequence_time"),
        )
    )
    return with_finding_metadata(
        result,
        detection_name="high_risk_sequence",
        identity_columns=("usage_id",),
        event_timestamp_column="sequence_time",
        run_id=run_id,
    )


def excessive_data_transferred(app_df: DataFrame, run_id: str) -> DataFrame:
    daily = (
        app_df.withColumn("app_ts", F.to_timestamp("ts"))
        .withColumn("data_mb_numeric", F.col("data_mb").cast("double"))
        .groupBy("user", F.to_date("app_ts").alias("activity_date"))
        .agg(F.sum("data_mb_numeric").alias("total_data_mb"))
    )
    history = (
        Window.partitionBy("user")
        .orderBy(F.col("activity_date").cast("timestamp").cast("long"))
        .rangeBetween(-7 * 86_400, -86_400)
    )
    result = (
        daily.withColumn("rolling_average_mb", F.avg("total_data_mb").over(history))
        .withColumn("history_days", F.count("total_data_mb").over(history))
        .filter(
            (F.col("history_days") >= 2)
            & (
                F.col("total_data_mb")
                > F.greatest(F.col("rolling_average_mb") * 3, F.lit(500.0))
            )
        )
    )
    return with_finding_metadata(
        result,
        detection_name="excessive_data_transferred",
        identity_columns=("user", "activity_date"),
        event_timestamp_column=None,
        run_id=run_id,
    ).withColumn("event_date", F.col("activity_date"))


def role_drift(sav_df: DataFrame, run_id: str) -> DataFrame:
    approved = sav_df.withColumn("request_ts", F.to_timestamp("ts")).filter(
        normalized_status("status").isin(*APPROVED_STATUSES)
    )
    result = (
        approved.groupBy("user", "app")
        .agg(
            F.min_by("role", "request_ts").alias("first_role"),
            F.max_by("role", "request_ts").alias("latest_role"),
            F.min("request_ts").alias("first_seen"),
            F.max("request_ts").alias("last_seen"),
            F.countDistinct("role").alias("distinct_roles"),
        )
        .filter(
            (F.col("distinct_roles") > 1) & (F.col("first_role") != F.col("latest_role"))
        )
    )
    return with_finding_metadata(
        result,
        detection_name="role_drift",
        identity_columns=("user", "app", "first_role", "latest_role"),
        event_timestamp_column="last_seen",
        run_id=run_id,
    )


def stale_access(app_df: DataFrame, sav_df: DataFrame, run_id: str) -> DataFrame:
    approved = (
        sav_df.withColumn("grant_ts", F.to_timestamp("ts"))
        .filter(normalized_status("status").isin(*APPROVED_STATUSES))
        .groupBy("user", "app")
        .agg(
            F.max("grant_ts").alias("latest_grant_time"),
            F.max_by("role", "grant_ts").alias("latest_role"),
        )
    )
    app = app_df.withColumn("usage_ts", F.to_timestamp("ts"))
    last_use = app.groupBy("user", "app").agg(F.max("usage_ts").alias("last_used_time"))
    as_of = app.agg(F.max("usage_ts").alias("as_of_time"))
    result = (
        approved.join(last_use, ["user", "app"], "left")
        .crossJoin(as_of)
        .withColumn(
            "inactive_days",
            F.datediff(
                "as_of_time", F.coalesce(F.col("last_used_time"), F.col("latest_grant_time"))
            ),
        )
        .filter(F.col("inactive_days") > 45)
    )
    return with_finding_metadata(
        result,
        detection_name="stale_access",
        identity_columns=("user", "app", "latest_role"),
        event_timestamp_column="as_of_time",
        run_id=run_id,
    )


def multi_app_session(app_df: DataFrame, run_id: str) -> DataFrame:
    result = (
        app_df.withColumn("app_ts", F.to_timestamp("ts"))
        .filter(nonblank("session_id"))
        .groupBy("user", "session_id")
        .agg(
            F.countDistinct("app").alias("application_count"),
            F.sort_array(F.collect_set("app")).alias("applications"),
            F.min("app_ts").alias("session_start"),
            F.max("app_ts").alias("session_end"),
        )
        .filter(F.col("application_count") > 4)
    )
    return with_finding_metadata(
        result,
        detection_name="multi_app_single_session",
        identity_columns=("user", "session_id"),
        event_timestamp_column="session_start",
        run_id=run_id,
    )


def mfa_drift(okta_df: DataFrame, run_id: str) -> DataFrame:
    okta = okta_df.withColumn("login_ts", F.to_timestamp("ts"))
    result = (
        okta.groupBy("user")
        .agg(
            F.min_by("mfa", "login_ts").alias("first_mfa_method"),
            F.max_by("mfa", "login_ts").alias("latest_mfa_method"),
            F.countDistinct("mfa").alias("distinct_mfa_methods"),
            F.sum(F.when(normalized_status("status").contains("FAIL"), 1).otherwise(0)).alias(
                "failed_attempts"
            ),
            F.count("*").alias("total_attempts"),
            F.max("login_ts").alias("last_login_time"),
        )
        .withColumn("failure_rate", F.col("failed_attempts") / F.col("total_attempts"))
        .filter(
            (F.col("distinct_mfa_methods") > 1)
            & (F.col("first_mfa_method") != F.col("latest_mfa_method"))
        )
    )
    return with_finding_metadata(
        result,
        detection_name="mfa_drift",
        identity_columns=("user", "first_mfa_method", "latest_mfa_method"),
        event_timestamp_column="last_login_time",
        run_id=run_id,
    )


def suspicious_approver(sav_df: DataFrame, run_id: str) -> DataFrame:
    decided = sav_df.withColumn("decision_ts", F.to_timestamp("ts")).filter(
        normalized_status("status").isin(
            *(APPROVED_STATUSES + REJECTED_STATUSES)
        )
    )
    result = (
        decided.groupBy("approver")
        .agg(
            F.count("*").alias("decision_count"),
            F.sum(
                F.when(normalized_status("status").isin(*APPROVED_STATUSES), 1).otherwise(0)
            ).alias("approved_count"),
            F.sum(
                F.when(normalized_status("status").isin(*REJECTED_STATUSES), 1).otherwise(0)
            ).alias("rejected_count"),
            F.max("decision_ts").alias("latest_decision_time"),
        )
        .withColumn("approval_rate", F.col("approved_count") / F.col("decision_count"))
        .filter(
            (F.col("decision_count") >= 5)
            & ((F.col("approval_rate") >= 0.9) | (F.col("approval_rate") <= 0.1))
        )
        .withColumnRenamed("approver", "user")
    )
    return with_finding_metadata(
        result,
        detection_name="suspicious_approver_behavior",
        identity_columns=("user",),
        event_timestamp_column="latest_decision_time",
        run_id=run_id,
    )


def ad_group_churn(ad_df: DataFrame, run_id: str) -> DataFrame:
    result = (
        ad_df.withColumn("event_ts", F.to_timestamp("ts"))
        .filter(F.col("action").isin("add_member", "remove_member"))
        .groupBy("user", "group")
        .agg(
            F.sum(F.when(F.col("action") == "add_member", 1).otherwise(0)).alias(
                "add_count"
            ),
            F.sum(F.when(F.col("action") == "remove_member", 1).otherwise(0)).alias(
                "remove_count"
            ),
            F.count("*").alias("change_count"),
            F.max("event_ts").alias("latest_change_time"),
        )
        .filter(F.col("change_count") >= 4)
    )
    return with_finding_metadata(
        result,
        detection_name="ad_group_churn",
        identity_columns=("user", "group"),
        event_timestamp_column="latest_change_time",
        run_id=run_id,
    )


def orphan_events(
    okta_df: DataFrame,
    sav_df: DataFrame,
    ad_df: DataFrame,
    app_df: DataFrame,
    hr_df: DataFrame,
    run_id: str,
) -> DataFrame:
    valid_users = hr_df.select("user").filter(nonblank("user")).distinct()
    events = union_all(
        [
            okta_df.select(
                "user",
                F.col("req_id").alias("source_event_id"),
                F.lit("okta").alias("source"),
                F.to_timestamp("ts").alias("event_time"),
            ),
            sav_df.select(
                "user",
                F.col("req_id").alias("source_event_id"),
                F.lit("saviynt").alias("source"),
                F.to_timestamp("ts").alias("event_time"),
            ),
            ad_df.select(
                "user",
                F.col("event_id").alias("source_event_id"),
                F.lit("ad").alias("source"),
                F.to_timestamp("ts").alias("event_time"),
            ),
            app_df.select(
                "user",
                F.col("usage_id").alias("source_event_id"),
                F.lit("app_usage").alias("source"),
                F.to_timestamp("ts").alias("event_time"),
            ),
        ]
    )
    result = events.join(valid_users, "user", "left_anti")
    return with_finding_metadata(
        result,
        detection_name="orphan_event",
        identity_columns=("source", "source_event_id"),
        event_timestamp_column="event_time",
        run_id=run_id,
    )


def orphan_users(
    okta_df: DataFrame,
    sav_df: DataFrame,
    ad_df: DataFrame,
    app_df: DataFrame,
    hr_df: DataFrame,
    run_id: str,
) -> DataFrame:
    valid_users = hr_df.select("user").filter(nonblank("user")).distinct()
    observed = union_all(
        [
            okta_df.select("user").withColumn("source", F.lit("okta")),
            sav_df.select("user").withColumn("source", F.lit("saviynt")),
            ad_df.select("user").withColumn("source", F.lit("ad")),
            app_df.select("user").withColumn("source", F.lit("app_usage")),
        ]
    )
    result = (
        observed.join(valid_users, "user", "left_anti")
        .groupBy("user")
        .agg(
            F.sort_array(F.collect_set("source")).alias("observed_sources"),
            F.count("*").alias("event_count"),
        )
    )
    return with_finding_metadata(
        result,
        detection_name="orphan_user",
        identity_columns=("user",),
        event_timestamp_column=None,
        run_id=run_id,
    )


def identity_timeline(
    okta_df: DataFrame,
    sav_df: DataFrame,
    ad_df: DataFrame,
    app_df: DataFrame,
    run_id: str,
) -> DataFrame:
    timeline = union_all(
        [
            okta_df.select(
                "user",
                F.to_timestamp("ts").alias("event_time"),
                F.lit("okta_login").alias("event_type"),
                F.col("req_id").alias("source_event_id"),
                F.concat_ws(
                    " | ",
                    F.concat(F.lit("status="), F.col("status")),
                    F.concat(F.lit("country="), F.col("country")),
                    F.concat(F.lit("mfa="), F.col("mfa")),
                ).alias("details"),
            ),
            sav_df.select(
                "user",
                F.to_timestamp("ts").alias("event_time"),
                F.lit("access_request").alias("event_type"),
                F.col("req_id").alias("source_event_id"),
                F.concat_ws(
                    " | ",
                    F.concat(F.lit("app="), F.col("app")),
                    F.concat(F.lit("role="), F.col("role")),
                    F.concat(F.lit("status="), F.col("status")),
                ).alias("details"),
            ),
            ad_df.select(
                "user",
                F.to_timestamp("ts").alias("event_time"),
                F.lit("directory_change").alias("event_type"),
                F.col("event_id").alias("source_event_id"),
                F.concat_ws(
                    " | ",
                    F.concat(F.lit("group="), F.col("group")),
                    F.concat(F.lit("action="), F.col("action")),
                    F.concat(F.lit("initiator="), F.col("initiator")),
                ).alias("details"),
            ),
            app_df.select(
                "user",
                F.to_timestamp("ts").alias("event_time"),
                F.lit("application_usage").alias("event_type"),
                F.col("usage_id").alias("source_event_id"),
                F.concat_ws(
                    " | ",
                    F.concat(F.lit("app="), F.col("app")),
                    F.concat(F.lit("action="), F.col("action")),
                    F.concat(F.lit("data_mb="), F.col("data_mb")),
                ).alias("details"),
            ),
        ]
    )
    return with_finding_metadata(
        timeline,
        detection_name="identity_timeline",
        identity_columns=("event_type", "source_event_id"),
        event_timestamp_column="event_time",
        run_id=run_id,
    )


def suspicious_time_warp(
    sav_df: DataFrame, ad_df: DataFrame, app_df: DataFrame, run_id: str
) -> DataFrame:
    approved = (
        sav_df.withColumn("approval_ts", F.to_timestamp("ts"))
        .filter(normalized_status("status").isin(*APPROVED_STATUSES))
        .filter(nonblank("req_id"))
        .select(
            F.col("req_id").alias("request_id"),
            F.col("user").alias("request_user"),
            "approval_ts",
        )
        .alias("s")
    )
    ad_actions = (
        ad_df.withColumn("action_ts", F.to_timestamp("ts"))
        .filter(nonblank("request_id"))
        .select(
            F.col("request_id"),
            F.col("user"),
            F.col("event_id").alias("source_event_id"),
            F.lit("ad").alias("source"),
            "action_ts",
        )
    )
    app_actions = (
        app_df.withColumn("action_ts", F.to_timestamp("ts"))
        .filter(nonblank("request_id"))
        .select(
            F.col("request_id"),
            F.col("user"),
            F.col("usage_id").alias("source_event_id"),
            F.lit("app_usage").alias("source"),
            "action_ts",
        )
    )
    actions = ad_actions.unionByName(app_actions).alias("a")
    result = (
        actions.join(
            approved,
            (F.col("a.request_id") == F.col("s.request_id"))
            & (F.col("a.user") == F.col("s.request_user")),
            "inner",
        )
        .filter(F.col("a.action_ts") < F.col("s.approval_ts"))
        .withColumn(
            "seconds_before_approval",
            F.unix_timestamp("s.approval_ts") - F.unix_timestamp("a.action_ts"),
        )
        .select(
            F.col("a.user").alias("user"),
            F.col("a.request_id").alias("request_id"),
            F.col("a.source_event_id").alias("source_event_id"),
            F.col("a.source").alias("source"),
            F.col("a.action_ts").alias("action_time"),
            F.col("s.approval_ts").alias("approval_time"),
            "seconds_before_approval",
        )
    )
    return with_finding_metadata(
        result,
        detection_name="suspicious_time_warp",
        identity_columns=("source", "source_event_id", "request_id"),
        event_timestamp_column="action_time",
        run_id=run_id,
    )


def risk_scores(
    *,
    hr_df: DataFrame,
    source_frames: tuple[DataFrame, ...],
    weighted_findings: dict[str, tuple[DataFrame, int]],
    anomaly_key_df: DataFrame,
    run_id: str,
) -> DataFrame:
    all_users = union_all(
        [frame.select("user") for frame in (hr_df, *source_frames) if "user" in frame.columns]
    ).filter(nonblank("user")).distinct()

    contributions: list[DataFrame] = []
    for detection_name, (frame, weight) in weighted_findings.items():
        if "user" not in frame.columns:
            continue
        contributions.append(
            frame.select("user")
            .filter(nonblank("user"))
            .withColumn("risk_points", F.lit(weight))
            .withColumn("contribution", F.lit(detection_name))
        )
    contributions.append(
        anomaly_key_df.select("user")
        .filter(nonblank("user"))
        .withColumn("risk_points", F.lit(1))
        .withColumn("contribution", F.lit("known_anomaly_label"))
    )
    combined = union_all(contributions)
    aggregate = combined.groupBy("user").agg(
        F.sum("risk_points").alias("risk_score"),
        F.sort_array(F.collect_set("contribution")).alias("risk_factors"),
        F.count("*").alias("finding_count"),
    )
    result = (
        all_users.join(aggregate, "user", "left")
        .fillna({"risk_score": 0, "finding_count": 0})
        .withColumn(
            "risk_level",
            F.when(F.col("risk_score") >= 15, "CRITICAL")
            .when(F.col("risk_score") >= 8, "HIGH")
            .when(F.col("risk_score") >= 3, "MEDIUM")
            .otherwise("LOW"),
        )
    )
    return with_finding_metadata(
        result,
        detection_name="security_reputation",
        identity_columns=("user",),
        event_timestamp_column=None,
        run_id=run_id,
    )


def cleanest_users(risk_df: DataFrame, run_id: str) -> DataFrame:
    result = risk_df.select(
        "user", "risk_score", "risk_level", "finding_count", "risk_factors"
    ).orderBy(F.col("risk_score").asc(), F.col("finding_count").asc(), F.col("user")).limit(10)
    return with_finding_metadata(
        result,
        detection_name="cleanest_users",
        identity_columns=("user",),
        event_timestamp_column=None,
        run_id=run_id,
    )


def build_detections(frames: dict[str, DataFrame], run_id: str) -> dict[str, DataFrame]:
    okta = frames["okta"]
    ad = frames["ad"]
    app = frames["app_usage"]
    saviynt = frames["saviynt"]
    hr = frames["hrlifecycle"]
    anomaly_key = frames["anomaly_key"]

    impossible = impossible_login(okta, app, run_id)
    mfa = mfa_bypass_detection(okta, app, run_id)
    travel = impossible_travel(okta, run_id)
    shadow = shadow_access(ad, saviynt, run_id)
    privilege = privilege_escalation_without_request(ad, saviynt, run_id)
    rejected = rejected_but_executed(ad, saviynt, run_id)
    excessive = excessive_data_transferred(app, run_id)
    stale = stale_access(app, saviynt, run_id)
    orphan_event = orphan_events(okta, saviynt, ad, app, hr, run_id)
    orphan_user = orphan_users(okta, saviynt, ad, app, hr, run_id)
    time_warp = suspicious_time_warp(saviynt, ad, app, run_id)

    weighted = {
        "impossible_login": (impossible, 3),
        "mfa_bypass": (mfa, 5),
        "impossible_travel": (travel, 5),
        "shadow_access": (shadow, 5),
        "privilege_escalation": (privilege, 5),
        "rejected_but_executed": (rejected, 5),
        "excessive_data": (excessive, 3),
        "stale_access": (stale, 1),
        "orphan_event": (orphan_event, 2),
        "orphan_user": (orphan_user, 3),
        "time_warp": (time_warp, 4),
    }
    risk = risk_scores(
        hr_df=hr,
        source_frames=(okta, ad, app, saviynt),
        weighted_findings=weighted,
        anomaly_key_df=anomaly_key,
        run_id=run_id,
    )

    return {
        "impossible_login_anomalies": impossible,
        "mfa_bypass_events": mfa,
        "mfa_bypass_events_all_apps": mfa_bypass_all_apps(mfa, run_id),
        "impossible_travel_events": travel,
        "privilege_escalation_without_request": privilege,
        "ad_volume_spikes": ad_volume_spikes(ad, run_id),
        "rejected_but_executed": rejected,
        "high_risk_sequence": high_risk_sequence(app, run_id),
        "excessive_data_transferred": excessive,
        "role_drift": role_drift(saviynt, run_id),
        "stale_access": stale,
        "multi_app_single_session": multi_app_session(app, run_id),
        "mfa_drift": mfa_drift(okta, run_id),
        "suspicious_approver_behavior": suspicious_approver(saviynt, run_id),
        "ad_group_churn_rate": ad_group_churn(ad, run_id),
        "orphan_event_detection": orphan_event,
        "orphan_user_detection": orphan_user,
        "cleanest_users": cleanest_users(risk, run_id),
        "shadow_access_events": shadow,
        "risk_scores": risk,
        "identity_timelines": identity_timeline(okta, saviynt, ad, app, run_id),
        "suspicious_time_warp": time_warp,
    }
