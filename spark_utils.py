"""
SparkSession 생성 공통 모듈.

- Windows: C:\\winutils 가 있으면 HADOOP_HOME 과 PATH 를 자동 설정한다 (Parquet 쓰기용).
- Linux / WSL / macOS: 추가 설정 없이 그대로 동작한다.
- Python UDF 를 쓰지 않으므로 Arrow 는 꺼 둔다 (Java 21 호환).
"""
from __future__ import annotations

import os
import sys

_WINUTILS = r"C:\winutils"


def _prepare_env() -> None:
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    if os.name == "nt" and os.path.isdir(_WINUTILS):
        os.environ["HADOOP_HOME"] = _WINUTILS
        os.environ["PATH"] = _WINUTILS + r"\bin;" + os.environ.get("PATH", "")


def get_spark(app_name: str, master: str = "local[*]", driver_memory: str = "4g",
              shuffle_partitions: int = 10):
    """공통 설정으로 SparkSession 을 만든다."""
    _prepare_env()
    from pyspark.sql import SparkSession

    java_opts = (
        "--add-opens=java.base/java.lang=ALL-UNNAMED"
        " --add-opens=java.base/java.nio=ALL-UNNAMED"
        " --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    )
    if os.name == "nt" and os.path.isdir(_WINUTILS):
        java_opts = "-Djava.library.path=C:/winutils/bin " + java_opts

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", java_opts)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
