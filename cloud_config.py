import os
import tempfile


_KAFKA_SSL_CAFILE = None


def get_kafka_ssl_cafile():
    global _KAFKA_SSL_CAFILE

    ssl_cafile = os.getenv("KAFKA_SSL_CAFILE")
    if ssl_cafile:
        return ssl_cafile

    ssl_ca_pem = os.getenv("KAFKA_SSL_CA_PEM")
    if not ssl_ca_pem:
        return None

    if _KAFKA_SSL_CAFILE:
        return _KAFKA_SSL_CAFILE

    ca_file = tempfile.NamedTemporaryFile(
        mode="w",
        prefix="kafka-ca-",
        suffix=".pem",
        delete=False,
    )
    ca_file.write(ssl_ca_pem.replace("\\n", "\n"))
    ca_file.close()
    _KAFKA_SSL_CAFILE = ca_file.name
    return _KAFKA_SSL_CAFILE


def get_kafka_bootstrap_servers():
    servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    return [server.strip() for server in servers.split(",") if server.strip()]


def get_kafka_producer_config(value_serializer):
    config = {
        "bootstrap_servers": get_kafka_bootstrap_servers(),
        "value_serializer": value_serializer,
    }

    username = os.getenv("KAFKA_USERNAME")
    password = os.getenv("KAFKA_PASSWORD")
    if username and password:
        config.update(
            {
                "security_protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
                "sasl_mechanism": os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
                "sasl_plain_username": username,
                "sasl_plain_password": password,
            }
        )

    ssl_cafile = get_kafka_ssl_cafile()
    if ssl_cafile:
        config["ssl_cafile"] = ssl_cafile

    return config


def get_spark_kafka_options():
    options = {
        "kafka.bootstrap.servers": ",".join(get_kafka_bootstrap_servers()),
        "subscribe": os.getenv("KAFKA_TOPICS", "aqi-data,traffic-data,weather-data"),
        "startingOffsets": os.getenv("KAFKA_STARTING_OFFSETS", "latest"),
    }

    username = os.getenv("KAFKA_USERNAME")
    password = os.getenv("KAFKA_PASSWORD")
    if username and password:
        mechanism = os.getenv("KAFKA_SASL_MECHANISM", "PLAIN")
        options.update(
            {
                "kafka.security.protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
                "kafka.sasl.mechanism": mechanism,
                "kafka.sasl.jaas.config": (
                    "org.apache.kafka.common.security.plain.PlainLoginModule "
                    f'required username="{username}" password="{password}";'
                ),
            }
        )

    ssl_ca_pem = os.getenv("KAFKA_SSL_CA_PEM")
    if ssl_ca_pem:
        options.update(
            {
                "kafka.ssl.truststore.type": "PEM",
                "kafka.ssl.truststore.certificates": ssl_ca_pem.replace("\\n", "\n"),
            }
        )

    return options


def get_elasticsearch_url():
    return os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")


def get_elasticsearch_index():
    return os.getenv("ELASTICSEARCH_INDEX", "smart-city-unified")


def get_elasticsearch_auth():
    username = os.getenv("ELASTICSEARCH_USERNAME")
    password = os.getenv("ELASTICSEARCH_PASSWORD")
    if username and password:
        return username, password
    return None
