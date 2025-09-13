import os
import time
import datetime
import logging
import csv
from tomlkit import document, table, aot, comment, nl, integer, boolean, string

logger = logging.getLogger("telegraf_manager.telegraf_config_generator")

MQTT_TOPIC_EXCLUSION_CHARS = [
    "+",    # Single-level wildcard character - illegal in topic names
    "#",    # Multi-level wildcard character - illegal in topic names
    "*",    # SMF wildcard character - causes interoperability issues
    ">",    # SMF wildcard character - causes interoperability issues
    "$",    # When used at start of topic - reserved for server implementation
    "!",    # When used at start of topic - causes interoperability issues in SMF (topic exclusions)
    # " "     # Space character - avoid as best practice to prevent parsing issues
]

class TelegrafConfigGenerator:
    """
    Generates Telegraf configuration for OPC UA nodes from a CSV file,
    with proper MQTT topic validation and sanitization to ensure compatibility.
    Exports each OPC UA node to a specific MQTT topic and InfluxDB.
    """
    def __init__(self, 
                 csv_file_path: str, 
                 output_file_path: str, 
                 mqtt_broker: str = "tcp://mosquitto:1883",
                 opcua_endpoint: str = "opc.tcp://100.94.111.58:4841",
                 influxdb_url: str = "http://64.226.126.250:8086"):
        
        if not csv_file_path:
            raise ValueError("CSV file path cannot be empty.")
        if not output_file_path:
            raise ValueError("Output file path cannot be empty.")
            
        self.csv_file_path = csv_file_path
        self.output_file_path = output_file_path
        self.mqtt_broker = mqtt_broker
        self.opcua_endpoint = opcua_endpoint
        self.influxdb_url = influxdb_url
        self._start_time = 0.0
        self._total_nodes_processed = 0
        self._topics_sanitized = 0
    
    def validate_mqtt_topic(self, topic_name: str) -> bool:
        """
        Validates an MQTT topic name against character restrictions.
        Returns True if valid, False otherwise.
        """
        # Check for reserved characters
        if any(char in topic_name for char in MQTT_TOPIC_EXCLUSION_CHARS):
            return False
        
        # Check for leading $ (allowed only for system topics)
        if topic_name.startswith("$") and not (
            topic_name.startswith("$SYS/") or 
            topic_name.startswith("$share/") or
            topic_name.startswith("$noexport/")
        ):
            return False
        
        # Check length restriction (250 bytes max per Solace docs)
        if len(topic_name.encode('utf-8')) > 250:
            return False
        
        # Check for level count restriction (128 levels max per Solace docs)
        if len(topic_name.split('/')) > 128:
            return False
            
        return True
    
    def sanitize_mqtt_topic(self, topic_name: str) -> str:
        """
        Sanitizes an MQTT topic name by replacing restricted characters.
        """
        # Replace restricted characters
        sanitized_name = topic_name
        for char in MQTT_TOPIC_EXCLUSION_CHARS:
            sanitized_name = sanitized_name.replace(char, '_')
        
        # Handle leading $ if not a system topic
        if sanitized_name.startswith("$") and not (
            sanitized_name.startswith("$SYS/") or 
            sanitized_name.startswith("$share/") or
            sanitized_name.startswith("$noexport/")
        ):
            sanitized_name = "_" + sanitized_name[1:]
            
        return sanitized_name
    
    def generate_telegraf_config(self) -> None:
        """
        Process the CSV file of OPC UA nodes and generate a Telegraf configuration
        with proper MQTT topic validation/sanitization and InfluxDB output settings.
        """
        # Check if CSV file exists
        if not os.path.exists(self.csv_file_path):
            logger.error(f"Error: CSV file '{self.csv_file_path}' not found.")
            return
        
        # Read nodes from CSV file
        nodes = []
        try:
            logger.info(f"Reading nodes from CSV file: {self.csv_file_path}")
            with open(self.csv_file_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    # Ensure required columns exist
                    if all(key in row for key in ['NodeId', 'MQTTCustomName']):
                        # Extract namespace and identifier from NodeId
                        node_id_parts = row['NodeId'].split(';')
                        if len(node_id_parts) != 2:
                            logger.warning(f"Skipping invalid NodeId format: {row['NodeId']}")
                            continue
                        
                        namespace = node_id_parts[0].replace('ns=', '')
                        identifier = node_id_parts[1].replace('s=', '')
                        mqtt_custom_name = row['MQTTCustomName']
                        
                        # Generate MQTT topic name using the MQTTCustomName and validate/sanitize it
                        mqtt_topic = f"telegraf/opcua/{mqtt_custom_name}"
                        if not self.validate_mqtt_topic(mqtt_topic):
                            original_topic = mqtt_topic
                            mqtt_topic = self.sanitize_mqtt_topic(mqtt_topic)
                            self._topics_sanitized += 1
                            logger.warning(f"MQTT topic '{original_topic}' contains restricted characters. "
                                          f"Using sanitized topic: '{mqtt_topic}'")
                        
                        nodes.append({
                            'node_id': row['NodeId'],
                            'mqtt_custom_name': mqtt_custom_name,
                            'namespace': namespace,
                            'identifier': identifier,
                            'identifier_type': 's',  # Assuming all are string type as per example
                            'mqtt_topic': mqtt_topic
                        })
                        self._total_nodes_processed += 1
                    else:
                        missing_columns = [key for key in ['NodeId', 'MQTTCustomName'] if key not in row or not row[key]]
                        logger.warning(f"Row missing required columns {missing_columns}: {row}")
            
            logger.info(f"Successfully processed {self._total_nodes_processed} nodes from CSV.")
            logger.info(f"Sanitized {self._topics_sanitized} MQTT topics with restricted characters.")
            
        except Exception as e:
            logger.error(f"Error reading CSV file: {e}", exc_info=True)
            return
        
        # Generate Telegraf configuration using tomlkit
        doc = document()
        
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Add header comments
        doc.add(comment(f"Generated at {timestamp}"))
        doc.add(comment("Telegraf Configuration for OPC UA Monitoring"))
        doc.add(comment("Generated from CSV file"))
        doc.add(nl())
        
        # Add agent section
        doc.add(comment("###############################################################################"))
        doc.add(comment("#                            AGENT SETTINGS                                   #"))
        doc.add(comment("###############################################################################"))
        
        agent = table()
        agent.add("interval", "10s")
        agent.add("round_interval", boolean(True))
        agent.add("metric_batch_size", integer(1000))
        agent.add("metric_buffer_limit", integer(10000))
        agent.add("collection_jitter", "0s")
        agent.add("flush_interval", "10s")
        agent.add("flush_jitter", "0s")
        agent.add("precision", "")
        agent.add("hostname", "")
        agent.add("omit_hostname", boolean(True))
        doc.add("agent", agent)
        doc.add(nl())
        
        # Add inputs section
        doc.add(comment("###############################################################################"))
        doc.add(comment("#                            INPUT PLUGINS                                    #"))
        doc.add(comment("###############################################################################"))
        doc.add(nl())
        doc.add(comment("# Read data from an OPC UA server"))
        
        inputs = table()
        opcua_aot = aot()
        opcua = table()
        opcua.add("endpoint", self.opcua_endpoint).comment("Replace with your OPC UA Server URL")
        opcua.add("security_policy", "None").comment('"None", "Basic128Rsa15", "Basic256", "Basic256Sha256".')
        opcua.add("security_mode", "None").comment('"None", "Sign", "SignAndEncrypt".')
        opcua.add("certificate", "").comment('Path to certificate file (Required if SecurityMode != "None").')
        opcua.add("private_key", "").comment('Path to private key file (Required if SecurityMode != "None").')
        opcua.add("auth_method", "Anonymous").comment('"Anonymous", "UserName", "Certificate".')
        opcua.add("connect_timeout", "10s").comment("Connection timeout for establishing the OPC UA connection.")
        opcua.add("request_timeout", "5s").comment("Request timeout for individual OPC UA read requests.")
        doc.add(nl())
        opcua.add(comment("## Node Configuration: Define the OPC UA nodes to read data from."))
        
        nodes_aot = aot()
        for node in nodes:
            node_table = table()
            node_table.add("name", node["mqtt_custom_name"])
            node_table.add("namespace", node["namespace"])
            node_table.add("identifier_type", node["identifier_type"])
            node_table.add("identifier", string(node["identifier"], multiline=True))
            nodes_aot.append(node_table)
        
        opcua.add("nodes", nodes_aot)
        opcua_aot.append(opcua)
        inputs.add("opcua", opcua_aot)
        doc.add("inputs", inputs)
        doc.add(nl())
        
        # Add outputs section
        doc.add(comment("###############################################################################"))
        doc.add(comment("#                            OUTPUT PLUGINS                                   #"))
        doc.add(comment("###############################################################################"))
        doc.add(nl())
        
        outputs = table()
        
        # InfluxDB output
        doc.add(comment("# --- InfluxDB v2 Output ---"))
        influxdb_v2_aot = aot()
        influxdb_v2 = table()
        influxdb_v2.add("urls", [self.influxdb_url]).comment("Replace with your InfluxDB URL")
        influxdb_v2.add("token", "$DOCKER_INFLUXDB_INIT_ADMIN_TOKEN").comment("Replace with your InfluxDB Token or env var")
        influxdb_v2.add("organization", "$DOCKER_INFLUXDB_INIT_ORG").comment("Replace with your InfluxDB Org or env var")
        influxdb_v2.add("bucket", "OPC UA")
        influxdb_v2_aot.append(influxdb_v2)
        outputs.add("influxdb_v2", influxdb_v2_aot)
        doc.add(nl())
        
        # MQTT outputs
        doc.add(comment("# --- MQTT Outputs: One per Node (Filtering on 'id' tag) ---"))
        doc.add(nl())
        
        mqtt_aot = aot()
        for node in nodes:
            doc.add(comment(f"# MQTT Output for Node: {node['identifier']}"))
            mqtt = table()
            mqtt.add("servers", [self.mqtt_broker])
            mqtt.add("topic", node["mqtt_topic"])
            mqtt.add("tagpass", {"id": [f"ns={node['namespace']};s={node['identifier']}"]})
            mqtt.add("qos", integer(0))
            mqtt.add("retain", boolean(False))
            mqtt.add("data_format", "template")
            mqtt.add("template", f'{{{{ .Field \\"{node["mqtt_custom_name"]}\\" }}}}')
            mqtt_aot.append(mqtt)
        
        outputs.add("mqtt", mqtt_aot)
        doc.add("outputs", outputs)
        
        # Write configuration to file
        try:
            logger.info(f"Writing Telegraf configuration to {self.output_file_path}")
            with open(self.output_file_path, 'w', encoding='utf-8') as f:
                f.write(doc.as_string())
            logger.info(f"Configuration successfully written to {self.output_file_path}")
        except Exception as e:
            logger.error(f"Error writing configuration file: {e}", exc_info=True)
    
    def run(self) -> None:
        """
        Orchestrates the entire process of generating the Telegraf configuration.
        """
        self._start_time = time.time()
        logger.info("Starting Telegraf configuration generation...")
        
        try:
            # Generate the Telegraf configuration
            self.generate_telegraf_config()
            
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}", exc_info=True)
        finally:
            total_duration = time.time() - self._start_time
            logger.info(f"Process finished. Total time: {total_duration:.2f} seconds.")
            logger.info("--- Generation Summary ---")
            logger.info(f"Total nodes processed: {self._total_nodes_processed}")
            logger.info(f"MQTT topics sanitized: {self._topics_sanitized}")
            logger.info(f"Configuration file: {self.output_file_path}")
            logger.info("-------------------------")