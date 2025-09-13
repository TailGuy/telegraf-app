import os
import time
import datetime
import logging
import csv

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
        


        # Generate Telegraf configuration
        config = []
        
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Add agent section
        config.append(f"""# Generated at {timestamp}
# Telegraf Configuration for OPC UA Monitoring
# Generated from CSV file

###############################################################################
#                            AGENT SETTINGS                                   #
###############################################################################
[agent]
  interval = "10s"
  round_interval = true
  metric_batch_size = 1000
  metric_buffer_limit = 10000
  collection_jitter = "0s"
  flush_interval = "10s"
  flush_jitter = "0s"
  precision = ""
  hostname = ""
  omit_hostname = true
""")
        
        # Add OPC UA input plugin
        config.append(f"""
###############################################################################
#                            INPUT PLUGINS                                    #
###############################################################################

# Read data from an OPC UA server
[[inputs.opcua]]
  ## OPC UA Server Endpoint URL.
  endpoint = "{self.opcua_endpoint}" # Replace with your OPC UA Server URL

  ## Security policy: "None", "Basic128Rsa15", "Basic256", "Basic256Sha256".
  security_policy = "None"
  ## Security mode: "None", "Sign", "SignAndEncrypt".
  security_mode = "None"

  ## Path to certificate file (Required if SecurityMode != "None").
  certificate = ""
  ## Path to private key file (Required if SecurityMode != "None").
  private_key = ""

  ## Authentication method: "Anonymous", "UserName", "Certificate".
  auth_method = "Anonymous"
  # username = "" # Required if AuthMethod="UserName"
  # password = "" # Required if AuthMethod="UserName"

  ## Connection timeout for establishing the OPC UA connection.
  connect_timeout = "10s"
  ## Request timeout for individual OPC UA read requests.
  request_timeout = "5s"

  ## Node Configuration: Define the OPC UA nodes to read data from.
""")
        
        # Add node configurations
        for node in nodes:
            config.append(f"""  [[inputs.opcua.nodes]]
    name = "{node['mqtt_custom_name']}"
    namespace = "{node['namespace']}"
    identifier_type = "{node['identifier_type']}"
    identifier = '''{node['identifier']}'''
""")
        
        # Add InfluxDB output plugin
        config.append(f"""              
###############################################################################
#                            OUTPUT PLUGINS                                   #
###############################################################################

# --- InfluxDB v2 Output ---
[[outputs.influxdb_v2]]
  urls = ["{self.influxdb_url}"] # Replace with your InfluxDB URL
  token = "$DOCKER_INFLUXDB_INIT_ADMIN_TOKEN" # Replace with your InfluxDB Token or env var
  organization = "$DOCKER_INFLUXDB_INIT_ORG" # Replace with your InfluxDB Org or env var
  bucket = "OPC UA"
""")
        
        # Add MQTT output plugins for each node
        config.append("""
# --- MQTT Outputs: One per Node (Filtering on 'id' tag) ---
""")
        
        for node in nodes:
            config.append(f'''# MQTT Output for Node: {node['identifier']}
[[outputs.mqtt]]
  servers = ["{self.mqtt_broker}"]
  topic = "{node['mqtt_topic']}"
  tagpass = {{ id = ["ns={node['namespace']};s={node['identifier']}"] }}
  qos = 0
  retain = false
  data_format = "template"
  template = "{{{{ .Field \\"{node['mqtt_custom_name']}\\" }}}}"
''')
        
        # Write configuration to file
        try:
            logger.info(f"Writing Telegraf configuration to {self.output_file_path}")
            with open(self.output_file_path, 'w', encoding='utf-8') as f:
                f.write(''.join(config))
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

