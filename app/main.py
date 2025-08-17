# type: ignore
import os
import time
import datetime
import docker
import logging
import aiofiles
import re
from urllib.parse import quote
import csv
import tempfile
from fastapi import FastAPI, HTTPException, Depends, status, Request, UploadFile, File, Form
from fastapi.responses import PlainTextResponse, HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.responses import FileResponse
from pydantic_settings import BaseSettings

# --- Configuration & Logging Setup ---
SHARED_PATH = "/app/shared_config"
SHARED_CONFIG_PATH = "/app/shared_config/telegraf.conf"
SHARED_NODES_PATH = "/app/shared_config/nodes.csv"

# DO NOT use logging.basicConfig() here. Uvicorn will manage the root logger.
# We just get a specific logger for our application.
logger = logging.getLogger("telegraf_manager")
# You can set the level here if you want to override Uvicorn's default
# logger.setLevel(logging.INFO) 

# --- MQTT Topic Character Exclusion List ---

MQTT_TOPIC_EXCLUSION_CHARS = [
    "+",    # Single-level wildcard character - illegal in topic names
    "#",    # Multi-level wildcard character - illegal in topic names
    "*",    # SMF wildcard character - causes interoperability issues
    ">",    # SMF wildcard character - causes interoperability issues
    "$",    # When used at start of topic - reserved for server implementation
    "!",    # When used at start of topic - causes interoperability issues in SMF (topic exclusions)
    # " "     # Space character - avoid as best practice to prevent parsing issues
]

# --- Base settings for configuration ---

class Settings(BaseSettings):
    telegraf_container_name: str = "telegraf"
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings()

# --- Application and Client Initialization ---

app = FastAPI(
    title="Telegraf Manager API",
    description="API for managing a Telegraf container.",
    version="1.4.1" # Version bump for interval change fix
)

# --- Logging Middleware ---
@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Logs every incoming request."""
    client_host = request.client.host
    logger.info(f"Request: {request.method} {request.url.path} from {client_host}")
    response = await call_next(request)
    logger.info(f"Response: {response.status_code} for {request.method} {request.url.path}")
    return response
# --- End of Middleware ---

templates = Jinja2Templates(directory="templates")

try:
    docker_client = docker.from_env()
    docker_client.ping()
    logger.info("Successfully connected to Docker daemon.")
except docker.errors.DockerException as e:
    logger.critical(f"Could not connect to Docker daemon. Error: {e}")
    exit(1)

START_TIME = time.time()

# --- Dependencies for Endpoints ---

def get_telegraf_container() -> docker.models.containers.Container:
    """Dependency to get the Telegraf container object."""
    logger.info(f"Attempting to get container '{settings.telegraf_container_name}'...")
    try:
        container = docker_client.containers.get(settings.telegraf_container_name)
        logger.info(f"Successfully retrieved container '{container.name}' (ID: {container.short_id}).")
        return container
    except docker.errors.NotFound:
        logger.error(f"Container '{settings.telegraf_container_name}' not found.")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Container '{settings.telegraf_container_name}' not found"
        )
    except docker.errors.APIError as e:
        logger.error(f"Docker API error while getting container: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Docker API error: {str(e)}"
        )

# --- Helper Functions ---

def get_telegraf_status_details(container: docker.models.containers.Container) -> dict:
    """A helper function to fetch, parse, and format the status of the Telegraf container."""
    logger.info(f"Getting status details for container '{container.name}'...")
    container.reload()
    state = container.attrs["State"]
    
    # ... (timestamp parsing logic remains the same) ...
    try:
        started_at_str = state["StartedAt"]
        if 'Z' in started_at_str: started_at_str = started_at_str.replace('Z', '+00:00')
        if '.' in started_at_str:
            parts = started_at_str.split('.')
            main_part = parts[0]
            frac_and_tz = parts[1]
            if '+' in frac_and_tz: frac_part, tz_part = frac_and_tz.split('+', 1); tz_part = '+' + tz_part
            elif '-' in frac_and_tz: frac_part, tz_part = frac_and_tz.split('-', 1); tz_part = '-' + tz_part
            else: frac_part, tz_part = frac_and_tz, ''
            started_at_str = f"{main_part}.{frac_part[:6]}{tz_part}"
        parsed_dt = datetime.datetime.fromisoformat(started_at_str)
        started_at = parsed_dt.replace(tzinfo=datetime.timezone.utc) if parsed_dt.tzinfo is None else parsed_dt
    except Exception as e:
        logger.warning(f"Could not parse 'StartedAt' timestamp '{state.get('StartedAt', 'N/A')}'. Using fallback. Error: {e}")
        started_at = datetime.datetime.now(datetime.timezone.utc)

    logs_raw = container.logs(tail=10, timestamps=True).decode("utf-8", errors="ignore").strip()
    # ... (log formatting logic remains the same) ...
    formatted_logs = []
    for line in logs_raw.split("\n"):
        if line:
            parts = line.split(" ", 1)
            formatted_logs.append({"timestamp": parts[0], "message": parts[1]} if len(parts) == 2 else {"message": line})

    health_check_data = state.get("Health")
    # ... (health check logic remains the same) ...
    health_check = None
    if health_check_data:
        health_check = {
            "status": health_check_data.get("Status"),
            "failing_streak": health_check_data.get("FailingStreak", 0),
            "last_log": health_check_data.get("Log", [])[-1] if health_check_data.get("Log") else "No health log available"
        }

    logger.info(f"Finished getting status for '{container.name}'. Status: {state.get('Status')}")
    return {
        "status": state.get("Status"),
        "running": state.get("Running", False),
        "started_at": started_at.isoformat(),
        "uptime": str(datetime.datetime.now(datetime.timezone.utc) - started_at),
        "health_check": health_check,
        "recent_logs": formatted_logs
    }

async def change_agent_interval(new_interval: str):
    """
    Reads, modifies line-by-line, and writes the telegraf.conf with a new agent interval.
    This approach is more robust than a single multi-line regex.
    """
    logger.info(f"Attempting to change agent interval to '{new_interval}' in '{SHARED_CONFIG_PATH}'.")

    try:
        async with aiofiles.open(SHARED_CONFIG_PATH, "r", encoding="utf-8") as f:
            lines = await f.readlines()
    except Exception as e:
        logger.error(f"Failed to read config file for modification: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to read config file: {e}")

    try:
        # Find the line index for [agent]
        agent_section_start_index = -1
        for i, line in enumerate(lines):
            if line.strip().lower() == '[agent]':
                agent_section_start_index = i
                break
        
        if agent_section_start_index == -1:
            raise ValueError("Config is malformed: '[agent]' section not found.")

        # Starting from just after '[agent]', find and replace the 'interval' line
        found_and_replaced = False
        interval_pattern = re.compile(r"^\s*interval\s*=\s*", re.IGNORECASE)
        for i in range(agent_section_start_index + 1, len(lines)):
            line = lines[i]
            # If we hit another section, stop searching
            if line.strip().startswith('['):
                break
            
            if interval_pattern.match(line.strip()):
                indentation = line[:len(line) - len(line.lstrip())]
                lines[i] = f'{indentation}interval = "{new_interval}"\n'
                found_and_replaced = True
                logger.info(f"Found and replaced interval line. Old: '{line.strip()}', New: '{lines[i].strip()}'")
                break # Essential: only replace the first one

        if not found_and_replaced:
            raise ValueError("Config is malformed: 'interval' key not found in '[agent]' section.")

    except ValueError as e:
        logger.error(str(e))
        raise HTTPException(status_code=400, detail=str(e))

    # Write the modified lines back to the file
    try:
        async with aiofiles.open(SHARED_CONFIG_PATH, "w", encoding="utf-8") as f:
            await f.writelines(lines)
        logger.info(f"Successfully updated agent interval to '{new_interval}'.")
    except Exception as e:
        logger.error(f"Failed to write updated config file: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to write updated config file: {e}")

def check_csv_exists() -> bool:
    """
    Checks if the shared nodes CSV file exists.
    """
    try:
        file_exists = os.path.exists(SHARED_NODES_PATH)
    except Exception as e:
        logger.error(f"Error checking if CSV file exists: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error checking CSV file existence: {e}")
    return file_exists

# --- Telegraf configuration generator class ---

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
                        logger.warning(f"Row missing required columns: {row}")
            
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


# --- API Endpoints ---

@app.get("/", response_class=HTMLResponse, tags=["Web Interface"])
async def root(request: Request):
    """Serves the main web interface dashboard."""
    logger.info("Rendering main dashboard.")
    uptime_seconds = time.time() - START_TIME
    app_status = {
        "status": "healthy",
        "uptime": str(datetime.timedelta(seconds=int(uptime_seconds))),
        "started_at": datetime.datetime.fromtimestamp(START_TIME).isoformat()
    }
    success_message = None
    error_message = None

    # backups = [f for f in os.listdir(SHARED_PATH) if f.startswith("nodes_backup_") and f.endswith(".csv")]
    # backups.sort(reverse=True) 

    backups = []
    for f in os.listdir(SHARED_PATH):
        if f.startswith("nodes_backup_") and f.endswith(".csv"):
            timestamp_str = f[13:-4]  # Slice from after "nodes_backup_" (13 chars) to before ".csv" (4 chars)
            try:
                dt = datetime.datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
                formatted = dt.strftime("%Y-%m-%d %H:%M:%S")
                backups.append({"filename": f, "date": formatted})
            except ValueError:
                logger.warning(f"Invalid timestamp in backup filename: {f}")
                continue  # Skip files with malformed timestamps
    backups.sort(key=lambda x: x["date"], reverse=True)  # Sort newest first by date string
        
    if request.query_params.get('success') == 'csv_uploaded':
        success_message = "CSV file uploaded, configuration generated, and Telegraf applied successfully."
    if request.query_params.get('error'):
        error_message = request.query_params.get('error')
    try:
        container = get_telegraf_container()
        telegraf_status = get_telegraf_status_details(container)
        file_exists = check_csv_exists()
    except HTTPException as e:
        telegraf_status = {"status": "error", "error": e.detail}
    except Exception as e:
        logger.error(f"Unexpected error on root path while getting container status or nodes csv file check: {e}", exc_info=True)
        telegraf_status = {"status": "error", "error": "An unexpected server error occurred."}
    return templates.TemplateResponse("index.html", {
        "request": request,
        "fastapi_status": app_status,
        "telegraf_status": telegraf_status,
        "container_name": settings.telegraf_container_name,
        "success_message": success_message,
        "error_message": error_message,
        "file_exists": file_exists,
        "backups": backups,
    })

@app.get("/telegraf/config", response_class=PlainTextResponse, tags=["Telegraf"])
async def telegraf_config():
    """Retrieves the current 'telegraf.conf' from the shared volume."""
    logger.info(f"Reading config from shared path '{SHARED_CONFIG_PATH}'.")
    try:
        async with aiofiles.open(SHARED_CONFIG_PATH, "r") as f:
            content = await f.read()
        return content
    except FileNotFoundError:
        logger.error(f"Config file not found at '{SHARED_CONFIG_PATH}'.")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Telegraf config file not found."
        )
    except Exception as e:
        logger.error(f"Failed to read config file: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to read Telegraf config: {e}"
        )

@app.post("/telegraf/set-interval", tags=["Telegraf"])
async def set_telegraf_interval(
    request: Request,
    interval: str = Form(...),
    container: docker.models.containers.Container = Depends(get_telegraf_container)
):
    """
    Sets the agent interval in the telegraf.conf file and restarts the container.
    """
    logger.info(f"Received request to set agent interval to '{interval}'.")

    # 1. Validate the input format (e.g., "10s", "1m", "2h")
    if not re.match(r"^\d+[smh]$", interval):
        error_message = f"Invalid interval format: '{interval}'. Must be a number followed by 's', 'm', or 'h'."
        logger.warning(error_message)
        # URL-encode the error message to handle special characters
        safe_error_message = quote(error_message)
        return RedirectResponse(url=f"/?error={safe_error_message}", status_code=status.HTTP_303_SEE_OTHER)

    # 2. Modify the configuration file
    try:
        await change_agent_interval(interval)
    except HTTPException as e:
        # If the file modification fails, redirect with that error
        safe_error_message = quote(e.detail)
        return RedirectResponse(url=f"/?error={safe_error_message}", status_code=status.HTTP_303_SEE_OTHER)

    # 3. Restart the container to apply the changes
    try:
        logger.info("Restarting Telegraf container to apply new interval...")
        container.restart(timeout=10)
        logger.info("Telegraf container restarted successfully.")
    except docker.errors.APIError as e:
        logger.error(f"Interval updated, but failed to restart container: {e}", exc_info=True)
        safe_error_message = quote(f"Interval updated, but container restart failed: {e}")
        return RedirectResponse(url=f"/?error={safe_error_message}", status_code=status.HTTP_303_SEE_OTHER)

    return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)

@app.post("/telegraf/upload-config", tags=["Telegraf"])
async def upload_telegraf_config(
    request: Request,
    file: UploadFile = File(...),
    container: docker.models.containers.Container = Depends(get_telegraf_container)
):
    """
    Uploads a new 'telegraf.conf' and automatically restarts or starts the container.
    """
    logger.info(f"Received upload request for new configuration: '{file.filename}'.")
    content = await file.read()
    if not content:
        logger.warning("Upload failed: file is empty.")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Uploaded file is empty.")

    # 1. Write the new config file to the shared volume
    try:
        async with aiofiles.open(SHARED_CONFIG_PATH, "wb") as f:
            await f.write(content)
        logger.info(f"Successfully wrote new configuration to '{SHARED_CONFIG_PATH}'.")
    except Exception as e:
        logger.error(f"Failed to write configuration to '{SHARED_CONFIG_PATH}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to write configuration file.")
    
    # 2. Apply the change by starting or restarting the container
    try:
        container.reload() # Get the latest status
        if container.status == 'running':
            logger.info("Container is running. Restarting to apply new config...")
            container.restart()
        else:
            logger.info(f"Container is '{container.status}'. Starting with new config...")
            container.start()
        logger.info("Successfully applied new configuration to Telegraf container.")
    except docker.errors.APIError as e:
        logger.error(f"Config uploaded, but failed to start/restart container: {e}", exc_info=True)
        # Let the user know the config was written but the apply step failed
        raise HTTPException(status_code=500, detail=f"Config uploaded, but container action failed: {e}")
        
    return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)

@app.post("/telegraf/restart", tags=["Telegraf"])
async def restart_telegraf(
    request: Request,
    container: docker.models.containers.Container = Depends(get_telegraf_container)
):
    """Restarts the Telegraf container."""
    logger.info(f"Received restart request for container '{container.name}'.")
    container.reload()
    if not container.attrs["State"]["Running"]:
        logger.warning(f"Cannot restart container '{container.name}': not running.")
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Container is not running, cannot restart.")

    start_time_before = container.attrs["State"]["StartedAt"]
    logger.info(f"Restarting container '{container.name}' (was started at {start_time_before}).")
    container.restart(timeout=10)
    
    # Wait a moment for the container state to update
    time.sleep(2)
    container.reload()
    start_time_after = container.attrs["State"]["StartedAt"]
    
    if container.status == "restarting" or start_time_before == start_time_after:
        logger.error(f"Restart command issued for '{container.name}', but failed to confirm restart. New start time: {start_time_after}")
        raise HTTPException(status_code=500, detail="Container restart command issued, but it failed to restart.")
    
    logger.info(f"Container '{container.name}' restarted successfully. New start time: {start_time_after}.")
    
    if "application/x-www-form-urlencoded" in request.headers.get("content-type", ""):
        return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)
    return {"success": True, "message": "Telegraf container restarted successfully."}

@app.post("/telegraf/stop", tags=["Telegraf"])
async def stop_telegraf(
    request: Request,
    container: docker.models.containers.Container = Depends(get_telegraf_container)
):
    """Stops the Telegraf container, breaking any restart loops."""
    logger.info(f"Received stop request for container '{container.name}'.")
    try:
        container.stop(timeout=10)
        logger.info(f"Container '{container.name}' stopped successfully.")
    except Exception as e:
        logger.error(f"Failed to stop container '{container.name}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to stop container: {e}")

    if "application/x-www-form-urlencoded" in request.headers.get("content-type", ""):
        return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)
    return {"success": True, "message": "Telegraf container stopped."}

@app.post("/telegraf/start", tags=["Telegraf"])
async def start_telegraf(
    request: Request,
    container: docker.models.containers.Container = Depends(get_telegraf_container)
):
    """Starts the Telegraf container."""
    logger.info(f"Received start request for container '{container.name}'.")
    try:
        container.start()
        logger.info(f"Container '{container.name}' started successfully.")
    except Exception as e:
        logger.error(f"Failed to start container '{container.name}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to start container: {e}")

    if "application/x-www-form-urlencoded" in request.headers.get("content-type", ""):
        return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)
    return {"success": True, "message": "Telegraf container started."}

# TODO fix this later
@app.post("/telegraf/upload-csv", tags=["Telegraf"])
async def upload_csv_for_config(
    request: Request,
    file: UploadFile = File(...),
    mqtt_broker: str = Form("tcp://mosquitto:1883"),
    opcua_endpoint: str = Form("opc.tcp://100.94.111.58:4841"),
    influxdb_url: str = Form("http://influxdb:8086"),
    container: docker.models.containers.Container = Depends(get_telegraf_container)
):
    logger.info(f"Received CSV upload for config generation: {file.filename}")
    content = await file.read()

    if not content:
        safe_error = quote("Empty file")
        return RedirectResponse(url=f"/?error={safe_error}", status_code=303)

    try:
        # # Save uploaded content to a temp file
        # with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as temp_csv:
        #     temp_csv.write(content)
        #     csv_path = temp_csv.name

        nodes_csv_backed_up = False
        try:
            logger.info(f"Writing uploaded CSV content to {SHARED_NODES_PATH}.")
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            try:
                if check_csv_exists():
                    os.rename(SHARED_NODES_PATH, SHARED_PATH + f"/nodes_backup_{timestamp}.csv")
                    nodes_csv_backed_up = True
                else:
                    logger.warning(f"{SHARED_NODES_PATH} does not exist, skipping backup.")
            except:
                logger.error(f"Failed to back up existing CSV file at {SHARED_NODES_PATH}.", exc_info=True)
            with open(SHARED_NODES_PATH, 'wb') as nodes_csv:
                nodes_csv.write(content)
        except Exception as e:
            if nodes_csv_backed_up:
                os.rename(SHARED_PATH + f"/nodes_backup_{timestamp}.csv", SHARED_NODES_PATH)
            logger.error(f"Failed to write uploaded CSV content to {SHARED_NODES_PATH}: {e}", exc_info=True)
            return RedirectResponse(url="/?error=Failed to write uploaded CSV content", status_code=303)   

        generator = TelegrafConfigGenerator(
            csv_file_path=SHARED_NODES_PATH,
            output_file_path=SHARED_CONFIG_PATH,
            mqtt_broker=mqtt_broker,
            opcua_endpoint=opcua_endpoint,
            influxdb_url=influxdb_url
        )
        generator.run()  # Generates and writes the config to SHARED_CONFIG_PATH

        # # Clean up temp file
        # os.unlink(csv_path)
        

    except Exception as e:
        logger.error(f"Config generation: {e}", exc_info=True)
        if os.path.exists(csv_path):
            os.unlink(csv_path)
        safe_error = quote(str(e))
        return RedirectResponse(url=f"/?error={safe_error}", status_code=303)

    try:
        container.reload()
        if container.status == 'running':
            container.restart(timeout=30)
        else:
            container.start()
        
    except Exception as e:
        safe_error = quote(f"Config generated, but apply failed: {e}")
        return RedirectResponse(url=f"/?error={safe_error}", status_code=303)

    # return RedirectResponse(url="/", status_code=303)
    return RedirectResponse(url="/?success=csv_uploaded", status_code=303)

@app.get("/export_nodes_csv")
async def export_nodes_csv():
    logger.info("Exporting nodes CSV file.")
    try:
        return FileResponse(SHARED_NODES_PATH, filename="nodes.csv")
    except:
        logger.error(f"Failed to export nodes CSV file from {SHARED_NODES_PATH}.")
        raise HTTPException(status_code=404, detail="Nodes CSV file not found.")

@app.get("/export_backup/{filename}")
async def export_backup(filename: str):
    if not filename.startswith("nodes_backup_") or not filename.endswith(".csv"):
        raise HTTPException(status_code=404, detail="File not found")
    
    file_path = os.path.join(SHARED_PATH, filename)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")
    
    logger.info(f"Exporting backup CSV file: {filename}")
    return FileResponse(file_path, filename=filename)

@app.get("/csvdata", response_class=HTMLResponse, tags=["CSV"])
async def get_csv_data(request: Request):
    if not os.path.exists(SHARED_NODES_PATH):
        raise HTTPException(status_code=404, detail="CSV file not found.")
    
    with open(SHARED_NODES_PATH, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader, None)  # Get headers; None if empty
        rows = list(reader)  # Get all data rows
    
    return templates.TemplateResponse("csv_table.html", {
        "request": request,
        "headers": headers or [],
        "rows": rows
    })