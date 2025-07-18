import os
import time
import datetime
import docker
import io
import tarfile
import logging
import aiofiles
import re
from urllib.parse import quote

from fastapi import FastAPI, HTTPException, Depends, status, Request, UploadFile, File, Form
from fastapi.responses import PlainTextResponse, HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pydantic_settings import BaseSettings

# --- Configuration & Logging Setup ---

SHARED_CONFIG_PATH = "/app/shared_config/telegraf.conf"

# DO NOT use logging.basicConfig() here. Uvicorn will manage the root logger.
# We just get a specific logger for our application.
logger = logging.getLogger("telegraf_manager")
# You can set the level here if you want to override Uvicorn's default
# logger.setLevel(logging.INFO) 

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
    try:
        container = get_telegraf_container()
        telegraf_status = get_telegraf_status_details(container)
    except HTTPException as e:
        telegraf_status = {"status": "error", "error": e.detail}
    except Exception as e:
        logger.error(f"Unexpected error on root path while getting container status: {e}", exc_info=True)
        telegraf_status = {"status": "error", "error": "An unexpected server error occurred."}
    return templates.TemplateResponse("index.html", {
        "request": request,
        "fastapi_status": app_status,
        "telegraf_status": telegraf_status,
        "container_name": settings.telegraf_container_name
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