import os
import time
import datetime
import docker
import io
import tarfile
from fastapi import FastAPI, HTTPException, Depends, Header, status, Request, UploadFile, File
from fastapi.responses import PlainTextResponse, HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from typing import Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize the FastAPI app
app = FastAPI(
    title="Telegraf Manager API",
    description="API for managing Telegraf container",
    version="1.0.0"
)

# Set up templates
templates = Jinja2Templates(directory="templates")

# Start time of the application for uptime calculation
START_TIME = time.time()

# Initialize Docker client
docker_client = docker.from_env()

# Container name
TELEGRAF_CONTAINER_NAME = "telegraf"

# Helper function to get the Telegraf container
def get_telegraf_container():
    try:
        container = docker_client.containers.get(TELEGRAF_CONTAINER_NAME)
        return container
    except docker.errors.NotFound:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Telegraf container '{TELEGRAF_CONTAINER_NAME}' not found"
        )
        
    except docker.errors.APIError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Docker API error: {str(e)}"
        )

# Simple API key authentication for the restart endpoint
def verify_api_key(api_key: Optional[str] = Header(None, alias="X-API-Key")):
    # Get the API key from environment variable or use a default for development
    expected_api_key = os.environ.get("API_KEY")
    
    # If API_KEY is not set in environment, skip authentication
    if not expected_api_key:
        return True
        
    if api_key != expected_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key"
        )
    return True

# Health status endpoint - moved from root to /status
@app.get("/status", tags=["Health"])
async def health():
    uptime_seconds = time.time() - START_TIME
    uptime = str(datetime.timedelta(seconds=int(uptime_seconds)))
    
    return {
        "status": "healthy",
        "uptime": uptime,
        "uptime_seconds": uptime_seconds,
        "started_at": datetime.datetime.fromtimestamp(START_TIME).isoformat()
    }

# Root endpoint - Simple web interface using templates
@app.get("/", response_class=HTMLResponse, tags=["Web Interface"])
async def root(request: Request):
    # Get FastAPI status
    uptime_seconds = time.time() - START_TIME
    uptime = str(datetime.timedelta(seconds=int(uptime_seconds)))
    
    fastapi_status = {
        "status": "healthy",
        "uptime": uptime,
        "started_at": datetime.datetime.fromtimestamp(START_TIME).isoformat()
    }
    
    # Get Telegraf status
    try:
        container = get_telegraf_container()
        container_info = container.attrs
        state = container_info["State"]
        
        # Get container logs (last 10 entries)
        logs = container.logs(tail=10, timestamps=True).decode("utf-8").strip()
        
        # Format logs
        formatted_logs = []
        if logs:
            log_lines = logs.split("\n")
            for log in log_lines:
                if log:
                    # Try to split timestamp and message
                    parts = log.split(" ", 1)
                    if len(parts) == 2:
                        timestamp, message = parts
                        formatted_logs.append({"timestamp": timestamp, "message": message})
                    else:
                        formatted_logs.append({"message": log})
        
        # Get health check results if available
        health_check = None
        if "Health" in state:
            health_check = {
                "status": state["Health"]["Status"],
                "failing_streak": state["Health"]["FailingStreak"],
                "last_check": state["Health"]["Log"][-1] if state["Health"]["Log"] else None
            }
        
        # Calculate last restart time
        started_at_str = state["StartedAt"]
        
        # Handle timestamp format
        try:
            if started_at_str.endswith('Z'):
                started_at_str = started_at_str.replace("Z", "+00:00")
            
            if '+' in started_at_str:
                parts = started_at_str.split('+')
                timestamp_part = parts[0]
                timezone_part = '+' + parts[1]
                
                if '.' in timestamp_part:
                    main_part, fractional_part = timestamp_part.split('.')
                    fractional_part = fractional_part[:6]
                    timestamp_part = f"{main_part}.{fractional_part}"
                
                started_at_str = timestamp_part + timezone_part
            
            started_at = datetime.datetime.fromisoformat(started_at_str)
        except ValueError:
            started_at = datetime.datetime.now(datetime.timezone.utc)
        
        telegraf_status = {
            "status": state["Status"],
            "running": state["Running"],
            "health_check": health_check,
            "uptime": str(datetime.datetime.now(datetime.timezone.utc) - started_at),
            "started_at": started_at.isoformat(),
            "last_restart": started_at.isoformat(),
            "recent_logs": formatted_logs
        }
    except Exception as e:
        # If there's an error getting Telegraf status, provide error info
        telegraf_status = {
            "status": "error",
            "running": False,
            "error": str(e)
        }
    
    # Render the template with the data
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "fastapi_status": fastapi_status,
            "telegraf_status": telegraf_status
        }
    )

# Telegraf status endpoint
@app.get("/telegraf/status", tags=["Telegraf"])
async def telegraf_status():
    try:
        container = get_telegraf_container()
        
        # Get container info
        container_info = container.attrs
        
        # Get container state
        state = container_info["State"]
        
        # Get container logs (last 10 entries)
        logs = container.logs(tail=10, timestamps=True).decode("utf-8").strip()
        
        # Format logs
        formatted_logs = []
        if logs:
            log_lines = logs.split("\n")
            for log in log_lines:
                if log:
                    # Try to split timestamp and message
                    parts = log.split(" ", 1)
                    if len(parts) == 2:
                        timestamp, message = parts
                        formatted_logs.append({"timestamp": timestamp, "message": message})
                    else:
                        formatted_logs.append({"message": log})
        
        # Get health check results if available
        health_check = None
        if "Health" in state:
            health_check = {
                "status": state["Health"]["Status"],
                "failing_streak": state["Health"]["FailingStreak"],
                "last_check": state["Health"]["Log"][-1] if state["Health"]["Log"] else None
            }
        
        # Calculate last restart time
        started_at_str = state["StartedAt"]
        
        # Handle different timestamp formats
        try:
            # Try to parse with fromisoformat after cleaning up the format
            if started_at_str.endswith('Z'):
                started_at_str = started_at_str.replace("Z", "+00:00")
            
            # Remove nanoseconds if present (Python's fromisoformat doesn't handle them well)
            if '+' in started_at_str:
                parts = started_at_str.split('+')
                timestamp_part = parts[0]
                timezone_part = '+' + parts[1]
                
                # Truncate to microseconds (6 digits after the decimal)
                if '.' in timestamp_part:
                    main_part, fractional_part = timestamp_part.split('.')
                    fractional_part = fractional_part[:6]  # Keep only up to 6 digits
                    timestamp_part = f"{main_part}.{fractional_part}"
                
                started_at_str = timestamp_part + timezone_part
            
            started_at = datetime.datetime.fromisoformat(started_at_str)
        except ValueError:
            # Fallback to a more flexible parser
            started_at = datetime.datetime.now(datetime.timezone.utc)
        
        return {
            "status": state["Status"],
            "running": state["Running"],
            "health_check": health_check,
            "uptime": str(datetime.datetime.now(datetime.timezone.utc) - started_at),
            "started_at": started_at.isoformat(),
            "last_restart": started_at.isoformat(),
            "recent_logs": formatted_logs
        }
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting Telegraf status: {str(e)}"
        )

# Telegraf config endpoint
@app.get("/telegraf/config", tags=["Telegraf"], response_class=PlainTextResponse)
async def telegraf_config():
    try:
        # Get the Telegraf container
        container = get_telegraf_container()
        
        # Execute command to read the config file
        exec_result = container.exec_run("cat /etc/telegraf/telegraf.conf")
        
        if exec_result.exit_code != 0:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to read Telegraf config: {exec_result.output.decode('utf-8')}"
            )
        
        return exec_result.output.decode("utf-8")
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error reading Telegraf config: {str(e)}"
        )

# Telegraf upload config endpoint
@app.post("/telegraf/upload-config", tags=["Telegraf"])
async def upload_telegraf_config(request: Request, file: UploadFile = File(...), _: bool = Depends(verify_api_key)):
    try:
        container = get_telegraf_container()
        content = await file.read()
        
        if not content:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Empty configuration file")
        
        logger.info(f"Received configuration file: {file.filename}, size: {len(content)} bytes")
        
        # Create temporary file
        temp_file_path = "/tmp/telegraf.conf"
        with open(temp_file_path, "wb") as temp_file:
            temp_file.write(content)
        
        if not os.path.exists(temp_file_path):
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to create temporary file")
        
        # Create tar archive and copy to container
        tar_stream = io.BytesIO()
        with tarfile.open(fileobj=tar_stream, mode='w') as tar:
            tar.add(temp_file_path, arcname="telegraf.conf.new")
        
        tar_stream.seek(0)
        put_result = container.put_archive("/tmp", tar_stream.read())
        
        if not put_result:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to copy configuration to container")
        
        # Move file to final location
        exec_result = container.exec_run("sh -c 'cat /tmp/telegraf.conf.new > /etc/telegraf/telegraf.conf'")
        
        if exec_result.exit_code != 0:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                               detail=f"Failed to update configuration file: {exec_result.output.decode('utf-8')}")
        
        # Restart container
        try:
            container.restart(timeout=10)
        except Exception as restart_error:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                               detail=f"Failed to restart Telegraf container: {str(restart_error)}")
        
        # Cleanup
        os.remove(temp_file_path)
        
        # Return appropriate response
        if request and request.headers.get("content-type", "").startswith("multipart/form-data"):
            return RedirectResponse(url="/", status_code=303)
        else:
            return {"success": True, "message": "Telegraf configuration updated successfully"}
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error updating Telegraf configuration: {str(e)}"
        )

# Telegraf restart endpoint - with redirect to homepage
@app.post("/telegraf/restart", tags=["Telegraf"])
async def restart_telegraf(request: Request = None, graceful: bool = True, _: bool = Depends(verify_api_key)):
    try:
        # Get the Telegraf container
        container = get_telegraf_container()
        
        # Check if container is running before restart
        container_info = container.attrs
        was_running = container_info["State"]["Running"]
        
        # Pre-restart validation
        if not was_running:
            return {
                "success": False,
                "message": "Telegraf container is not running, cannot restart",
                "status": "not_running"
            }
        
        # Get the start time before restart
        start_time_before = container_info["State"]["StartedAt"]
        
        # Restart the container
        container.restart(timeout=10 if graceful else 0)
        
        # Wait a moment for the container to restart
        time.sleep(2)
        
        # Get updated container info
        container = get_telegraf_container()
        container_info = container.attrs
        
        # Check if restart was successful
        is_running = container_info["State"]["Running"]
        start_time_after = container_info["State"]["StartedAt"]
        
        # Verify that the start time has changed
        restart_successful = is_running and start_time_before != start_time_after
        
        # Check if this is a form submission from the web interface
        if request and request.headers.get("content-type") == "application/x-www-form-urlencoded":
            # Redirect back to the homepage
            return RedirectResponse(url="/", status_code=303)
        else:
            # Return JSON response for API calls
            if restart_successful:
                return {
                    "success": True,
                    "message": f"Telegraf container restarted {'gracefully' if graceful else 'forcefully'}",
                    "status": "running",
                    "started_at": start_time_after
                }
            else:
                return {
                    "success": False,
                    "message": "Telegraf container restart failed",
                    "status": "failed",
                    "is_running": is_running
                }
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error restarting Telegraf container: {str(e)}"
        )