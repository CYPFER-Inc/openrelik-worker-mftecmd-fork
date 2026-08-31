import os
import shutil
import subprocess
import time
from uuid import uuid4

from celery import signals
from celery.utils.log import get_task_logger

from openrelik_common.logging import Logger
from openrelik_worker_common.file_utils import create_output_file
from openrelik_worker_common.task_utils import create_task_result, get_input_files

import yaml
from pathvalidate import sanitize_filename

from .app import celery
from .ts_datetime import add_datetime_column

# Task name used to register and route the task to the correct queue.
TASK_NAME = "openrelik-worker-mftecmd.tasks.mftecmd"

# Task metadata for registration in the core system.
TASK_METADATA = {
    "display_name": "Eric Zimmerman's MFTECmd ",
    "description": "Runs Eric Zimmerman's MFTECmd  application on MFT files",
}

# Every name the extracted USN journal can arrive under. The extraction step
# decides this, and it has produced at least four shapes:
#   $UsnJrnl%3A$J   URL-escaped colon (the in-container VR path form)
#   $UsnJrnl$J      colon stripped   <-- KAN-1109: what we ACTUALLY get
#   $J              bare
#   UsnJrnl-J       dash-separated
#
# This list is consumed TWICE -- once to admit the file at all
# (COMPATIBLE_INPUTS) and once to decide whether to pass `-m $MFT` so MFTECmd
# can resolve USN records to real paths. It used to be duplicated as two
# literals, and they drifted: `$UsnJrnl$J` was in neither, so MFTECmd had
# never run on the journal on any case. One constant, both uses.
USN_JOURNAL_NAMES = [
    "$UsnJrnl%3A$J",
    "$UsnJrnl$J",
    "$J",
    "UsnJrnl-J",
]

COMPATIBLE_INPUTS = {
    "data_types": [],
    "mime_types": ["application/octet-stream", "text/plain"],
    "filenames": [
        "$Boot",
        "$I30","INDX",
        *USN_JOURNAL_NAMES,
        "$MFT",
        "$Secure_$SDS","$Secure%3A$SDS",
        "$LogFile",
        ".openrelik-config"
        ],
}

log_root = Logger()
logger = log_root.get_logger(__name__, get_task_logger(__name__))

@signals.task_prerun.connect
def on_task_prerun(sender, task_id, task, args, kwargs, **_):
    log_root.bind(
        task_id=task_id,
        task_name=task.name,
        worker_name=TASK_METADATA.get("display_name"),
    )

@celery.task(bind=True, name=TASK_NAME, metadata=TASK_METADATA)
def mftecmd(
    self,
    pipe_result=None,
    input_files=[],
    output_path=None,
    workflow_id=None,
    task_config={},
) -> str:
    output_files = []
    input_files = get_input_files(pipe_result, input_files or [], filter=COMPATIBLE_INPUTS)
    if not input_files:
        return create_task_result(
            output_files=output_files,
            workflow_id=workflow_id,
            command="",
        )

    # .openrelik-config 'hostname' key support
    prefix = ""
    config_item = next((f for f in input_files if f.get('display_name') == ".openrelik-config"), None)
    if config_item:
        try:
            with open(config_item.get('path'), "r", encoding="utf-8") as f:
                config_data = yaml.safe_load(f)

            if isinstance(config_data, dict) and "hostname" in config_data:
                raw_hostname = str(config_data["hostname"]).strip()
                prefix = f"{sanitize_filename(raw_hostname)}_"
            else:
                logger.info("No 'hostname' key found in .openrelik-config file.")

        except yaml.YAMLError:
            logger.error(".openrelik-config is not a valid YAML file.")
        except Exception as e:
            logger.error(f"Error reading .openrelik-config: {e}")

        # Pass through .openrelik-config as an output
        config_passthrough_file = create_output_file(
            output_path,
            display_name=config_item.get('display_name'),
            data_type="openrelik:openrelik-config:openrelik-config",
        )
        # link file to location of new output_file
        os.link(config_item.get("path"), config_passthrough_file.path)
        # output our file
        output_files.append(config_passthrough_file.to_dict())

    # Create temporary directory and hard link files for processing
    temp_dir = os.path.join(output_path, uuid4().hex)
    os.mkdir(temp_dir)
    # don't run on the .openrelik-config file
    for file in (f for f in input_files if f.get('display_name') != ".openrelik-config"):
        filename = os.path.basename(file.get("path"))
        os.link(file.get("path"), f"{temp_dir}/{filename}")

        output_file = create_output_file(
            output_path,
            display_name=f"{prefix}{file.get('display_name')}_MFTECmd_output.csv",
            data_type="openrelik:mftecmd:mftecmd",
        )

        command = [
            "dotnet",
            "/mftecmd/MFTECmd.dll",
            "-f",
            file.get("path"),
            "--csv",
            output_path,
            "--csvf",
            output_file.path,
        ]

        # add mft enrichment if this is a journal file and an mft file exists
        if file.get('display_name') in USN_JOURNAL_NAMES:
            if (mft_item := next((f for f in input_files if f.get('display_name') == "$MFT"), None)):
                command.append('-m')
                command.append(mft_item.get("path"))

        INTERVAL_SECONDS = 2
        process = subprocess.Popen(command)
        while process.poll() is None:
            self.send_event("task-progress", data=None)
            time.sleep(INTERVAL_SECONDS)
        
        # only append file if created
        # LogFile not supported by tool yet, but it tries to run against it and assumes it works
        # This leads it to try and collect a file that hasn't been made
        if os.path.exists(output_file.path):
            # KAN-1160: MFTECmd names the $MFT timestamp columns Created0x10,
            # LastModified0x10 etc -- none contains "time", so the TimeSketch
            # import client rejects the whole file and the NTFS timeline is
            # always empty, even though this task succeeded and the artefact is
            # complete. Add a `datetime` column derived from Created before the
            # CSV is handed on. Header-driven, so anything TimeSketch already
            # accepts (notably $UsnJrnl$J, which has UpdateTimestamp) is left
            # untouched.
            try:
                add_datetime_column(output_file.path, logger=logger)
            except Exception as e:
                # A failed rewrite must not lose the artefact: the original CSV
                # is left intact and still emitted. It will fail the TimeSketch
                # upload as before, which is visible, rather than vanishing.
                logger.error(f"Could not add datetime column to MFTECmd CSV: {e}")
            output_files.append(output_file.to_dict())

    # Remove temp directory
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)

    return create_task_result(
        output_files=output_files,
        workflow_id=workflow_id,
        command=" ".join(command),
    )
