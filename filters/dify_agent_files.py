"""
title: DIFY Manifold Pipe
authors: Estel
author_url: https://github.com/azdolinski
funding_url: https://github.com/azdolinski
version: 0.1
description: This process is used for DIFY's API interface to interact with DIFY's API
"""

import logging
import os
import requests
import json
import time
from typing import List, Union, Generator, Iterator, Optional
from pydantic import BaseModel, Field
from open_webui.utils.misc import pop_system_message
from open_webui.config import UPLOAD_DIR
import base64
import tempfile
from urllib.parse import quote
from io import BytesIO

# Debug mode
DEBUG_MODE = True


def get_file_extension(file_name: str) -> str:
    return os.path.splitext(file_name)[1].strip(".")


# Get closure variables from __event_emitter__
def get_closure_info(func):
    # Get the function's closure variables
    if hasattr(func, "__closure__") and func.__closure__:
        for cell in func.__closure__:
            if isinstance(cell.cell_contents, dict):
                return cell.cell_contents
    return None


class Pipe:
    class Valves(BaseModel):
        # Environment variables
        DIFY_BASE_URL: str = Field(default="http://192.168.1.5/v1")
        DIFY_KEY: str = Field(default="app-rJVcNBiZ4u1VxdpDziTtGhUv")
        FILE_SERVER: str = Field(default="http://192.168.1.5/v1/files/upload")
        DIFY_WORKFLOW: str = Field(default="Dify_API_GPT4o")
        DIFY_MODLE_ID: str = Field(default="dify_id")

    def __init__(self):
        
        # If file not exist.. create empty
        if not os.path.exists("data/dify/dify_file_data.json"):
            with open("data/dify/dify_file_data.json", "w", encoding="utf-8") as f:
                json.dump({}, f, ensure_ascii=False, indent=2)
        
        self.type = "manifold"
        self.id = "dify"
        self.name = "dify/"
        self.chat_message_mapping = {}
        self.dify_chat_model = {}
        self.dify_file_list = {}
        self.data_cache_dir = "/data/dify"
        self.load_state()
        self.valves = self.Valves()




    def save_state(self):
        """
        Persist Dify-related state variables to files
        The main purpose of this function is to save program runtime state information to local files,
        allowing the program to restore its previous state after a restart
        """
        # Create data cache directory if it doesn't exist
        # exist_ok=True means no error will be raised if directory already exists
        os.makedirs(self.data_cache_dir, exist_ok=True)

        # 1. Save chat message mapping relationship
        # chat_message_mapping.json stores the mapping between chat IDs and DIFY message IDs
        chat_mapping_file = os.path.join(
            self.data_cache_dir, "chat_message_mapping.json"
        )
        # Open file for writing, using UTF-8 encoding to support all characters
        with open(chat_mapping_file, "w", encoding="utf-8") as f:
            # json.dump converts Python objects to JSON format and writes to file
            # ensure_ascii=False allows writing non-ASCII characters
            # indent=2 sets 2-space indentation for better readability
            json.dump(self.chat_message_mapping, f, ensure_ascii=False, indent=2)

        # 2. Save chat model information
        # chat_model.json stores the model information used for each chat
        chat_model_file = os.path.join(self.data_cache_dir, "chat_model.json")
        with open(chat_model_file, "w", encoding="utf-8") as f:
            json.dump(self.dify_chat_model, f, ensure_ascii=False, indent=2)

        # 3. Save file list information
        # file_list.json stores information about uploaded files
        file_list_file = os.path.join(self.data_cache_dir, "file_list.json")
        with open(file_list_file, "w", encoding="utf-8") as f:
            json.dump(self.dify_file_list, f, ensure_ascii=False, indent=2)

    def load_state(self):
        """Load Dify-related state variables from files"""
        try:
            # chat_message_mapping.json
            chat_mapping_file = os.path.join(
                self.data_cache_dir, "chat_message_mapping.json"
            )
            if os.path.exists(chat_mapping_file):
                with open(chat_mapping_file, "r", encoding="utf-8") as f:
                    self.chat_message_mapping = json.load(f)
            else:
                self.chat_message_mapping = {}

            # chat_model.json
            chat_model_file = os.path.join(self.data_cache_dir, "chat_model.json")
            if os.path.exists(chat_model_file):
                with open(chat_model_file, "r", encoding="utf-8") as f:
                    self.dify_chat_model = json.load(f)
            else:
                self.dify_chat_model = {}

            # file_list.json
            file_list_file = os.path.join(self.data_cache_dir, "file_list.json")
            if os.path.exists(file_list_file):
                with open(file_list_file, "r", encoding="utf-8") as f:
                    self.dify_file_list = json.load(f)
            else:
                self.dify_file_list = {}

        except Exception as e:
            print(f"Failed to load Dify state files: {e}")
            # Use empty dictionaries if loading fails
            self.chat_message_mapping = {}
            self.dify_chat_model = {}
            self.dify_file_list = {}

    def get_models(self):
        """
        Get the list of DIFY models
        """
        return [
            {"id": self.valves.DIFY_MODLE_ID, "name": self.valves.DIFY_WORKFLOW},
        ]

    def upload_file(self, user_id: str, file_path: str, mime_type: str) -> str:
        """
        Upload file to DIFY server

        Args:
            user_id: User ID
            file_path: File path
            mime_type: File MIME type

        Returns:
            str: File ID returned after successful upload

        Raises:
            FileNotFoundError: File does not exist
            requests.exceptions.RequestException: API request failed
            ValueError: Invalid server response format
        """
        try:
            url = f"{self.valves.DIFY_BASE_URL}/files/upload"
            headers = {
                "Authorization": f"Bearer {self.valves.DIFY_KEY}",
            }

            file_name = os.path.basename(file_path)

            # Use 'with' statement to ensure proper file closure
            with open(file_path, "rb") as file:
                files = {
                    "file": (file_name, file, mime_type),
                    "user": (None, user_id),
                }
                response = requests.post(
                    url, headers=headers, files=files, timeout=(5, 30)
                )
                response.raise_for_status()  # Check response status

                result = response.json()
                if "id" not in result:
                    raise ValueError(f"Invalid server response format: {result}")

                return result["id"]

        except FileNotFoundError:
            logging.error(f"File not found: {file_path}")
            raise
        except requests.exceptions.RequestException as e:
            logging.error(f"File upload failed: {str(e)}")
            raise
        except Exception as e:
            logging.error(f"Error processing file: {str(e)}")
            raise

    def upload_images(self, image_data_base64: str, user_id: str) -> str:
        """
        Upload base64 encoded image to DIFY server, return image path
        Supported types: 'JPG', 'JPEG', 'PNG', 'GIF', 'WEBP', 'SVG'
        """
        try:
            # Remove the data URL scheme prefix if present
            if image_data_base64.startswith("data:"):
                # Extract the base64 data after the comma
                image_data_base64 = image_data_base64.split(",", 1)[1]

            # Decode base64 image data
            image_data = base64.b64decode(image_data_base64)

            # Create and save temporary file
            with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as tmp_file:
                tmp_file.write(image_data)
                temp_file_path = tmp_file.name
            try:
                file_id = self.upload_file(user_id, temp_file_path, "image/png")
            finally:
                os.remove(temp_file_path)
            return file_id
        except Exception as e:
            raise ValueError(f"Failed to process base64 image data: {str(e)}")

    def pipes(self) -> List[dict]:
        return self.get_models()

    def pipe(
        self,
        body: dict,
        __event_emitter__: dict,
        __user__: Optional[dict],
        __task__=None,
    ) -> Union[str, Generator, Iterator]:
        # Main process
        if DEBUG_MODE:
            print("-----------------------------------------------------------------")
            print("Debug - Pipe Function ")
            print(f"body:{body}")
            print(f"__task__:{__task__}")
            print("-----------------------------------------------------------------")
        # Get model name
        model_name = body["model"][body["model"].find(".") + 1 :]
        # Handle special tasks
        if __task__ is not None:
            if __task__ == "title_generation":
                return model_name
            elif __task__ == "tags_generation":
                return f'{{"tags":[{model_name}]}}'

        # Get current user
        current_user = __user__["email"]

        # Handle system messages and regular messages
        system_message, messages = pop_system_message(body["messages"])
        if DEBUG_MODE:
            print(f"system_message:{system_message}")
            print(f"messages:{messages}, {len(messages)}")

        # Get chat_id and message_id from event_emitter
        cell_contents = get_closure_info(__event_emitter__)
        chat_id = cell_contents["chat_id"]
        message_id = cell_contents["message_id"]
        # Handle conversation model and context
        parent_message_id = None
        # Modify the conversation history processing logic in the pipe function
        if len(messages) == 1:
            # Keep the new conversation logic unchanged
            self.dify_chat_model[chat_id] = model_name
            self.chat_message_mapping[chat_id] = {
                "dify_conversation_id": "",
                "messages": [],
            }
            self.dify_file_list[chat_id] = {}
        else:
            # Check if history exists
            if chat_id in self.chat_message_mapping:
                # First, validate the model
                if chat_id in self.dify_chat_model:
                    if self.dify_chat_model[chat_id] != model_name:
                        raise ValueError(
                            f"Cannot change model in an existing conversation. This conversation was started with {self.dify_chat_model[chat_id]}"
                        )
                else:
                    # If somehow the model wasn't recorded (exceptional case), record the current model
                    self.dify_chat_model[chat_id] = model_name

                chat_history = self.chat_message_mapping[chat_id]["messages"]
                current_msg_index = len(messages) - 1  # Index of the current message

                # If not the first message, get the dify_id of the previous message as parent
                if current_msg_index > 0 and len(chat_history) >= current_msg_index:
                    previous_msg = chat_history[current_msg_index - 1]
                    parent_message_id = list(previous_msg.values())[0]
                    # Key modification: Truncate message history after current position
                    self.chat_message_mapping[chat_id]["messages"] = chat_history[
                        :current_msg_index
                    ]
        # Get the last message as query
        message = messages[-1]
        query = ""
        file_list = []
        # DIFY APIs optional parameters model and system_message
        inputs = {
            "model": model_name,
            "system_message": (
                system_message.get("content", "") if system_message else ""
            ),  # Safer way to access the content
        }
        # Process message content
        if isinstance(message.get("content"), list):
            for item in message["content"]:
                if item["type"] == "text":
                    query += item["text"]
                if item["type"] == "image_url":
                    upload_file_id = self.upload_images(
                        item["image_url"]["url"], current_user
                    )
                    upload_file_dict = {
                        "type": "image",
                        "transfer_method": "local_file",
                        "url": "",
                        "upload_file_id": upload_file_id,
                    }
                    print("-----------------4------------------")
                    file_list.append(upload_file_dict)
        else:
            query = message.get("content", "")


        with open("data/dify/dify_file_data.json", "r", encoding="utf-8") as f:
            file_info = json.load(f)
        
        if DEBUG_MODE:
            print(f"file_info:{file_info}")
        if file_info.get("flag", False) is True:
            file_info["flag"] = False
            with open("data/dify/dify_file_data.json", "w", encoding="utf-8") as f:
                json.dump(file_info, f, ensure_ascii=False, indent=2)
            url = f"{self.valves.DIFY_BASE_URL}/files/upload"
            try:
                file_name = file_info["name"]
                file_extension = get_file_extension(file_name).upper()
                # Determine file type based on DifyAPI file extension
                file_type = "custom"  # Default type
                if file_extension in [
                    "TXT",
                    "MD",
                    "MARKDOWN",
                    "PDF",
                    "HTML",
                    "XLSX",
                    "XLS",
                    "DOC",
                    "DOCX",
                    "CSV",
                    "EML",
                    "MSG",
                    "PPTX",
                    "PPT",
                    "XML",
                    "EPUB",
                ]:
                    file_type = "document"
                elif file_extension in ["JPG", "JPEG", "PNG", "GIF", "WEBP", "SVG"]:
                    file_type = "image"
                elif file_extension in ["MP3", "M4A", "WAV", "WEBM", "AMR"]:
                    file_type = "audio"
                elif file_extension in ["MP4", "MOV", "MPEG", "MPGA"]:
                    file_type = "video"
                file_DYFI_FILE_ID = self._get_file_dify_server(
                    file_info["user_id"],
                    f"{file_info['id']}_{file_info['name']}",
                )
                # file_path_url=f"{file_info['id']}_{file_info['name']}"
                # file_DYFI_FILE_ID=self.upload_file(file_id,f"data/uploads/{file_path_url}",file_type)
                print(f"------------------file_DYFI_FILE_ID: {file_DYFI_FILE_ID}")

                file_list.append(
                    {
                        "type": file_type,
                        "transfer_method": "local_file",
                        "upload_file_id": file_DYFI_FILE_ID["id"],
                    }
                )
                print(f"Successfully added file: {file_name}, type: {file_type}")
            except Exception as e:
                print(f"Failed to process file {file_name}: {str(e)}")



        # Start sending data to Dify API

        # Build payload
        payload = {
            "inputs": inputs,
            "parent_message_id": parent_message_id,
            "query": query,
            "response_mode": "streaming" if body.get("stream", False) else "blocking",
            "conversation_id": self.chat_message_mapping[chat_id].get(
                "dify_conversation_id", ""
            ),
            "user": current_user,
            "files": file_list,
        }
        if DEBUG_MODE:
            print(f"file_list: {file_list}")
            print(f"payload: {payload}")
        
        # Set request headers
        headers = {
            "Authorization": f"Bearer {self.valves.DIFY_KEY}",
            "content-type": "application/json",
        }

        url = f"{self.valves.DIFY_BASE_URL}/chat-messages"

        try:
            if body.get("stream", False):
                print(f"Streaming {payload}")
                return self.stream_response(url, headers, payload, chat_id, message_id)
            else:
                print(f"Non-streaming {payload}")
                return self.non_stream_response(
                    url, headers, payload, chat_id, message_id
                )
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            return f"Error: Request failed: {e}"
        except Exception as e:
            print(f"Error in pipe method: {e}")
            return f"Error: {e}"

    def stream_response(self, url, headers, payload, chat_id, message_id):
        """Handle streaming response"""
        try:
            with requests.post(
                url, headers=headers, json=payload, stream=True, timeout=(3.05, 60)
            ) as response:
                if response.status_code != 200:
                    raise Exception(
                        f"HTTP Error {response.status_code}: {response.text}"
                    )

                for line in response.iter_lines():
                    if line:
                        line = line.decode("utf-8")
                        if line.startswith("data: "):
                            try:
                                data = json.loads(line[6:])
                                event = data.get("event")

                                if event == "message":
                                    # Process plain text messages
                                    yield data.get("answer", "")
                                elif event == "message_file":
                                    # Process file (image) messages
                                    pass
                                elif event == "message_end":
                                    # Save conversation and message ID mapping
                                    dify_conversation_id = data.get(
                                        "conversation_id", ""
                                    )
                                    dify_message_id = data.get("message_id", "")

                                    self.chat_message_mapping[chat_id][
                                        "dify_conversation_id"
                                    ] = dify_conversation_id
                                    self.chat_message_mapping[chat_id][
                                        "messages"
                                    ].append({message_id: dify_message_id})

                                    # Save state
                                    self.save_state()
                                    break
                                elif event == "error":
                                    # Handle errors
                                    error_msg = f"Error {data.get('status')}: {data.get('message')} ({data.get('code')})"
                                    yield f"Error: {error_msg}"
                                    break

                                time.sleep(0.01)
                            except json.JSONDecodeError:
                                print(f"Failed to parse JSON: {line}")
                            except KeyError as e:
                                print(f"Unexpected data structure: {e}")
                                print(f"Full data: {data}")
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            yield f"Error: Request failed: {e}"
        except Exception as e:
            print(f"General error in stream_response method: {e}")
            yield f"Error: {e}"

    def non_stream_response(self, url, headers, payload, chat_id, message_id):
        """Handle non-streaming response"""
        try:
            response = requests.post(
                url, headers=headers, json=payload, timeout=(3.05, 60)
            )
            if response.status_code != 200:
                raise Exception(f"HTTP Error {response.status_code}: {response.text}")

            res = response.json()

            # Save conversation and message ID mapping
            dify_conversation_id = res.get("conversation_id", "")
            dify_message_id = res.get("message_id", "")

            self.chat_message_mapping[chat_id][
                "dify_conversation_id"
            ] = dify_conversation_id
            self.chat_message_mapping[chat_id]["messages"].append(
                {message_id: dify_message_id}
            )

            # Save state
            self.save_state()

            return res.get("answer", "")
        except requests.exceptions.RequestException as e:
            print(f"Failed non-stream request: {e}")
            return f"Error: {e}"

    def _get_file_dify_server(self, User_id: str, file_name: str) -> str:
        # Read file from local uploads directory and upload to DIFY server in multipart/form-data format
        try:
            # Build local file path
            local_file_path = os.path.join("data/uploads", file_name)
            if DEBUG_MODE:
                print(f"Reading local file: {local_file_path}")

            upload_url = self.valves.FILE_SERVER
            headers = {"Authorization": f"Bearer {self.valves.DIFY_KEY}"}

            # Prepare file data and user ID
            with open(local_file_path, "rb") as file:
                files = {"file": (file_name, file, "application/octet-stream")}
                data = {"user": User_id}  # Add user ID parameter

                # Send POST request
                response = requests.post(
                    upload_url,
                    headers=headers,
                    files=files,
                    data=data,
                    timeout=(5, 30),  # Connection timeout 5 seconds, read timeout 30 seconds
                )
                # Check response
                try:
                    response.raise_for_status()
                except requests.exceptions.HTTPError as e:
                    error_msg = f"HTTP Error: {e.response.status_code}"
                    if response.headers.get("content-type") == "application/json":
                        error_detail = response.json()
                        error_msg += f" - {error_detail.get('message', '')}"
                    logging.error(error_msg)
                    raise

                result = response.json()
                required_fields = ["id", "name"]
                if not all(field in result for field in required_fields):
                    raise ValueError(f"Invalid server response format: {result}")
                if DEBUG_MODE:
                    print(f"File upload successful: {result}")
                return result

        except FileNotFoundError:
            logging.error(f"File not found: {local_file_path}")
            raise
        except requests.exceptions.RequestException as e:
            logging.error(f"File upload failed: {str(e)}")
            raise
        except Exception as e:
            logging.error(f"File processing failed: {str(e)}")
            raise
