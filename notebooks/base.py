import json
import os
import sys
from abc import abstractmethod
from collections import defaultdict
from datetime import datetime
from functools import cached_property
from pathlib import Path
from typing import Generator, TypeVar, Type

import pydantic
import snowflake.connector
import zlogging

sys.path[0] = str(Path(__file__).resolve().parents[1])  # Allows access to qa-frame modules

from qa_frame.consts.logging import essentials_only_logging_config  # noqa: E402
from qa_frame.models.environment import EnvConfig  # noqa: E402
from qa_frame.models.kafka.schema import AvroSchema  # noqa: E402
from qa_frame.plugins.toolbox import EnvironmentTool  # noqa: E402


class BaseScript:
    """
    ``BaseScript`` is a class that all scripts can inherit from. Provides some boilerplate functionality that includes:

    1. A configured logger with `self.logger`
    2. An initialized environment configuration with `self.config`
    3. Methods to write/read JSON line files of ``AvroSchema`` content
    4. Standardized `run` abstract method to streamline how scripts get ran

    The read/write methods assume the format of 1 JSON object per line in a file.
    """
    name = ""

    def __init__(self):
        """
        Initializes the logger, environment, and output directories.
        """
        if not self.name:
            raise ValueError("Must define the `name` attribute for a script!")

        zlogging.configure(essentials_only_logging_config)
        self.logger = zlogging.getLogger(__name__)
        EnvironmentTool.disable_loggers(self.logger, "snowflake.*")

        self.config = EnvConfig.get()

        self.root_output_folder = Path(__file__).parent / "output"
        self.base_output_folder = self.root_output_folder / self.name
        self.input_folder = Path(__file__).parent / "input"
        os.makedirs(self.root_output_folder, exist_ok=True)
        os.makedirs(self.base_output_folder, exist_ok=True)
        self.logger.info("Initialized output directory", output_path=self.base_output_folder)

        # Because script produces many .JSON file output, need to keep track of which file is being written
        # <file_prefix>: (<amount_of_files_written>, <rows_written>, <create_time>)
        self.__file_counter = defaultdict(lambda: [0, 0, f"{int(datetime.utcnow().timestamp())}"])
        self.__current_file_name = ""

    def __call__(self, *args, **kwargs):
        self.run()

    @abstractmethod
    def run(self):
        raise NotImplementedError("Must implement the scripts `run` method!")

    @cached_property
    def snowflake_connection(self) -> snowflake.connector.SnowflakeConnection:
        """
        Establishes a new connection to Snowflake and will return it

        Returns
        -------
        connection : snowflake.connector.SnowflakeConnection
            New connection to Snowflake
        """
        connection = snowflake.connector.connect(**self.config.interface.db.snowflake.__dict__)
        self.logger.info("Connected to snowflake", connection={
            k: v
            for k, v in self.config.interface.db.snowflake.__dict__.items()
            if "password" not in k.lower()
        })
        return connection

    def _write_json_files(self, file_prefix: str, *contents: AvroSchema) -> None:
        """
        Writes a single piece of content to a group of files by ``file_prefix``. By default, this will write a max
        of 200 pieces of content, 1 line per piece, to a single `.JSON` file.

        Parameters
        ----------
        file_prefix : str
            File prefix to write to. So, if you enter something like "human_review", then files will be generated
            with the titles ["human_review_0.json", "human_review_1.json", "human_review_2.json", ...]

        contents : AvroSchema
            Pieces of content to write

        Returns
        -------
        None
        """
        files_written, rows_written, create_time = self.__file_counter[file_prefix]

        output_folder = self.base_output_folder / (file_prefix + f"_{create_time}")

        if files_written == 0 and rows_written == 0:  # Need to create a sub output directory for the output
            os.makedirs(output_folder, exist_ok=True)

        for content in contents:
            file_name = f"{file_prefix}_{files_written}.json"
            if file_name != self.__current_file_name:  # Moved on to the next file
                self.logger.info("Writing file", file_name=file_name)
                self.__current_file_name = file_name

            with (output_folder / file_name).open("a") as json_output_file:
                json.dump(content.as_dict(), json_output_file)
                json_output_file.write("\n")
                self.__file_counter[file_prefix][1] += 1
                files_written, rows_written, create_time = self.__file_counter[file_prefix]

                if rows_written >= 200:
                    self.__file_counter[file_prefix] = [files_written + 1, 0, create_time]

    __AnyAvroSchema = TypeVar("__AnyAvroSchema", bound=AvroSchema)

    def _read_json_files(self, path: str, avro_type: Type[__AnyAvroSchema]) -> Generator[__AnyAvroSchema, None, None]:
        """
        Reads the content from a specified directory containing JSON files that contain 1 object per line and converts
        each object into a specified ``AvroSchema`` object and yields.

        The deserialization is best-effort and if

        Parameters
        ----------
        path : str
            Path of the directory that contains the files

        avro_type : Type[__AnyAvroSchema]
            Avro type to deserialize content to

        Yields
        -------
        content : __AnyAvroSchema
            Yields deserialized ``AvroSchema`` objects from all JSON files in a given directory
        """
        bad_obj_counter = 0  # Amount of objects we could not deserialize
        good_obj_counter = 0   # Amount of objects successfully deserialized

        for path in (self.input_folder / path).glob("*.json"):
            self.logger.info("Reading file", file_path=path)

            with path.open() as input_file:
                for line in input_file:
                    try:
                        obj = avro_type(**json.loads(line))
                        good_obj_counter += 1
                        yield obj
                    except (pydantic.ValidationError, json.JSONDecodeError):
                        self.logger.warning(f"Error deserializing {json.loads(line)}... Continuing")
                        bad_obj_counter += 1
                        continue

        self.logger.info(
            "Finished reading JSON files",
            successfully_read=good_obj_counter,
            unsuccessfully_read=bad_obj_counter
        )
