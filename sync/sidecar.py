import json
import os
import subprocess
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route
from loguru import logger


async def sync(request):
    try:
        data = await request.json()

        if "source" in data:
            source = data["source"]
            if not (
                "source_host" in source
                and "source_port" in source
                and "source_user" in source
                and "source_password" in source
            ):
                return JSONResponse("invalid connection details", 400)
        else:
            return JSONResponse("source is missing", 400)

        if "schema" in data:
            schema = data["schema"]
        else:
            return JSONResponse("schema is missing", 400)

        try:
            schema_str = json.dumps(schema)
        except ValueError:
            return JSONResponse("invalid schema", 400)

        # Validate connection config
        config = {
            "PG_HOST": source["source_host"],
            "PG_PORT": source["source_port"],
            "PG_USER": source["source_user"],
            "PG_PASSWORD": source["source_password"],
            "LOG_LEVEL": os.getenv("LOG_LEVEL"),
            "ELASTICSEARCH_HOST": os.getenv("ELASTICSEARCH_HOST"),
            "ELASTICSEARCH_PORT": os.getenv("ELASTICSEARCH_PORT"),
            "ELASTICSEARCH_SCHEME": os.getenv("ELASTICSEARCH_SCHEME"),
            "REDIS_HOST": os.getenv("REDIS_HOST"),
            "REDIS_PORT": os.getenv("REDIS_PORT"),
            "REDIS_AUTH": os.getenv("REDIS_AUTH"),
            "ELASTICSEARCH": os.getenv("ELASTICSEARCH"),
            "OPENSEARCH": os.getenv("OPENSEARCH"),
        }

        # Write schema to file
        file_name = "schema.json"
        schema_file = open(file_name, "w")
        schema_file.write(schema_str)
        schema_file.close()

        # Run bootstrap
        bootstrap_proc = subprocess.Popen(
            ["/usr/local/bin/bootstrap", "--config", file_name],
            env=config,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        bootstrap_proc.wait()
        out, err = bootstrap_proc.communicate()
        logger.info(out)
        logger.error(err)

        if bootstrap_proc.returncode is None or bootstrap_proc.returncode != 0:
            return JSONResponse("failed to run pgsync boostrap", 400)

        # Start pgsync
        sync_proc = subprocess.Popen(
            ["/usr/local/bin/pgsync", "--config", file_name, "--daemon"],
            env=config,
        )

    except json.decoder.JSONDecodeError as e:
        logger.error(e)
    return JSONResponse(data)


app = Starlette(
    debug=True,
    routes=[
        Route("/sync", sync, methods=["POST"]),
    ],
)
