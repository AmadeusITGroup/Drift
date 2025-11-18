import base64
import logging
import os
from argparse import ArgumentParser
from logging.config import fileConfig

from azure.identity import ClientSecretCredential
from databricks.sdk import WorkspaceClient
from pydataio.pipeline import Pipeline


def parse_arguments():
    parser = ArgumentParser(description="Entrypoint for Spark job.")
    parser.add_argument("--config", type=str, required=True, help="Path to the config file")
    parser.add_argument("--tenant", type=str, required=True, help="Tenant id")
    parser.add_argument("--vault_name", type=str, required=True, help="Vault Name")
    parser.add_argument("--data_asset_version", type=str, required=False, help="Data asset version for retraining")
    return parser.parse_args()


def main():
    load_logging_configuration()

    logger = logging.getLogger(__name__)
    logger.info("Starting pipeline...")
    args = parse_arguments()

    logger.info("Initialize databricks workspace client...")
    w = WorkspaceClient()

    logger.info("Initializing credential...")
    credential = ClientSecretCredential(
        tenant_id=args.tenant,
        client_id=base64.b64decode(w.secrets.get_secret(args.vault_name, "ApplicationID").value).decode("utf-8"),
        client_secret=base64.b64decode(w.secrets.get_secret(args.vault_name, "ApplicationPassword").value).decode("utf-8"),
    )

    logger.info("Config path: %s", args.config)
    logger.debug("Instantiating Pipeline...")
    pipeline = Pipeline()
    logger.info("Running pipeline...")
    pipeline.run(args.config, credential, additionalArgs={"data_asset_version": args.data_asset_version, "vault_name": args.vault_name})
    logger.info("Pipeline completed.")
    logger.info("Pipeline completed.")


def load_logging_configuration():
    # Path to the logging configuration file
    config_path = os.path.join(os.path.dirname(__file__), "config", "logging.ini")

    logging.info("Config path: %s", config_path)

    # Load the logging configuration
    fileConfig(config_path)
